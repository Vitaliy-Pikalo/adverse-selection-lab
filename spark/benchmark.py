"""Fresh-process labeling benchmark: three runs per engine on the audited backfill.

No research outputs are written. Each run writes temporary Snappy Parquet on
Linux disk. The parent times the complete worker command, then validates its
output and removes it outside the timed interval. Raw receipts/logs are retained.
"""

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import shlex
import signal
import statistics
import subprocess
import sys
import tarfile
import time
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
ROWS = {"KXBTC": 1010733, "KXETH": 110811}
FILES = {"KXBTC": 704, "KXETH": 693}
ORDER = ("pandas", "spark", "spark", "pandas", "pandas", "spark")
COMPARISON_ORDER = (
    "pandas", "spark_baseline", "spark", "spark", "pandas", "spark_baseline",
    "spark_baseline", "spark", "pandas",
)
CODE_FILES = (
    "src/asl/markouts.py", "src/asl/pricer.py", "src/asl/fixed.py",
    "spark/session.py", "spark/ingestion.py", "spark/spot.py", "spark/pricer.py",
    "spark/labels.py", "spark/benchmark.py",
)


def write_json(path: Path, data) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temp.replace(path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def memory_snapshot() -> dict:
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        if key in {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}:
            values[key + "_bytes"] = int(value.split()[0]) * 1024
    return {**values, "load_average": list(os.getloadavg())}


def machine_info() -> dict:
    raw = json.loads(subprocess.check_output(["lscpu", "-J"], text=True))
    selected = {
        "Architecture:", "CPU(s):", "Model name:", "Thread(s) per core:",
        "Core(s) per socket:", "Socket(s):", "Hypervisor vendor:",
    }
    return {
        "cpu": {row["field"].rstrip(":"): row["data"] for row in raw["lscpu"]
                if row["field"] in selected},
        "affinity_cpu_count": len(os.sched_getaffinity(0)),
        "kernel": platform.release(), "architecture": platform.machine(),
        "os": platform.freedesktop_os_release()["PRETTY_NAME"],
        "python": platform.python_version(), "python_executable": sys.executable,
        "packages": {name: importlib.metadata.version(name) for name in (
            "pandas", "numpy", "pyarrow", "pyspark",
        )},
        "java": subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT,
                                        text=True).strip(),
        "filesystems": subprocess.check_output(
            ["df", "-T", "/var/tmp", str(ROOT)], text=True,
        ).strip(),
        "memory": memory_snapshot(),
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                            text=True).strip(),
    }


def input_manifest(backfill: Path) -> dict:
    paths = []
    for series, product in (("KXBTC", "BTC-USD"), ("KXETH", "ETH-USD")):
        trades = sorted((backfill / "trades").glob(f"{series}-*.parquet"))
        if len(trades) != FILES[series]:
            raise ValueError(f"{series}: expected {FILES[series]} trade files, got {len(trades)}")
        paths.extend([backfill / f"markets_{series}.parquet",
                      backfill / "spot" / f"{product}_1m.parquet", *trades])
    entries = [
        {"path": str(path.relative_to(backfill)), "bytes": path.stat().st_size,
         "sha256": sha256(path)}
        for path in sorted(paths)
    ]
    digest = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()
    return {"digest": digest, "files": len(entries),
            "bytes": sum(entry["bytes"] for entry in entries), "entries": entries}


def worker(args) -> None:
    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    receipt = {"engine": args.engine, "status": "PASS", "compression": "snappy"}
    if args.engine == "pandas":
        import pandas as pd

        from asl import markouts as reference

        for series in ROWS:
            print(f"pandas: labeling {series}", flush=True)
            trades = reference.load_trades(args.backfill, series)
            markets = reference.load_markets(args.backfill, series)
            spot = reference.prepare_spot(pd.read_parquet(
                args.backfill / "spot" / f"{reference.product_for(series)}_1m.parquet",
            ))
            labels = reference.build_labels(trades, markets, spot)
            labels.to_parquet(output / f"markouts_{series}.parquet", engine="pyarrow",
                              compression="snappy", index=False)
            del trades, markets, spot, labels
        receipt["settings"] = {"parquet_engine": "pyarrow", "index": False}
    else:
        # Disable progress rendering only; keep the labeling/session code unchanged.
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--conf spark.ui.showConsoleProgress=false pyspark-shell"
        )
        from spark.labels import load_labels
        from spark.session import create_session

        session = create_session("asl-label-benchmark")
        session.sparkContext.setLogLevel("ERROR")
        try:
            receipt["settings"] = {
                "master": session.sparkContext.master,
                "default_parallelism": session.sparkContext.defaultParallelism,
                "spark.ui.showConsoleProgress": session.sparkContext.getConf().get(
                    "spark.ui.showConsoleProgress",
                ),
                "jvm_max_heap_bytes": session.sparkContext._jvm.java.lang.Runtime
                .getRuntime().maxMemory(),
                **{name: session.conf.get(name) for name in (
                    "spark.sql.session.timeZone", "spark.sql.shuffle.partitions",
                    "spark.sql.adaptive.enabled", "spark.sql.autoBroadcastJoinThreshold",
                    "spark.sql.execution.arrow.maxRecordsPerBatch",
                )},
            }
            for series in ROWS:
                print(f"spark: labeling {series}", flush=True)
                labels = load_labels(session, args.backfill, series)
                labels.write.option("compression", "snappy").parquet(
                    str(output / f"markouts_{series}.parquet"),
                )
        finally:
            session.stop()
    write_json(args.receipt, receipt)


def validate_output(output: Path) -> dict:
    import pyarrow.parquet as pq

    result = {}
    for series, expected in ROWS.items():
        path = output / f"markouts_{series}.parquet"
        parts = [path] if path.is_file() else sorted(path.glob("*.parquet"))
        if not parts:
            raise AssertionError(f"Missing Parquet output for {series}")
        tables = [pq.ParquetFile(part) for part in parts]
        rows = sum(table.metadata.num_rows for table in tables)
        columns = tables[0].schema_arrow.names
        assert rows == expected, f"{series}: expected {expected} rows, got {rows}"
        assert len(columns) == 27
        assert all(table.schema_arrow.names == columns for table in tables)
        result[series] = {
            "rows": rows, "columns": columns, "part_files": len(parts),
            "bytes": sum(part.stat().st_size for part in parts),
        }
    return result


def execute(
    command: list[str], log: Path, engine: str, number: int, *, cwd: Path = ROOT,
) -> float:
    """Time process launch through exit, including worker imports and shutdown."""
    with log.open("w", encoding="utf-8") as stream:
        environment = dict(os.environ)
        environment["PYTHONPATH"] = (
            str(cwd / "src") + os.pathsep + environment.get("PYTHONPATH", "")
        )
        started = time.perf_counter()
        process = subprocess.Popen(command, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True, env=environment)
        try:
            while True:
                try:
                    code = process.wait(timeout=30)
                    break
                except subprocess.TimeoutExpired:
                    print(f"RUNNING {engine} #{number}: {time.perf_counter() - started:.1f}s",
                          flush=True)
        except BaseException:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
        elapsed = time.perf_counter() - started
    if code:
        raise RuntimeError(f"{engine} #{number} exited {code}; inspect {log}")
    return elapsed


def run(args) -> None:
    if sys.platform != "linux":
        raise SystemExit("Run this benchmark inside WSL/Linux with the isolated Spark Python")
    backfill = args.backfill.resolve()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    directory = ROOT / "spark" / "benchmarks" / stamp
    directory.mkdir(parents=True, exist_ok=False)
    order = COMPARISON_ORDER if args.baseline_ref else ORDER
    report = {
        "status": "RUNNING", "started_utc": datetime.now(UTC).isoformat(),
        "command": shlex.join([sys.executable, "-B", "-m", "spark.benchmark", *sys.argv[1:]]),
        "backfill": str(backfill), "machine": machine_info(), "runs": [],
        "protocol": {
            "order": list(order), "runs_per_engine": 3, "fresh_process_per_run": True,
            "includes": ["imports", "Spark startup", "input reads", "engine validation",
                         "spot preparation", "joins", "labels", "Snappy Parquet write",
                         "Spark stop and Python exit"],
            "excludes": ["input hashing", "parent output validation", "output removal",
                         "bootstrap summaries", "parity comparison"],
            "cache": "All input bytes hashed before timing; OS caches not flushed; no JVM reuse",
            "output_filesystem": "/var/tmp (Linux disk); native file layout per engine",
            "source_import_path": "Each worker prepends its checkout's src directory to PYTHONPATH",
        },
    }
    print(f"REPORT_DIRECTORY {directory}", flush=True)
    print("Hashing inputs before timing (warms filesystem cache)", flush=True)
    manifest = input_manifest(backfill)
    write_json(directory / "inputs.json", manifest)
    report["inputs"] = {key: manifest[key] for key in ("digest", "files", "bytes")}
    code_hashes = {name: sha256(ROOT / name) for name in CODE_FILES}
    report["code_sha256"] = code_hashes
    write_json(directory / "results.json", report)
    counters = dict.fromkeys(order, 0)
    with ExitStack() as stack:
        roots = dict.fromkeys(counters, ROOT)
        baseline_hashes = None
        if args.baseline_ref:
            baseline_sha = subprocess.check_output(
                ["git", "rev-parse", "--verify", args.baseline_ref + "^{commit}"],
                cwd=ROOT, text=True,
            ).strip()
            snapshot_parent = ROOT / "data" / "spark-port"
            snapshot_parent.mkdir(parents=True, exist_ok=True)
            snapshot = Path(stack.enter_context(TemporaryDirectory(
                prefix="benchmark-baseline-", dir=snapshot_parent,
            )))
            archive = snapshot / "snapshot.tar"
            with archive.open("wb") as stream:
                subprocess.run(
                    ["git", "archive", baseline_sha], cwd=ROOT, stdout=stream, check=True,
                )
            with tarfile.open(archive) as bundle:
                bundle.extractall(snapshot, filter="data")
            archive.unlink()
            roots["spark_baseline"] = snapshot
            baseline_hashes = {name: sha256(snapshot / name) for name in CODE_FILES}
            report["baseline"] = {
                "git_ref": args.baseline_ref, "git_commit": baseline_sha,
                "code_sha256": baseline_hashes,
                "code_storage": "temporary snapshot on the repository filesystem (data/spark-port)",
            }
        try:
            run_trials(report, directory, backfill, order, counters, roots)
            print("Rehashing inputs and checking engine code after all runs", flush=True)
            assert input_manifest(backfill)["digest"] == manifest["digest"], "Input data changed"
            final_hashes = {name: sha256(ROOT / name) for name in CODE_FILES}
            assert final_hashes == code_hashes, "Engine code changed"
            if baseline_hashes is not None:
                assert {
                    name: sha256(roots["spark_baseline"] / name) for name in CODE_FILES
                } == baseline_hashes, "Baseline code changed"
            medians = {
                engine: statistics.median(item["wall_seconds"] for item in report["runs"]
                                          if item["engine"] == engine)
                for engine in counters
            }
            report.update({"status": "PASS", "finished_utc": datetime.now(UTC).isoformat(),
                           "median_seconds": medians,
                           "spark_over_pandas_ratio": medians["spark"] / medians["pandas"],
                           "inputs_unchanged": True, "engine_code_unchanged": True})
            if "spark_baseline" in medians:
                report["baseline_over_optimized_ratio"] = (
                    medians["spark_baseline"] / medians["spark"]
                )
                report["optimized_latency_reduction_percent"] = 100 * (
                    1 - medians["spark"] / medians["spark_baseline"]
                )
        except BaseException as error:
            report.update({"status": "FAILED", "error": f"{type(error).__name__}: {error}"})
            write_json(directory / "results.json", report)
            raise
    write_json(directory / "results.json", report)
    print("BENCHMARK_RESULT " + json.dumps({key: report[key] for key in (
        "status", "median_seconds", "spark_over_pandas_ratio", "inputs_unchanged",
    )}), flush=True)


def run_trials(report, directory, backfill, order, counters, roots) -> None:
    for engine in order:
        counters[engine] += 1
        number = counters[engine]
        label = f"{engine}_{number}"
        log = directory / f"{label}.log"
        before = memory_snapshot()
        with TemporaryDirectory(prefix="asl-benchmark-", dir="/var/tmp") as temp:
            root = Path(temp)
            actual_engine = "spark" if engine == "spark_baseline" else engine
            command = [
                sys.executable, "-B", "-m", "spark.benchmark", "worker",
                "--engine", actual_engine, "--backfill", str(backfill),
                "--output", str(root / "labels"), "--receipt", str(root / "worker.json"),
            ]
            print(f"START {engine} #{number}: {shlex.join(command)}", flush=True)
            elapsed = execute(command, log, engine, number, cwd=roots[engine])
            after = memory_snapshot()
            receipt = json.loads((root / "worker.json").read_text())
            assert receipt["engine"] == actual_engine and receipt["status"] == "PASS"
            outputs = validate_output(root / "labels")
            if report["runs"]:
                for series in ROWS:
                    assert outputs[series]["columns"] == report["runs"][0]["outputs"][series][
                        "columns"
                    ]
            report["runs"].append({
                "engine": engine, "number": number, "wall_seconds": elapsed,
                "command": command, "cwd": str(roots[engine]), "log": log.name,
                "worker": receipt, "outputs": outputs,
                "memory_before": before, "memory_after": after,
            })
            write_json(directory / "results.json", report)
        print(f"DONE {engine} #{number}: {elapsed:.6f}s (output counts/schema PASS)", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    parent = subparsers.add_parser(
        "run", help="Run six trials, or nine with a frozen Spark control",
    )
    parent.add_argument("--backfill", type=Path, default=ROOT / "data" / "backfill")
    parent.add_argument("--baseline-ref", help="Interleave three Spark control runs from a Git ref")
    child = subparsers.add_parser("worker", help="Internal single-engine command")
    child.add_argument("--engine", choices=["pandas", "spark"], required=True)
    child.add_argument("--backfill", type=Path, required=True)
    child.add_argument("--output", type=Path, required=True)
    child.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "run":
        run(args)
    else:
        worker(args)


if __name__ == "__main__":
    main()
