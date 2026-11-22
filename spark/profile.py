"""Record full-data plans and Spark event-log metrics, separately from benchmarks."""

import argparse
import json
import os
import re
import time
from collections import Counter
from contextlib import redirect_stdout
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from spark.benchmark import ROOT, validate_output, write_json


def summarize_events(directory: Path) -> dict:
    jobs, stages = {}, {}
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        for line in path.read_text().splitlines():
            event = json.loads(line)
            kind = event["Event"]
            if kind == "SparkListenerJobStart":
                props = event.get("Properties") or {}
                jobs[event["Job ID"]] = {
                    "id": event["Job ID"], "start_ms": event["Submission Time"],
                    "description": props.get("spark.job.description", ""),
                    "stage_ids": event["Stage IDs"],
                }
            elif kind == "SparkListenerJobEnd":
                job = jobs[event["Job ID"]]
                job["duration_ms"] = event["Completion Time"] - job["start_ms"]
                job["result"] = event["Job Result"]["Result"]
            elif kind == "SparkListenerStageSubmitted":
                info = event["Stage Info"]
                stages[info["Stage ID"]] = {
                    "id": info["Stage ID"], "name": info["Stage Name"],
                    "planned_tasks": info["Number of Tasks"], "completed_tasks": 0,
                    "executor_run_ms": 0, "input_bytes": 0, "shuffle_read_bytes": 0,
                    "shuffle_write_bytes": 0,
                }
            elif kind == "SparkListenerTaskEnd":
                stage = stages[event["Stage ID"]]
                metrics = event.get("Task Metrics") or {}
                stage["completed_tasks"] += 1
                stage["executor_run_ms"] += metrics.get("Executor Run Time", 0)
                stage["input_bytes"] += metrics.get("Input Metrics", {}).get("Bytes Read", 0)
                read = metrics.get("Shuffle Read Metrics", {})
                stage["shuffle_read_bytes"] += read.get("Remote Bytes Read", 0) + read.get(
                    "Local Bytes Read", 0,
                )
                stage["shuffle_write_bytes"] += metrics.get("Shuffle Write Metrics", {}).get(
                    "Shuffle Bytes Written", 0,
                )
    listing = [job for job in jobs.values() if "listing" in job["description"].lower()]
    listing_stages = {stage for job in listing for stage in job["stage_ids"]}
    return {
        "jobs": list(jobs.values()), "stages": list(stages.values()),
        "total_tasks": sum(stage["completed_tasks"] for stage in stages.values()),
        "listing_jobs": len(listing),
        "listing_job_wall_ms": sum(job["duration_ms"] for job in listing),
        "listing_tasks": sum(stages[s]["completed_tasks"] for s in listing_stages if s in stages),
        "total_executor_run_ms": sum(stage["executor_run_ms"] for stage in stages.values()),
        "input_bytes": sum(stage["input_bytes"] for stage in stages.values()),
        "shuffle_read_bytes": sum(stage["shuffle_read_bytes"] for stage in stages.values()),
        "shuffle_write_bytes": sum(stage["shuffle_write_bytes"] for stage in stages.values()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--backfill", type=Path, default=ROOT / "data" / "backfill")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9_-]+", args.label):
        parser.error("label must contain lowercase letters, numbers, underscores, or hyphens")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    destination = ROOT / "spark" / "profiles" / f"{stamp}-{args.label}"
    destination.mkdir(parents=True, exist_ok=False)
    report = {"label": args.label, "started_utc": datetime.now(UTC).isoformat(), "plans": {}}
    with TemporaryDirectory(prefix="asl-profile-", dir="/var/tmp") as temp:
        root = Path(temp)
        events = root / "events"
        events.mkdir()
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--conf spark.ui.showConsoleProgress=false --conf spark.eventLog.enabled=true "
            "--conf spark.eventLog.compress=false --conf spark.eventLog.rolling.enabled=false "
            f"--conf spark.eventLog.dir={events.as_uri()} pyspark-shell"
        )
        from spark.labels import load_labels
        from spark.session import create_session

        started = time.perf_counter()
        session = create_session("asl-plan-profile")
        session.sparkContext.setLogLevel("ERROR")
        try:
            report["application_id"] = session.sparkContext.applicationId
            report["listing_threshold"] = session.conf.get(
                "spark.sql.sources.parallelPartitionDiscovery.threshold",
            )
            for series in ("KXBTC", "KXETH"):
                print(f"PROFILE {args.label}: {series}", flush=True)
                frame = load_labels(session, args.backfill, series)
                plan_file = destination / f"{series}_plan.txt"
                with plan_file.open("w", encoding="utf-8") as stream, redirect_stdout(stream):
                    frame.explain(mode="formatted")
                lines = plan_file.read_text().splitlines()
                text = "\n".join(line.rstrip() for line in lines).rstrip() + "\n"
                plan_file.write_text(text, encoding="utf-8")
                nodes = re.findall(r"^\(\d+\) ([^\n]+)", text, flags=re.MULTILINE)
                report["plans"][series] = {"file": plan_file.name, "node_counts": dict(Counter(
                    node.split(" [")[0].strip() for node in nodes
                ))}
                frame.write.option("compression", "snappy").parquet(
                    str(root / f"markouts_{series}.parquet"),
                )
        finally:
            session.stop()
        report["instrumented_wall_seconds"] = time.perf_counter() - started
        report["outputs"] = validate_output(root)
        report["metrics"] = summarize_events(events)
    report["status"] = "PASS"
    write_json(destination / "summary.json", report)
    print("PROFILE_RESULT " + json.dumps({
        "directory": str(destination), "status": report["status"],
        "total_tasks": report["metrics"]["total_tasks"],
        "listing_tasks": report["metrics"]["listing_tasks"],
        "listing_job_wall_ms": report["metrics"]["listing_job_wall_ms"],
    }), flush=True)


if __name__ == "__main__":
    main()
