# Local Spark port: environment, ingestion, and labels

This additive port uses Ubuntu under WSL2. The pandas pipeline in `src/asl/`,
research outputs, collector, and deployment files remain unchanged. Environment
setup, ingestion/joins, labels, and pytest parity gates are implemented.
The measured local benchmark and dedicated CI job are below. No Spark speedup
is claimed on this dataset.

## Verified environment (November 19, 2026)

| Component | Version |
|---|---|
| WSL | 2.6.3, distribution running as WSL2 |
| Ubuntu | 26.04.1 LTS |
| Python | 3.11.9, isolated from Ubuntu's system Python |
| OpenJDK | 17.0.21.1 |
| PySpark | 4.0.1 |
| pandas / NumPy / PyArrow | 3.1.1 / 2.4.6 / 26.0.1 |
| uv (environment provisioning) | 0.13.9 |

The numerical library versions match the existing Windows research environment.
`requirements.lock` also pins the resolved runtime and development dependencies;
the editable project reference is relative to the repository root.

Host: Samsung 730QCJ/730QCR, Intel i7-10510U (4 cores, 8 logical CPUs),
approximately 11.8 GiB visible RAM. These are environment facts, not benchmark
results. WSL resource limits and available memory must be recorded at benchmark time.

## Setup from PowerShell

On a new machine, install Ubuntu (administrator access or a restart may be needed):

```powershell
wsl --install -d Ubuntu --no-launch
wsl --list --verbose
```

For this local environment, provisioning and the verification commands explicitly
select the WSL root account, so interactive Linux account setup was not needed.
All port environment files live under `/opt/asl-spark`; the existing Windows
`.venv` is not used. No cloud account or paid service is involved.

From the Windows repository directory, run:

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec bash spark/setup_wsl.sh
```

Adjust the mounted repository path on another machine. The setup script installs
JDK 17 and Python venv support through apt, obtains Python 3.11.9 through uv,
syncs the lock file, and runs the smoke check. It can be rerun to sync the same
environment. It does not launch the recorder, backfill, replay, or markout CLI.

The apt packages receive Ubuntu security updates; their observed versions above
are a snapshot. Python and the Python package versions are explicitly pinned.

## Verification commands and observed output

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m spark.smoke
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m pytest -q -p no:cacheprovider
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/ruff check spark src tests
```

Smoke output (Spark startup logs omitted):

```json
{
  "status": "PASS",
  "python": "3.11.9",
  "spark": "4.0.1",
  "java": "17.0.21.1",
  "pandas": "3.1.1",
  "numpy": "2.4.6",
  "pyarrow": "26.0.1",
  "master": "local[*]",
  "timezone": "UTC",
  "rows": 1000,
  "sum_id": 499500,
  "python_worker_sum_squares": 285,
  "parquet_roundtrip": "PASS"
}
```

The existing test suite reported `59 passed in 10.85s`. This is a test run duration,
not a labeling benchmark. The suite emits an existing pytest-asyncio configuration
deprecation warning. Spark reports that it uses built-in Java classes instead of
the optional native Hadoop library; the local Parquet check passed.

The smoke check uses synthetic data and an automatically removed Linux temporary
directory. It verifies JVM computation, Python worker startup, and Parquet row
equality in both directions, including multiplicity. It does not establish
research-label parity.

## Spark concepts used here

- **Lazy evaluation:** transformations build an execution plan; actions execute it.
  The smoke check forces execution with aggregation, collection, and Parquet writes.
- **Partitions:** Spark processes chunks of data in parallel. `local[*]` permits
  tasks on all logical CPUs available to the local Spark process, not a cloud cluster.
- **Shuffles:** some operations redistribute rows between partitions. The smoke
  aggregation and row comparison exercise this mechanism; it adds execution overhead.

The session fixes timestamps to UTC, binds the driver to loopback, and uses the
same Python executable for workers. The web UI is disabled. No session is created
at module import time.

## Refreshing the lock deliberately

Run inside Ubuntu from the repository root only when intentionally updating dependencies:

```bash
export UV_CACHE_DIR=/opt/asl-spark/cache
/opt/asl-spark/bootstrap/bin/uv pip compile \
  --python /opt/asl-spark/venv/bin/python \
  spark/requirements.txt -o spark/requirements.lock
```

References: [Spark 4.0.1 installation requirements](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html),
[Microsoft WSL installation](https://learn.microsoft.com/en-us/windows/wsl/install),
and [uv installation](https://docs.astral.sh/uv/getting-started/installation/).

## Phase 2: ingestion and minute joins

`ingestion.py` reads the existing trade, market, and candle Parquet files with
explicit schemas. It coerces invalid strike strings to null, maps settlement
results, and applies the pandas eligibility rules: exclude block trades, unmatched
markets, unknown settlements, and trades at or after close. The market join uses
null-safe key equality, matching pandas' null-key merge behavior.

`spot.py` builds a complete UTC minute grid with null candle gaps, then left-joins
minute opens at t, t-1m, t-5m, t+1m, t+5m, and t+15m. It never forward-fills gaps.
Future spot columns are named separately from historical inputs. The initial
Phase 2 comparison covered ingestion/joins; Phase 3 extends them with labels below.

A **broadcast join** copies the small spot table to each worker so the large trade
table can be joined without redistributing its rows for each spot lookup. Market
join strategy is left to Spark's optimizer.

### Duplicate and ordering contract

Identical trade/candle records with the same key collapse safely. Conflicting
records with the same trade ID or candle timestamp raise `ValueError`: pandas
keeps the first input record, and Spark does not guarantee that file/row order.
This is an explicit restriction of the port. Both hourly datasets passed these
guards. Any duplicate market ticker is rejected, including identical rows,
matching pandas' many-to-one join validation.

The loaders run Spark actions for these guards. Future end-to-end benchmarks must
include that validation cost. Output order is unspecified; comparison uses unique
trade IDs and checks row multiplicity.

### Commands and observed results (November 19, 2026)

From the Windows repository directory:

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m pytest tests/test_spark_ingestion.py -q -p no:cacheprovider
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m spark.verify_ingestion --backfill data/backfill --series KXBTC KXETH
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/ruff check spark src tests
.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
```

Observed test/lint output:

```text
5 passed, 14 warnings in 63.09s
All checks passed!
59 passed, 1 skipped in 45.60s
```

The Windows suite skips the Spark test module when PySpark is absent, preserving
compatibility with the current CI environment. The new tests cover minute-open
selection at microsecond boundaries, missing minutes, all eligibility filters,
identical and conflicting duplicates, market-key validation, and empty trade input.
The Spark-to-pandas conversion in the small fixture emits pandas copy-keyword
deprecation warnings; all comparisons pass.

The full check compares all 20 intermediate columns by trade ID against the
existing pandas loaders and spot lookup functions. It checks missing-value masks,
unknown/duplicate/omitted keys, and numeric values with `rtol=1e-12, atol=1e-12`.
Spark output is written to an automatically removed Linux temporary directory
and compared in batches of 10,000 rows; research outputs are not overwritten.

| Series | Compared rows | Compared columns | Result |
|---|---:|---:|---|
| KXBTC | 1,010,733 | 20 | PASS |
| KXETH | 110,811 | 20 | PASS |
| Total | 1,121,544 | 20 | PASS |

Missing spot rows matched pandas at every offset:

| Series | t | t-1m | t-5m | t+1m | t+5m | t+15m |
|---|---:|---:|---:|---:|---:|---:|
| KXBTC | 2,991 | 2,984 | 3,143 | 2,990 | 2,642 | 2,267 |
| KXETH | 185 | 181 | 181 | 179 | 185 | 188 |

These are ingestion/join correctness results. The separate `data/backfill15m`
dataset is outside this phase's validation scope.

## Phase 3: volatility and labels

`spot.prepare_spot` computes minute log-close differences and sample standard
deviation using native Spark window functions. A **window function** computes
over neighboring ordered rows while retaining each row. On the complete minute
grid, the frame `rowsBetween(-60, -1)` reproduces pandas' 60-minute rolling
standard deviation shifted by one minute. At least 30 valid returns are required.
A missing close invalidates its return and the following return; gaps stay missing.

Each call handles one product. Its ordered window uses one partition over the
small spot table (roughly 43,200 rows here), then the prepared spot table is
broadcast into the trade joins. The million-row trade table is not placed in a
single ordered window. Future spot joins project only the open; only the t join
exposes sigma.

`pricer.py` implements the existing log-normal probability formula with Spark
expressions for logarithms, square roots, validity checks, and clipping. Spark
4.0.1 has no native Python API `erf` function. An Arrow-backed **pandas UDF** moves
column batches between the JVM and Python for the normal CDF; `math.erf` is
evaluated per element with the same formula as the reference pricer. This is a
Python computation cost that must be included in benchmarks. It does not run the
pandas labeling pipeline inside Spark.

`labels.build_labels` produces the same 27 columns as the default pandas labels,
with unordered rows keyed by trade ID:

- Fixed-point price/quantity scaling, maker sign, and microsecond time-to-close.
- Current fair value, maker edge, and 1/5-minute stale-move features.
- Settlement markout and 1/5/15-minute markouts and value moves.
- Trade-time sigma retained for every pricing call, including future labels.
- Settlement pinning at or after close, even without future spot or valid sigma.

Missing values are preserved before Spark's null-skipping `greatest`/`least`
operations. This avoids inventing a volatility floor or a clipped probability
when the reference value is missing.

`load_labels(session, backfill, series)` combines the local readers and label
expressions. It does not write data. The research bootstrap and summary functions
remain unchanged and are not invoked by the port.

### Phase 3 verification

The synthetic cases cover volatility warmup/gaps, all strike types, sigma floors,
invalid/nonfinite inputs, microsecond timestamps, fixed-point scaling, mixed
contract labels, future-candle mutation, and settlement exactly at close without
future spot or sigma. Label decomposition is checked where trade-time fair value
exists: a future markout can be defined when the current fair value is missing,
in which case maker edge and value move are missing in both implementations.

The diagnostic below computes a fresh pandas reference with existing functions,
writes Spark labels only to an automatically removed Linux temporary directory,
and compares batches of 10,000 rows. It checks exact keys, row multiplicity,
timestamps, maker signs, settlements, price, quantity, spot, and missing-value
masks. The remaining numeric columns use tolerances declared before the full run:
`rtol=1e-10, atol=1e-12`. Maximum absolute errors are reported, not hidden.

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m pytest tests/test_spark_ingestion.py tests/test_spark_labels.py -q -p no:cacheprovider
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m spark.verify_labels --backfill data/backfill --series KXBTC KXETH
.venv\Scripts\python.exe -B -m pytest tests/test_markouts.py tests/test_spark_ingestion.py tests/test_spark_labels.py -q -p no:cacheprovider
```

Observed test output:

```text
11 passed, 113 warnings in 161.79s
16 passed, 2 skipped in 6.93s
```

The warnings include Spark's pandas copy-keyword deprecations and an expected
NumPy invalid-divide warning from deliberately testing infinite volatility in
the unchanged reference pricer. Test durations are not labeling benchmarks.

Full-data diagnostic output on November 19, 2026:

```text
KXBTC: PASS — 1,010,733 rows, 27 columns
KXETH: PASS —   110,811 rows, 27 columns
Total:       1,121,544 rows
```

All keys, missing-value masks, and exact comparison columns matched. Maximum
absolute differences from the diagnostic (rounded here; the command reports full
precision):

| Column | KXBTC | KXETH |
|---|---:|---:|
| tau_min (minutes) | 1.8189894e-12 | 1.8189894e-12 |
| floor_strike / cap_strike | 0 | 0 |
| sigma | 2.3342873e-16 | 1.0863706e-16 |
| fv | 4.1167070e-13 | 1.3389290e-13 |
| maker_edge | 4.1167070e-13 | 1.3389290e-13 |
| stale_move_1m | 2.2210012e-13 | 1.0691448e-13 |
| stale_move_5m | 2.9937164e-13 | 1.2623236e-13 |
| mo_settle | 0 | 0 |
| mo_1m | 4.3121062e-13 | 1.2789769e-13 |
| mo_5m | 4.0689674e-13 | 1.3022916e-13 |
| mo_15m | 5.6976646e-13 | 1.1346479e-13 |
| move_1m | 2.2043478e-13 | 8.5931262e-14 |
| move_5m | 3.1097347e-13 | 1.0635937e-13 |
| move_15m | 5.6887828e-13 | 1.3389290e-13 |

The floating comparison uses both relative and absolute tolerance; it is not an
absolute-error-only threshold. All horizon markout differences were below the
1e-12 absolute tolerance. The diagnostic holds a pandas reference while Spark
runs, so its runtime and memory profile do not measure the engines separately.

This diagnostic supported Phase 3 development. The dedicated Phase 4 pytest gate
is described below. No labeling benchmark has been performed.

References: [Spark window frames](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rowsBetween.html),
[Spark pandas UDFs](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html).

## Phase 4: pytest parity gate

`tests/test_spark_parity.py` tests the complete Parquet-to-label pipeline. The
shared comparator in `spark/parity.py` is also used by the interactive diagnostic;
its tolerances are unchanged from Phase 3. It requires all 27 columns, checks
every row by trade ID, checks exact columns and missing-value masks, and bounds
the remaining floating differences. It streams Spark output in 10,000-row
batches. Physical row order and pandas/Spark dtype representations may differ;
timestamps are compared as UTC instants without rounding.

The routine end-to-end fixture generates deterministic **synthetic** Parquet
inputs in a pytest temporary directory. Its 49 eligible trades cover all three
strike types, both maker signs, volatility warmup, candle gaps, microsecond
timestamps, settlement horizons, identical duplicates, and eligibility filters.
It is not a sampled claim about the research dataset. It requires no credentials,
network access, or local research data and is suitable for the Phase 6 CI job.

Thirteen comparator checks run even without PySpark. They verify rejection of
missing rows, duplicate keys within/across batches, unknown/null keys, missing
columns (including empty output), changed float values, changed missing-value
masks, changed exact prices, changed timestamps, and errors near zero. Reordered
rows and a float difference inside the declared tolerance are accepted.

Routine command from the Windows repository directory:

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m pytest tests/test_spark_parity.py -q -s -p no:cacheprovider
```

The full-data tests are opt-in. `--spark-full-data` requires PySpark; it errors
if PySpark is absent. Missing inputs, incorrect event-file counts, or incorrect
reference row counts fail the tests. An explicit full run cannot quietly fall
back to the synthetic fixture or pass with a partial hourly backfill.

The audited hourly scope is pinned to 704 KXBTC trade files / 1,010,733 label
rows and 693 KXETH trade files / 110,811 label rows. Deliberately replacing that
dataset requires reviewing the `FULL_FILES` and `FULL_ROWS` constants in the test.
These are file/row-count checks, not cryptographic dataset hashes.

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m pytest tests/test_spark_parity.py -m spark_full --spark-full-data --spark-backfill data/backfill -q -s -p no:cacheprovider
```

`--spark-backfill` accepts a local path; relative paths resolve from the repository
root. Without `--spark-full-data`, the two full cases are explicitly skipped.
Without PySpark, the synthetic integration case is also skipped while the
comparator checks still run. The original pandas pipeline, stored research
outputs, collector, and deployment remain unchanged.

Both full cases compute fresh pandas labels with the original functions and
Spark labels with `load_labels`. Spark output is written to automatically removed
Linux temporary directories, including when comparison fails. No bootstrap,
research-summary generation, backfill download, or collector command runs.
Pytest elapsed times and this co-resident comparison are not labeling benchmarks.

Observed routine results on November 19, 2026:

```text
WSL parity module: 14 passed, 2 skipped in 122.33s
Synthetic sample: PASS — 49 rows, 27 columns
Windows complete suite: 72 passed, 5 skipped in 12.38s
ruff check spark src tests: All checks passed!
```

The routine WSL run skipped only the two opt-in full cases. The Windows run
also skipped Spark integration because PySpark is intentionally isolated in WSL.
A negative check with `--spark-full-data` in the Windows environment exited
nonzero with `--spark-full-data requires PySpark`, confirming that an explicitly
requested full gate cannot be reported as a skip when Spark is unavailable.

Observed explicit full-data pytest result on November 19, 2026:

```text
2 passed, 14 deselected in 454.03s
```

| Full pytest case | Compared rows | Columns | Result |
|---|---:|---:|---|
| test_full_dataset_parity[KXBTC] | 1,010,733 | 27 | PASS |
| test_full_dataset_parity[KXETH] | 110,811 | 27 | PASS |
| Total | 1,121,544 | 27 | PASS |

The 14 deselected cases were the routine comparator/sample tests, which passed
in the separate routine run above. Both full cases ran; neither was skipped.
All keys, row counts, exact-value columns, and missing-value masks matched.
Settlement markouts had zero difference for both series. The largest absolute
horizon-markout differences were 5.6976646e-13 dollars/contract for KXBTC and
1.3022916e-13 for KXETH. The full per-column error reports matched the Phase 3
table above. The label engine was unchanged during Phase 4.

This completes the full-dataset parity gate. Phase 5 must measure pandas and
Spark separately for three runs each and report medians with machine/resource
settings. The 454.03-second pytest duration includes reference computation,
Spark execution, validation, and comparison; it is not an engine benchmark.

## Phase 5: measured local benchmark

The six-run benchmark completed on November 19, 2026 (Eastern time), recorded
as November 20 in UTC. All runs labeled the same 1,121,544 hourly trades across
KXBTC/KXETH, wrote all 27 columns, and passed output row-count/schema checks.

| Engine | Run 1 (s) | Run 2 (s) | Run 3 (s) | Median (s) |
|---|---:|---:|---:|---:|
| pandas | 55.983380 | 36.537986 | 35.832539 | **36.537986** |
| Spark local[*] | 175.041353 | 192.466803 | 160.712250 | **175.041353** |

**Spark took 4.79 times as long as pandas by the median** for this fresh-process
batch workload. All three measurements per engine are retained; none were
discarded or replaced. This result is specific to this implementation, machine,
dataset, storage placement, and protocol. It does not measure a distributed
cluster or a long-lived, warmed Spark session.

Reproduce from the Windows repository directory:

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m spark.benchmark run --backfill data/backfill
```

Observed final output:

```text
BENCHMARK_RESULT {"status": "PASS", "median_seconds": {"pandas": 36.53798611800005, "spark": 175.04135288199996}, "spark_over_pandas_ratio": 4.790667781105968, "inputs_unchanged": true}
```

### Machine and settings

- Samsung 730QCJ/730QCR; Intel i7-10510U, 4 physical cores / 8 logical CPUs.
- Host RAM: 12,655,898,624 bytes (about 11.8 GiB), checked with Windows CIM.
- Ubuntu 26.04.1 on WSL2, kernel 6.6.87.2; 8 CPUs available to the process.
- WSL RAM: 6,098,735,104 bytes (about 5.68 GiB); 2 GiB configured swap.
- Python 3.11.9, pandas 3.1.1, NumPy 2.4.6, PyArrow 26.0.1 for both engines.
- PySpark 4.0.1 / OpenJDK 17.0.21.1, master `local[*]`, default parallelism 8.
- JVM maximum heap: 1 GiB; 200 initial shuffle partitions; adaptive execution on;
  broadcast threshold 10 MiB; Arrow batch limit 10,000; UTC session timezone.
- Spark console progress rendering disabled for the benchmark; label/session
  implementation otherwise unchanged. No DataFrame caching or JVM reuse.
- Inputs on Windows-mounted `/mnt/c` (9p); temporary outputs on Linux ext4
  `/var/tmp`. `/tmp` was avoided because this WSL installation mounts it as tmpfs.

### Timing boundary and limitations

The fixed run order was **pandas, Spark, Spark, pandas, pandas, Spark**, with each
engine launched in a separate Python process. The parent measured process launch
through worker exit with `time.perf_counter`. Imports, JVM/Python-worker startup,
input reads, each implementation's validation, minute-grid/volatility preparation,
joins, pricing/labels, Snappy Parquet writes, and Spark stop/Python exit counted.
WSL boot, parent environment discovery, hashing, parent output checks, output
removal, parity comparison, and the research bootstrap/summary did not count.

Both engines used Snappy and the same output filesystem. Their native file
layouts were retained: pandas wrote one file per series; Spark wrote nine BTC
parts and eight ETH parts. Writes forced every label column to materialize;
DataFrame construction or row counts alone were not used as the timed workload.

All 1,401 input files (59,818,237 bytes) were hashed before timing, which pre-read
their contents and warmed filesystem caches. Caches were not flushed; no warmup
run was discarded. These are fresh-process, filesystem-cache-influenced runs,
not cold-disk measurements. This was a shared laptop, with normal background
activity and no fixed CPU-frequency or thermal controls. Per-run load/memory/swap
snapshots are recorded, but they are not peak-memory measurements. Three trials
provide a descriptive median, not a confidence interval or a universal ratio.

At this scale the data fits in memory. The current Spark path pays for JVM and
worker startup, scheduling/shuffles, many small input files, extra duplicate-key
validation scans, and Arrow/Python CDF transfers. Those costs are plausible
contributors to the slower batch result; they were not individually profiled.
This benchmark includes them rather than claiming a speedup from parallelism.

### Raw evidence

- [Results, exact worker commands, machine/settings, six timings, and output metadata](benchmarks/20261120T030922Z/results.json)
- [Per-file input hashes and sizes](benchmarks/20261120T030922Z/inputs.json)
- [pandas run 1 log](benchmarks/20261120T030922Z/pandas_1.log),
  [run 2](benchmarks/20261120T030922Z/pandas_2.log), [run 3](benchmarks/20261120T030922Z/pandas_3.log)
- [Spark run 1 log](benchmarks/20261120T030922Z/spark_1.log),
  [run 2](benchmarks/20261120T030922Z/spark_2.log), [run 3](benchmarks/20261120T030922Z/spark_3.log)

The post-run input digest matched the pre-run digest, and the measured engine and
harness source hashes were unchanged. The measured engine baseline was commit
`dab5a0f`, which had already passed the full parity gate. Only generated benchmark
receipts/logs are retained; temporary labeled data was removed. No original
research outputs, results, collector files, or deployments were modified.

## Phase 6: CI and portfolio documentation

The main [README](../README.md#pyspark-port-parity-before-performance) summarizes
full parity, all six benchmark measurements, machine specifications, caveats,
reproduction commands, and a resume bullet.

[Spark parity CI](../.github/workflows/spark-parity.yml) runs on standard
`ubuntu-24.04` GitHub-hosted runners with Python 3.11, Temurin JDK 17, and the same
locked package versions. It installs into a separate virtual environment and
runs ingestion tests, no-lookahead/pricing tests, and the synthetic sample parity
gate with `-m "not spark_full"`. It never downloads the research dataset or runs
the collector, backfill, bootstrap, full-data benchmark, or deployment.

Equivalent local test command after setup:

```powershell
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m pytest tests/test_spark_ingestion.py tests/test_spark_labels.py tests/test_spark_parity.py -m "not spark_full" -q -s -p no:cacheprovider
```

The original CI workflow is retained. Standard hosted runners are free for this
public repository ([GitHub's runner policy](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)).
The CI runner is a correctness check, not the laptop used for the benchmark.
No Databricks execution or cluster-scale performance is claimed.
