# Local Spark port: environment and ingestion

This additive port uses Ubuntu under WSL2. The pandas pipeline in `src/asl/`,
research outputs, collector, and deployment files remain unchanged. Environment
setup and ingestion/joins are implemented. Labels, full label parity, benchmarks,
and a CI job are later phases; no speedup is claimed.

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
Future spot columns are named separately from historical inputs. Volatility,
pricing, fixed-point scaling, and labels are reserved for Phase 3.

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

These are ingestion/join correctness results. Full label parity and labeling
benchmark numbers are not yet available. The separate `data/backfill15m` dataset
is outside this phase's validation scope.
