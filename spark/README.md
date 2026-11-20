# Local Spark port: Phase 1 environment

This additive port uses Ubuntu under WSL2. The pandas pipeline in `src/asl/`,
research outputs, collector, and deployment files remain unchanged. Labeling,
parity, benchmarks, and a CI job are later phases; no speedup is claimed.

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
