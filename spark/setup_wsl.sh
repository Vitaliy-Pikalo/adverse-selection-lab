#!/usr/bin/env bash
# Run as root inside Ubuntu WSL. Does not use the Windows .venv or touch data.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$(id -u)" -ne 0 ]; then
    echo "Run with: sudo bash spark/setup_wsl.sh" >&2
    exit 1
fi
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends openjdk-17-jdk-headless python3-venv ca-certificates
mkdir -p /opt/asl-spark
python3 -m venv /opt/asl-spark/bootstrap
/opt/asl-spark/bootstrap/bin/python -m pip install --no-cache-dir uv==0.13.9
export UV_CACHE_DIR=/opt/asl-spark/cache
export UV_PYTHON_INSTALL_DIR=/opt/asl-spark/python
export UV_PYTHON_BIN_DIR=/opt/asl-spark/bin
uv_bin=/opt/asl-spark/bootstrap/bin/uv
"$uv_bin" python install 3.11.9
"$uv_bin" venv --allow-existing --python 3.11.9 /opt/asl-spark/venv
"$uv_bin" pip sync --python /opt/asl-spark/venv/bin/python spark/requirements.lock
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
/opt/asl-spark/venv/bin/python -B -m spark.smoke
