#!/usr/bin/env bash
# Pull new code, reinstall, test, restart the recorder. Aborts (recorder untouched) if tests fail.
#     bash deploy/update.sh
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
git pull --ff-only
UV="$(command -v uv || echo "$HOME/.local/bin/uv")"
"$UV" pip install --python .venv/bin/python -q -e ".[dev]"
.venv/bin/python -m pytest -q
sudo systemctl restart asl-recorder.service
sleep 5
systemctl is-active asl-recorder.service
git log --oneline -1
