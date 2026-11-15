#!/usr/bin/env bash
# One-time setup of a Linux box as the 24/7 Adverse Selection Lab collector.
#
# Run from the repo root as your normal user (it calls sudo where needed):
#     bash deploy/setup_collector.sh
#
# What it does:
#   1. installs uv (user-level) and a Python 3.11 venv with the project
#   2. runs the test suite
#   3. installs a systemd service (asl-recorder) that starts at boot and restarts on failure
#   4. disables suspend/hibernate and makes lid-close do nothing
#   5. turns on NTP time sync (recv timestamps are only as good as the clock)
# It never touches your .env contents; create that file yourself (see README).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="$(id -un)"
cd "$REPO"
echo "== repo: $REPO  user: $RUN_USER"

# --- 1. python env ---------------------------------------------------------
if ! command -v uv >/dev/null 2>&1 && [ ! -x "$HOME/.local/bin/uv" ]; then
  echo "== installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
UV="$(command -v uv || echo "$HOME/.local/bin/uv")"
"$UV" venv --python 3.11 .venv
"$UV" pip install --python .venv/bin/python -e ".[dev]"

# --- 2. tests --------------------------------------------------------------
echo "== running tests"
.venv/bin/python -m pytest -q

# --- 3. .env check ---------------------------------------------------------
if [ ! -f .env ]; then
  echo "!! .env missing. create it now (KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PATH, KALSHI_ENV=prod),"
  echo "!! then re-run this script. Nothing else was changed."
  exit 1
fi
chmod 600 .env
KEY_PATH="$(grep -E '^KALSHI_PRIVATE_KEY_PATH=' .env | cut -d= -f2- | sed 's/[[:space:]]*#.*$//' | tr -d '"' | xargs)"
if [ -z "$KEY_PATH" ] || [ ! -f "$KEY_PATH" ]; then
  echo "!! KALSHI_PRIVATE_KEY_PATH in .env does not point to a file on this machine: '$KEY_PATH'"
  exit 1
fi
chmod 600 "$KEY_PATH"

# --- 4. systemd service ----------------------------------------------------
echo "== installing systemd service asl-recorder (sudo)"
sed -e "s|__USER__|$RUN_USER|g" -e "s|__REPO__|$REPO|g" deploy/asl-recorder.service \
  | sudo tee /etc/systemd/system/asl-recorder.service >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable asl-recorder.service

# --- 5. never sleep --------------------------------------------------------
echo "== disabling sleep/hibernate and lid-close suspend (sudo)"
sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target
sudo mkdir -p /etc/systemd/logind.conf.d
printf '[Login]\nHandleLidSwitch=ignore\nHandleLidSwitchExternalPower=ignore\nHandleLidSwitchDocked=ignore\nIdleAction=ignore\n' \
  | sudo tee /etc/systemd/logind.conf.d/asl-no-sleep.conf >/dev/null
echo "   (logind change applies after next reboot; it will NOT log you out now)"

# --- 6. clock --------------------------------------------------------------
echo "== enabling NTP (sudo)"
sudo timedatectl set-ntp true || echo "   timedatectl failed; check time sync manually"
timedatectl | grep -E 'synchronized|NTP service' || true

echo
echo "== done. start collecting with:   sudo systemctl start asl-recorder"
echo "   live log:                      journalctl -u asl-recorder -f"
echo "   health check:                  bash deploy/check.sh"
