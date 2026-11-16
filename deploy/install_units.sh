#!/usr/bin/env bash
# (Re)install systemd units from deploy/*.service|*.timer. Idempotent; safe on every update.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_USER="$(id -un)"
for unit in asl-recorder.service asl-compress.service asl-compress.timer; do
  sed -e "s|__USER__|$RUN_USER|g" -e "s|__REPO__|$REPO|g" "$REPO/deploy/$unit" \
    | sudo tee "/etc/systemd/system/$unit" >/dev/null
done
sudo systemctl daemon-reload
sudo systemctl enable asl-recorder.service >/dev/null
sudo systemctl enable --now asl-compress.timer >/dev/null
# Security updates still install, but must not restart the recorder on their own
# (2026-11-15 06:47 UTC: needrestart restarted it mid-collection). Restarts are ours to do.
if [ -d /etc/needrestart ]; then
  sudo mkdir -p /etc/needrestart/conf.d
  echo "\$nrconf{override_rc}{qr(^asl-recorder\\.service\$)} = 0;" \
    | sudo tee /etc/needrestart/conf.d/asl.conf >/dev/null
fi
echo "== units installed: asl-recorder (enabled), asl-compress.timer (active)"
