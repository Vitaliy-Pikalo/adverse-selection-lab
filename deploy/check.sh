#!/usr/bin/env bash
# Collector health check. Run any time:  bash deploy/check.sh
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW="$REPO/data/raw"

echo "== service"
systemctl is-active asl-recorder.service && systemctl show asl-recorder.service \
  -p ActiveEnterTimestamp -p NRestarts | sed 's/^/   /'

echo "== last status lines"
journalctl -u asl-recorder.service -n 3 --no-pager -o cat 2>/dev/null | sed 's/^/   /'

echo "== freshest raw files (should be < 2 min old)"
now=$(date +%s)
for src in kalshi coinbase; do
  f=$(ls -t "$RAW"/${src}_*.jsonl* 2>/dev/null | head -1)
  if [ -z "$f" ]; then echo "   $src: NO FILES"; continue; fi
  age=$(( now - $(stat -c %Y "$f") ))
  flag=$([ "$age" -gt 120 ] && echo "  <-- STALE" || echo "")
  echo "   $src: $(basename "$f")  age=${age}s$flag"
done

echo "== compression timer"
systemctl list-timers asl-compress.timer --no-pager 2>/dev/null | sed -n 2p | sed 's/^/   /'
journalctl -u asl-compress.service -n 2 --no-pager -o cat 2>/dev/null | sed 's/^/   /'
echo "   plain .jsonl files waiting: $(ls "$RAW"/*.jsonl 2>/dev/null | wc -l) (normal: 2, the current hour)"

echo "== disk"
du -sh "$RAW" 2>/dev/null | sed 's/^/   raw data: /'
df -h "$REPO" | tail -1 | awk '{print "   free: "$4" of "$2" ("$5" used)"}'

echo "== reboots / clock"
last -x reboot 2>/dev/null | head -3 | sed 's/^/   /'
timedatectl 2>/dev/null | grep -E 'synchronized' | sed 's/^/  /'
