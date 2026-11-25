#!/usr/bin/env bash
set -u
cd "$(dirname "$0")"

export PYTHONUNBUFFERED=1
mkdir -p data

rm -f /tmp/supervisor_stop /tmp/reload_restreamer /tmp/restart_all_streams /tmp/daemon_ready /tmp/bot_ready
: > data/bot.log
: > data/restreamer.log

rotate_if_large() {
  local f="$1"
  if [ -f "$f" ]; then
    local sz
    sz=$(wc -c < "$f" 2>/dev/null || echo 0)
    if [ "$sz" -gt 5242880 ]; then
      mv -f "$f" "${f}.prev" 2>/dev/null || : > "$f"
      : > "$f"
    fi
  fi
}

run_forever() {
  local script="$1" logfile="$2" pidfile="$3"
  while [ ! -f /tmp/supervisor_stop ]; do
    rotate_if_large "$logfile"
    echo "[$(date '+%F %T')] [SUPERVISOR] starting $script" >> "$logfile"
    python3 "$script" >> "$logfile" 2>&1 &
    local child=$!
    echo "$child" > "$pidfile"
    wait "$child"
    local rc=$?
    rm -f "$pidfile"
    if [ -f /tmp/supervisor_stop ]; then
      echo "[$(date '+%F %T')] [SUPERVISOR] stopping $script cleanly (rc=$rc)" >> "$logfile"
      break
    fi
    echo "[$(date '+%F %T')] [SUPERVISOR] $script exited (code $rc), restarting in 3s" >> "$logfile"
    sleep 3
  done
}

shutdown_children() {
  local sig="${1:-TERM}"
  touch /tmp/supervisor_stop
  if [ -f /tmp/bot.pid ]; then
    kill -TERM "$(cat /tmp/bot.pid 2>/dev/null)" 2>/dev/null || true
  fi
  if [ -f /tmp/restreamer.pid ]; then
    kill -"$sig" "$(cat /tmp/restreamer.pid 2>/dev/null)" 2>/dev/null || true
  fi
  kill "$BOT_PID" "$STREAM_PID" 2>/dev/null || true
  sleep 1
  pkill -TERM -f "python3.*bot\.py" 2>/dev/null || true
  if [ "$sig" = "USR1" ]; then
    pkill -KILL -x ffmpeg 2>/dev/null || true
    pkill -KILL -x mbuffer 2>/dev/null || true
    pkill -KILL -f "flv_engine\.py" 2>/dev/null || true
  else
    pkill -TERM -f "python3.*restreamer\.py" 2>/dev/null || true
  fi
  exit 0
}

trap 'shutdown_children TERM' TERM INT
trap 'shutdown_children USR1' USR1

echo "Starting Telegram Bot..."
run_forever bot.py data/bot.log /tmp/bot.pid &
BOT_PID=$!

echo "Starting Multi-Stream Restreamer..."
run_forever restreamer.py data/restreamer.log /tmp/restreamer.pid &
STREAM_PID=$!

wait
