#!/usr/bin/env bash
set -u
cd "$(dirname "$0")"

mkdir -p data
: > data/bot.log
: > data/restreamer.log

# كل برنامج بيشتغل في حلقة مستقلة: لو وقع لأي سبب بيرجع يشتغل لوحده بعد 3 ثواني
run_forever() {
  local script="$1" logfile="$2"
  while true; do
    echo "[$(date '+%F %T')] [SUPERVISOR] starting $script" >> "$logfile"
    python3 "$script" >> "$logfile" 2>&1
    echo "[$(date '+%F %T')] [SUPERVISOR] $script exited (code $?), restarting in 3s" >> "$logfile"
    sleep 3
  done
}

echo "Starting Telegram Bot..."
run_forever bot.py data/bot.log &
BOT_PID=$!

echo "Starting Multi-Stream Restreamer..."
run_forever restreamer.py data/restreamer.log &
STREAM_PID=$!

trap 'kill $BOT_PID $STREAM_PID 2>/dev/null; pkill -f "python3 restreamer.py" 2>/dev/null; pkill -f "python3 bot.py" 2>/dev/null; exit 0' TERM INT

wait
