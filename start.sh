#!/usr/bin/env bash
set -u
cd "$(dirname "$0")"

mkdir -p data

echo "Starting Telegram Bot..."
python3 bot.py > data/bot.log 2>&1 &
BOT_PID=$!

echo "Starting Multi-Stream Restreamer..."
python3 restreamer.py > data/restreamer.log 2>&1 &
STREAM_PID=$!

trap 'kill $BOT_PID $STREAM_PID 2>/dev/null; exit 0' TERM INT

wait
