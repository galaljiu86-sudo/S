# S (`vps-s`) — Multi-Channel TikTok LIVE -> RTMP Restreamer Cluster Node

Continuous 2-node rotating GitHub Actions restreaming system (`S` <-> `G`) over Tailscale.

## Core Architecture
- **Session-Aware FLV Splicing (`flv_engine.py`)**: Deduplicates overlapping TikTok CDN GOP-cache frames across Streamlink reconnects using independent audio/video watermarks, anchors sequence headers accurately, and realigns CDN clock resets seamlessly.
- **Persistent Downstream Pipelines (`restreamer.py`)**: Keeps one persistent FFmpeg/RTMP connection open per active live stream across Streamlink reconnects, backed by `QueueStreamReader` (10s silence + 12s throughput stall detection) and real-time `run_filler` pacing (`PACE_BUFFER_SEC = 5.0`, `FILLER_KBPS = 3850` for YouTube H.264 copy).
- **Sub-Second Pre-Warmed Handoff (`run.yml` + `start.sh`)**: Standby node boots with zero Telegram `409 Conflict` and zero RTMP collisions, pre-warms IDR-aligned GOP buffers in RAM, and takes over in `< 1s` without emitting `FCUnpublish`.
- **Telegram Management (`bot.py`)**: Multi-channel management with atomic `flock`-protected state (`state_store.py`), inline cancel/delete confirmations, live card refresh, and per-channel FFmpeg logs.

## Required Repository Secrets
> **Important:** Make sure both `S` and `G` GitHub repositories are **Private**, because `data/streams.json` (which contains stream keys) is synced and backed up to the repository.

- `BOT_TOKEN`: Telegram bot token
- `ADMIN_USERNAME`: Admin Telegram username (without `@`)
- `ADMIN_ID` (optional): Numeric Telegram user ID(s), comma-separated
- `PAT`: GitHub Personal Access Token (`repo` + `workflow` scopes)
- `TS_AUTHKEY` (or `TS_OAUTH_CLIENT_ID` + `TS_OAUTH_SECRET`): Tailscale auth credentials
