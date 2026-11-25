#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Multi-Channel TikTok LIVE -> YouTube / Meta / Custom RTMP Restreamer.

Applies the proven single-channel persistent FLV architecture at multi-channel scale:
- One persistent FFmpeg/RTMP publisher per active TikTok LIVE.
- FlvSessionSplicer deduplicates overlapping TikTok CDN GOP-cache packets across
  Streamlink reconnects while preserving continuous timeline alignment.
- QueueStreamReader enforces 10s silence + 12s throughput stall detection.
- flv_engine.py pacing/filler stage provides 5.0s pre-buffered real-time pacing
  and optional H.264 NALU bitrate padding (3850k) for YouTube H.264 copy mode.
- Standby/Prewarm gate allows sub-second 2-node handoffs with zero dual-RTMP
  collisions, zero Telegram 409 conflicts, and zero RTMP FCUnpublish drops.
"""

from __future__ import annotations

import asyncio
import gc
import html
import http.server
import json
import os
import queue
import shlex
import signal
import socketserver
import subprocess
import sys
import threading
import time
from typing import Optional

import requests
import streamlink

from flv_engine import (
    FlvSessionSplicer,
    PACE_BUFFER_SEC,
    PrewarmRingBuffer,
    PrewarmSink,
    QueueStreamReader,
    StreamStalled,
    env_value,
    run_filler,
)
from state_store import (
    DATA_DIR,
    ensure_data_dir,
    load_global_config,
    load_streams,
    prune_statuses,
    save_global_config,
    save_streams,
    update_stream_status,
)

try:
    import resource
except ImportError:  # Windows test compatibility
    resource = None

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import ConnectEvent, LiveEndEvent

    TIKTOKLIVE_IMPORT_ERROR = None
except Exception as exc:
    TikTokLiveClient = None
    TIKTOKLIVE_IMPORT_ERROR = exc


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MY_NAME = env_value("MY_NAME", "Runner")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

QUALITY = env_value("QUALITY", "h264_hd")
ALLOW_H264_FALLBACK = env_value("ALLOW_H264_FALLBACK", 0, int)
COPY_VIDEO = env_value("COPY_VIDEO", 1, int)
FILLER_KBPS = env_value("FILLER_KBPS", 3850, int)

AUDIO_BITRATE = env_value("AUDIO_BITRATE", "128k")
AUDIO_RATE = env_value("AUDIO_RATE", 44100, int)
AUDIO_CHANNELS = env_value("AUDIO_CHANNELS", 2, int)
if AUDIO_RATE not in (44100, 48000):
    raise ValueError("AUDIO_RATE must be 44100 or 48000")
if AUDIO_CHANNELS != 2:
    raise ValueError("AUDIO_CHANNELS must be 2 (stereo)")

BUF_MB = env_value("BUF_MB", 8, int)
PREFILL_PCT = env_value("PREFILL_PCT", 0, int)

VIDEO_BITRATE = env_value("VIDEO_BITRATE", "2500k")
META_BITRATE = env_value("META_BITRATE", "2500k")
FPS = env_value("FPS", 30, int)
X264_PRESET = env_value("X264_PRESET", "superfast")
REENCODE_THREADS = env_value("REENCODE_THREADS", 2, int)
GOP = max(1, FPS * 2)

SLATE_SEC = env_value("SLATE_SEC", 0, int)
SLATE_CODEC = "h264"
SLATE_SIZE = env_value("SLATE_SIZE", "1080x1920")
SLATE_FPS = env_value("SLATE_FPS", 30, int)
SLATE_MIN_OFFLINE = env_value("SLATE_MIN_OFFLINE", 120, int)
SLATE_DIR = env_value("SLATE_DIR", os.path.dirname(os.path.abspath(__file__)))
SLATE_PATH = os.path.join(
    SLATE_DIR,
    f"slate_{SLATE_CODEC}_{SLATE_SIZE}_{SLATE_FPS}.flv",
)

CHECK_INTERVAL = env_value("CHECK_INTERVAL", 3.0, float)
MIN_TIKTOK_API_GAP = env_value("MIN_TIKTOK_API_GAP", 1.5, float)
LIVE_END_COOLDOWN = env_value("LIVE_END_COOLDOWN", 15, int)
RESTART_DELAY = env_value("RESTART_DELAY", 0.2, float)
CRASH_DELAY = env_value("CRASH_DELAY", 2.0, float)

RSS_LIMIT_MB = env_value("RSS_LIMIT_MB", 450, int)
SESSION_TTL_SEC = env_value("SESSION_TTL_SEC", 600, int)
MEMORY_CHECK_INTERVAL = env_value("MEMORY_CHECK_INTERVAL", 30, int)
MEMORY_CLEANUP_COOLDOWN = env_value("MEMORY_CLEANUP_COOLDOWN", 120, int)

STREAM_STALL_SECONDS = env_value("STREAM_STALL_SECONDS", 10.0, float)
SOURCE_QUEUE_CHUNKS = env_value("SOURCE_QUEUE_CHUNKS", 16, int)
OFFLINE_CONFIRMATIONS = env_value("OFFLINE_CONFIRMATIONS", 4, int)
OFFLINE_CONFIRM_INTERVAL = env_value("OFFLINE_CONFIRM_INTERVAL", 2.5, float)
WS_OFFLINE_FAILSAFE_SECONDS = env_value("WS_OFFLINE_FAILSAFE_SECONDS", 120.0, float)

FAST_REOPEN_TRIES = env_value("FAST_REOPEN_TRIES", 0, int)
FAST_REOPEN_MIN_SESSION_SEC = env_value("FAST_REOPEN_MIN_SESSION_SEC", 2.0, float)

CONTROL_PORT = env_value("CONTROL_PORT", 9001, int)
STANDBY_FLAG_PATH = env_value("STANDBY_FLAG_PATH", "/tmp/standby_mode")
BOT_READY_PATH = env_value("BOT_READY_PATH", "/tmp/bot_ready")
REQUIRE_BOT_READY = env_value("REQUIRE_BOT_READY", 1, int)
PREWARM_READY_WAIT_SEC = env_value("PREWARM_READY_WAIT_SEC", 10.0, float)
STANDBY_MAX_SEC = env_value("STANDBY_MAX_SEC", 180.0, float)

FLV_ENGINE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flv_engine.py")

TIKTOK_API_LOCK = threading.Lock()
LAST_TIKTOK_API_CALL = 0.0
SHUTDOWN_EVENT = threading.Event()
HANDOFF_QUIESCE_EVENT = threading.Event()
CLUSTER_ACTIVE_EVENT = threading.Event()
DAEMON_BOOT_TIME = time.monotonic()
DAEMON_READY_EVENT = threading.Event()

WORKERS_LOCK = threading.Lock()
WORKERS: dict[str, dict] = {}


def log(message: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def get_rss_mb() -> float:
    try:
        with open("/proc/self/statm", "r", encoding="utf-8") as handle:
            fields = handle.read().split()
        resident_pages = int(fields[1])
        page_size = os.sysconf("SC_PAGE_SIZE")
        return resident_pages * page_size / (1024 * 1024)
    except Exception:
        if resource is None:
            return 0.0
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def notify_telegram(message: str) -> None:
    if not BOT_TOKEN:
        return
    try:
        cfg = load_global_config()
        chat_id = cfg.get("chat_id")
        if chat_id:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            requests.post(
                url,
                json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                timeout=5,
            )
    except Exception as exc:
        log(f"[NOTIFY_ERROR] {exc}")


# ---------------------------------------------------------------------------
# FFmpeg pipeline builder and slate generator
# ---------------------------------------------------------------------------

def build_persistent_pipeline(
    target: str,
    platform: str,
    layout: str,
    use_slate: bool = False,
) -> str:
    if BUF_MB <= 0 or not 0 <= PREFILL_PCT <= 100:
        raise ValueError("BUF_MB must be > 0 and PREFILL_PCT must be 0..100")

    is_meta = platform in ("Facebook", "Instagram")
    needs_reencode = (
        is_meta
        or (not COPY_VIDEO)
        or (layout in ("landscape_to_portrait_pad", "portrait_stable"))
    )

    buffer_cmd = ["mbuffer", "-q", "-m", f"{BUF_MB}M"]
    if PREFILL_PCT > 0:
        buffer_cmd += ["-P", str(PREFILL_PCT)]

    audio_options = [
        "-c:a", "aac",
        "-profile:a", "aac_low",
        "-b:a", AUDIO_BITRATE,
        "-ar", str(AUDIO_RATE),
        "-ac", str(AUDIO_CHANNELS),
        "-af", "aresample=async=1:min_hard_comp=0.100",
    ]

    if not needs_reencode:
        video_options = ["-c:v", "copy"]
        filler_kbps = FILLER_KBPS if (FILLER_KBPS > 0 and layout != "landscape_raw") else 0
    else:
        if layout == "landscape_to_portrait_pad":
            # Fixed 180% center zoom on 720x1280 portrait black canvas
            vf_filter = (
                f"fps={FPS},"
                "scale=w=1296:h=-2:flags=bicubic,"
                "crop=w=min(iw\\,720):h=min(ih\\,1280):x=(iw-ow)/2:y=(ih-oh)/2,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
                "setsar=1,"
                "format=yuv420p"
            )
        elif layout == "landscape_raw":
            vf_filter = (
                f"fps={FPS},"
                "scale=w=1280:h=720:force_original_aspect_ratio=decrease,"
                "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,"
                "setsar=1,"
                "format=yuv420p"
            )
        else:
            vf_filter = (
                f"fps={FPS},"
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
                "setsar=1,"
                "format=yuv420p"
            )

        bitrate = META_BITRATE if is_meta else VIDEO_BITRATE
        video_options = [
            "-vf", vf_filter,
            "-c:v", "libx264",
            "-preset", X264_PRESET,
            "-threads", str(max(0, REENCODE_THREADS)),
            "-profile:v", "high",
            "-level", "4.1",
            "-b:v", bitrate,
            "-minrate", bitrate,
            "-maxrate", bitrate,
            "-bufsize", bitrate,
            "-x264-params", "nal-hrd=cbr:force-cfr=1:scenecut=0",
            "-r", str(FPS),
            "-fps_mode", "cfr",
            "-g", str(GOP),
            "-keyint_min", str(GOP),
            "-sc_threshold", "0",
        ]
        filler_kbps = 0

    use_pacer_stage = (filler_kbps > 0) or (PACE_BUFFER_SEC > 0)

    ffmpeg_1 = [
        "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
        "-stats", "-stats_period", "30",
        "-analyzeduration", "10M", "-probesize", "10M",
        "-fflags", "+genpts+discardcorrupt",
        "-i", "pipe:0",
        "-map", "0:v:0", "-map", "0:a:0?",
        *video_options,
        *audio_options,
        "-max_muxing_queue_size", "4096" if needs_reencode else "2048",
        "-flvflags", "no_duration_filesize",
        "-f", "flv", "pipe:1" if use_pacer_stage else target,
    ]
    if needs_reencode:
        ffmpeg_1 = ["nice", "-n", "10", *ffmpeg_1]

    commands = [buffer_cmd, ffmpeg_1]

    if use_pacer_stage:
        filler_cmd = [sys.executable, FLV_ENGINE_PATH, "--filler", str(filler_kbps)]
        if use_slate and filler_kbps > 0 and SLATE_SEC > 0 and os.path.isfile(SLATE_PATH):
            filler_cmd += ["--slate", SLATE_PATH, str(SLATE_SEC)]
        commands.append(filler_cmd)

        ffmpeg_2 = [
            "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
            "-f", "flv", "-i", "pipe:0",
            "-c", "copy",
            "-flvflags", "no_duration_filesize",
            "-f", "flv", target,
        ]
        commands.append(ffmpeg_2)

    return "set -o pipefail; " + " | ".join(shlex.join(cmd) for cmd in commands)


def ensure_slate() -> None:
    if SLATE_SEC <= 0 or os.path.exists(SLATE_PATH):
        return

    log(f"[SLATE] Building H.264 slate {SLATE_SIZE} at {SLATE_FPS} FPS.")
    temporary_path = SLATE_PATH + ".tmp"
    command = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", f"color=c=black:s={SLATE_SIZE}:r={SLATE_FPS}",
        "-f", "lavfi", "-i", f"anullsrc=r={AUDIO_RATE}:cl=stereo",
        "-t", str(max(SLATE_SEC + 12, 20)),
        "-c:v", "libx264", "-preset", "ultrafast",
        "-g", str(SLATE_FPS), "-sc_threshold", "0",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-profile:a", "aac_low", "-b:a", AUDIO_BITRATE,
        "-ar", str(AUDIO_RATE), "-ac", str(AUDIO_CHANNELS),
        "-f", "flv", temporary_path,
    ]
    try:
        subprocess.run(command, check=True)
        os.replace(temporary_path, SLATE_PATH)
        log("[SLATE] Slate ready.")
    except Exception as exc:
        try:
            if os.path.exists(temporary_path):
                os.remove(temporary_path)
        except OSError:
            pass
        log(f"[SLATE] Failed to build slate: {exc}")


# ---------------------------------------------------------------------------
# Streamlink session and live-end monitoring
# ---------------------------------------------------------------------------

def make_session():
    session = streamlink.Streamlink()
    try:
        session.set_option("stream-timeout", 15)
        session.set_option("http-timeout", 10)
        session.set_option(
            "http-headers",
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Referer": "https://www.tiktok.com/",
            },
        )
    except Exception as exc:
        log(f"[WARN] Could not set Streamlink session options: {exc}")
    return session


def select_stream(streams: dict, needs_reencode: bool):
    if QUALITY in streams:
        return QUALITY, streams[QUALITY]
    if needs_reencode:
        for candidate in ("h264_hd", "h264_uhd", "origin", "hd", "best"):
            if candidate in streams:
                return candidate, streams[candidate]
    elif ALLOW_H264_FALLBACK:
        for candidate in ("h264_uhd", "h264_sd", "h264_ld"):
            if candidate in streams:
                return candidate, streams[candidate]
    return None, None


class LiveEndMonitor(threading.Thread):
    """WebSocket monitor active only while a channel is actually live."""

    def __init__(self, owner: "ChannelRelay"):
        super().__init__(daemon=True, name=f"ws-{owner.user}")
        self.owner = owner

    def run(self) -> None:
        asyncio.run(self._main())

    async def _main(self) -> None:
        while not self.owner.stop_event.is_set() and not SHUTDOWN_EVENT.is_set():
            # Avoid hammering TikTokLive endpoints while the channel is offline.
            if not self.owner.ws_should_connect.is_set():
                self.owner.ws_connected.clear()
                self.owner.stop_event.wait(1.0)
                continue

            try:
                client = TikTokLiveClient(unique_id=f"@{self.owner.user}")

                @client.on(ConnectEvent)
                async def on_connect(event):
                    self.owner.ws_connected.set()
                    log(f"[{self.owner.user}] [WS] Connected to TikTok LIVE event monitor.")

                @client.on(LiveEndEvent)
                async def on_live_end(event):
                    log(f"[{self.owner.user}] [WS] LiveEndEvent received; verifying source (3/5 offline checks)...")
                    offline_confirmations = 0
                    for attempt in range(1, 6):
                        if self.owner.stop_event.is_set() or SHUTDOWN_EVENT.is_set():
                            return
                        await asyncio.sleep(3)
                        streams = await asyncio.to_thread(
                            self.owner.fetch_source_streams, True
                        )
                        if streams is None:
                            continue
                        if streams:
                            log(f"[{self.owner.user}] [WS] False alarm ignored at check {attempt}/5.")
                            return
                        offline_confirmations += 1
                        if offline_confirmations >= 3:
                            self.owner.on_live_ended("تأكيد انتهاء البث عبر WebSocket + 3 فحوصات")
                            return

                if await client.is_live():
                    await client.connect()
                self.owner.ws_connected.clear()
            except Exception as exc:
                self.owner.ws_connected.clear()
                if not self.owner.stop_event.is_set() and not SHUTDOWN_EVENT.is_set():
                    log(f"[{self.owner.user}] [WS] Monitor reconnecting: {exc}")
            self.owner.stop_event.wait(5)


# ---------------------------------------------------------------------------
# Per-channel relay worker
# ---------------------------------------------------------------------------

class ChannelRelay:
    def __init__(self, stream_data: dict, slate_thread: Optional[threading.Thread] = None):
        self.data = dict(stream_data)
        self.sid = stream_data["id"]
        self.user = stream_data["tiktok_user"]
        self.tiktok_url = f"https://www.tiktok.com/@{self.user}/live"
        self.target = stream_data["target_url"]
        self.platform = stream_data.get("platform", "YouTube")
        self.layout = stream_data.get("layout", "portrait_copy")
        self.slate_thread = slate_thread

        self.is_meta = self.platform in ("Facebook", "Instagram")
        self.needs_reencode = (
            self.is_meta
            or (not COPY_VIDEO)
            or (self.layout in ("landscape_to_portrait_pad", "portrait_stable"))
        )

        self.stop_event = threading.Event()
        self.live_ended_event = threading.Event()
        self.ws_should_connect = threading.Event()
        self.ws_connected = threading.Event()
        self.session_lock = threading.Lock()
        self.session = make_session()
        self.session_created_at = time.monotonic()

        self.proc_lock = threading.Lock()
        self.promote_lock = threading.Lock()
        self.proc: Optional[subprocess.Popen] = None
        self.current_fd = None
        self.current_fd_lock = threading.Lock()

        self.flv_splicer: Optional[FlvSessionSplicer] = None
        self.prewarm_ring: Optional[PrewarmRingBuffer] = None
        self.prewarm_sink: Optional[PrewarmSink] = None
        self.pipeline_live = False
        self.prewarming = False

        self.last_live_end: Optional[float] = None
        self.live_started_at: Optional[float] = None
        self.session_number = 0
        self.total_dup_skipped = 0
        self.last_memory_check = 0.0
        self.last_memory_cleanup = 0.0
        self.last_wait_log = 0.0
        self.last_quality_log = 0.0
        self.last_status_error_log = 0.0
        self.end_reason = ""
        self.last_error = ""
        self.last_downstream_notify = -1e9
        self.announced = False
        self.monitor_started = False

    def _rebuild_session_unlocked(self) -> None:
        old_session = self.session
        try:
            old_session.http.close()
        except Exception:
            pass
        self.session = make_session()
        self.session_created_at = time.monotonic()

    def fetch_source_streams(self, priority: bool = False):
        global LAST_TIKTOK_API_CALL
        with self.session_lock:
            now = time.monotonic()
            if now - self.session_created_at >= SESSION_TTL_SEC:
                self._rebuild_session_unlocked()
            if priority:
                LAST_TIKTOK_API_CALL = time.monotonic()
            else:
                with TIKTOK_API_LOCK:
                    wait = MIN_TIKTOK_API_GAP - (time.monotonic() - LAST_TIKTOK_API_CALL)
                    if wait > 0:
                        if self.stop_event.wait(wait) or SHUTDOWN_EVENT.is_set():
                            return None
                    LAST_TIKTOK_API_CALL = time.monotonic()
            try:
                return self.session.streams(self.tiktok_url)
            except Exception as exc:
                wall_now = time.time()
                if wall_now - self.last_status_error_log >= 60:
                    log(f"[{self.user}] [CHECK] TikTok status request failed: {exc}")
                    self.last_status_error_log = wall_now
                return None

    def maintain_memory(self) -> None:
        now = time.monotonic()
        if now - self.last_memory_check < max(5, MEMORY_CHECK_INTERVAL):
            return
        self.last_memory_check = now

        with self.session_lock:
            if now - self.session_created_at >= SESSION_TTL_SEC:
                self._rebuild_session_unlocked()

            rss_mb = get_rss_mb()
            with WORKERS_LOCK:
                limit_mb = max(RSS_LIMIT_MB, 250 + 80 * max(1, len(WORKERS)))
            if rss_mb <= limit_mb:
                return

            if now - self.last_memory_cleanup < max(30, MEMORY_CLEANUP_COOLDOWN):
                return

            self.last_memory_cleanup = now
            log(
                f"[{self.user}] [MEM] RSS is {rss_mb:.1f} MiB (limit {limit_mb} MiB); "
                "rotating Streamlink session and collecting garbage."
            )
            self._rebuild_session_unlocked()
            gc.collect()

    def start_pipeline(self, use_slate: bool = False) -> None:
        with self.proc_lock:
            if self.proc is not None:
                return
            log(f"[{self.user} | {self.platform}] Starting ONE persistent FFmpeg/RTMP pipeline ({self.layout})...")
            err_file = f"/tmp/pipeline_err_{self.sid}.log"
            ef = open(err_file, "w", encoding="utf-8")
            try:
                self.proc = subprocess.Popen(
                    build_persistent_pipeline(self.target, self.platform, self.layout, use_slate=use_slate),
                    shell=True,
                    executable="/bin/bash",
                    stdin=subprocess.PIPE,
                    stderr=ef,
                    start_new_session=True,
                )
            finally:
                ef.close()
            if self.proc.stdin is None:
                raise RuntimeError("Could not open persistent pipeline stdin")

    def write_pipeline_bytes(self, data: bytes) -> None:
        process = self.proc
        if process is None or process.stdin is None:
            raise BrokenPipeError("Persistent FFmpeg pipeline is not running")
        if process.poll() is not None:
            raise BrokenPipeError(f"Persistent pipeline exited with code {process.returncode}")
        process.stdin.write(data)
        process.stdin.flush()

    def stop_pipeline(self, reason: str = "shutdown", abrupt_handoff: bool = False) -> None:
        with self.proc_lock:
            process, self.proc = self.proc, None
        if process is None:
            return

        if abrupt_handoff:
            # Handoff cutover: drop TCP immediately with SIGKILL so ffmpeg never emits RTMP FCUnpublish.
            log(f"[{self.user}] [HANDOFF] Fast-terminating RTMP publisher without FCUnpublish ({reason}).")
            try:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
            except Exception:
                pass
            return

        log(f"[{self.user} | {self.platform}] Closing persistent pipeline ({reason})...")
        try:
            if process.stdin:
                process.stdin.close()
        except (BrokenPipeError, OSError, ValueError):
            pass

        drain_timeout = (
            max(35.0, PACE_BUFFER_SEC * 5)
            if "ended" in reason
            else 8.0
        )
        try:
            process.wait(timeout=drain_timeout)
            log(f"[{self.user}] [PROC] Pipeline drained and exited with code {process.returncode}.")
            return
        except subprocess.TimeoutExpired:
            log(f"[{self.user}] [PROC] Pipeline did not exit within {drain_timeout:.0f}s; terminating.")

        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=4)
        except Exception:
            try:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
            except Exception:
                pass

    def promote_from_standby(self) -> bool:
        """Promote a pre-warmed channel into an active persistent RTMP pipeline."""
        with self.promote_lock:
            if not self.prewarming or self.prewarm_sink is None or self.pipeline_live:
                return False
            try:
                self.start_pipeline(use_slate=False)
                flushed = self.prewarm_sink.activate(self.write_pipeline_bytes)
                self.prewarming = False
                self.pipeline_live = True
                self.live_started_at = time.time()
                self.announced = True
                self.ws_should_connect.set()
                self._sync_status(True)
                log(f"[{self.user}] [PREWARM] Activated RTMP publisher cleanly (flushed {flushed}B IDR-aligned GOP).")
                return True
            except Exception as exc:
                self.last_error = str(exc)
                log(f"[{self.user}] [PREWARM] Activation failed: {exc}")
                self.stop_pipeline("prewarm activation failure")
                return False

    def is_prewarmed(self) -> bool:
        ring = self.prewarm_ring
        return bool(self.prewarming and ring is not None and ring.is_warm())

    def pipeline_error_tail(self, lines: int = 12) -> str:
        try:
            with open(f"/tmp/pipeline_err_{self.sid}.log", "r", encoding="utf-8", errors="replace") as f:
                data = f.read()[-2000:].strip().splitlines()[-lines:]
            return "\n".join(data)
        except Exception:
            return ""

    def report_downstream_exit(self, code) -> None:
        tail = self.pipeline_error_tail()
        self.last_error = tail.splitlines()[-1] if tail else f"exit={code}"
        log(f"[{self.user}] [PROC] Downstream exited ({code}); restarting. stderr tail:\n{tail}")
        now = time.monotonic()
        if now - self.last_downstream_notify >= 600:
            self.last_downstream_notify = now
            detail = html.escape(tail[-350:]) if tail else f"exit code {code}"
            notify_telegram(
                f"⚠️ <b>انقطع الإرسال إلى {self.platform} (@{self.user})</b>\n"
                f"<pre>{detail}</pre>\n🔄 جاري إعادة الاتصال تلقائياً..."
            )

    def _sync_status(self, is_live: bool) -> None:
        update_stream_status(
            self.sid,
            self.user,
            is_live,
            extra={
                "platform": self.platform,
                "layout": self.layout,
                "sessions": self.session_number,
                "dup_skipped": self.total_dup_skipped,
                "live_since": self.live_started_at if is_live else None,
                "prewarming": self.prewarming,
                "last_error": self.last_error,
            },
        )

    def on_live_ended(self, reason: str = "") -> None:
        if reason:
            self.end_reason = reason
            log(f"[{self.user}] [LIVE_END] {reason}")
        self.live_ended_event.set()
        with self.current_fd_lock:
            fd = self.current_fd
        if fd is not None:
            try:
                fd.close()
            except Exception:
                pass

    def stream_session(self, stream, session_number: int) -> bool:
        started = time.monotonic()
        ts_before = (
            self.flv_splicer.last_output_timestamp
            if self.flv_splicer is not None else -1
        )
        try:
            fd = stream.open()
        except Exception as exc:
            log(f"[{self.user}] [SOURCE] Could not open Streamlink session: {exc}")
            return False

        with self.current_fd_lock:
            self.current_fd = fd

        chunks: queue.Queue = queue.Queue(maxsize=max(4, SOURCE_QUEUE_CHUNKS))
        session_done = threading.Event()

        def should_stop() -> bool:
            return (
                self.stop_event.is_set()
                or self.live_ended_event.is_set()
                or SHUTDOWN_EVENT.is_set()
                or session_done.is_set()
            )

        def enqueue(item) -> bool:
            while not should_stop():
                try:
                    chunks.put(item, timeout=0.5)
                    return True
                except queue.Full:
                    continue
            return False

        def reader_thread() -> None:
            try:
                while not should_stop():
                    block = fd.read(64 * 1024)
                    if not block:
                        enqueue(("eof", None))
                        return
                    if not enqueue(("data", block)):
                        return
            except Exception as exc:
                if not should_stop():
                    enqueue(("error", exc))

        threading.Thread(
            target=reader_thread,
            name=f"sl-reader-{self.sid}-{session_number}",
            daemon=True,
        ).start()

        reader = QueueStreamReader(
            chunks,
            self.stop_event,
            self.live_ended_event,
            STREAM_STALL_SECONDS,
        )

        try:
            if self.flv_splicer is None:
                raise RuntimeError("FLV splicer was not initialized")

            result = self.flv_splicer.process_handle(
                reader,
                source_name=f"{self.user} session {session_number}",
            )
            if result.get("empty"):
                log(f"[{self.user}] [SOURCE] Session {session_number} ended before new media arrived.")
                return False

            self.total_dup_skipped += int(result.get("dropped_dup", 0))
            log(
                f"[{self.user}] [SPLICER] S{session_number}: offset={result['offset']}ms, "
                f"dup_skipped={result['dropped_dup']}, "
                f"dropped_video={result['dropped_video']}, "
                f"audio={result['audio_written']}, "
                f"video={result['video_written']}, "
                f"first_IDR={result['first_idr']}ms"
            )
        except StreamStalled as exc:
            log(f"[{self.user}] [SOURCE] {exc}; closing session and reconnecting.")
        except BrokenPipeError as exc:
            log(f"[{self.user}] [PROC] Persistent pipeline closed input: {exc}")
        except (EOFError, OSError, ValueError, RuntimeError, InterruptedError) as exc:
            if not self.stop_event.is_set() and not self.live_ended_event.is_set() and not SHUTDOWN_EVENT.is_set():
                log(f"[{self.user}] [SOURCE] Session {session_number} ended: {exc}")
        finally:
            session_done.set()
            with self.current_fd_lock:
                if self.current_fd is fd:
                    self.current_fd = None
            try:
                fd.close()
            except Exception:
                pass

        wrote_media = (
            self.flv_splicer is not None
            and self.flv_splicer.last_output_timestamp > ts_before
        )
        return bool(
            wrote_media
            and time.monotonic() - started >= FAST_REOPEN_MIN_SESSION_SEC
        )

    def run(self) -> None:
        log(f"[{self.user} | {self.platform}] Channel relay started (layout={self.layout}).")
        if TikTokLiveClient and self.user and not self.monitor_started:
            LiveEndMonitor(self).start()
            self.monitor_started = True

        offline_count = 0
        offline_since = None

        try:
            while not self.stop_event.is_set() and not SHUTDOWN_EVENT.is_set():
                self.maintain_memory()

                # Promote prewarmed channel immediately if cluster became active.
                if self.prewarming and CLUSTER_ACTIVE_EVENT.is_set():
                    if not self.promote_from_standby():
                        self.prewarming = False
                        self.prewarm_ring = None
                        self.prewarm_sink = None
                        self.flv_splicer = None
                        self.stop_event.wait(CRASH_DELAY)
                        continue

                if self.live_ended_event.is_set():
                    self.ws_should_connect.clear()
                    if self.pipeline_live:
                        self.stop_pipeline("TikTok LIVE ended")
                    self.pipeline_live = False
                    self.prewarming = False
                    self.prewarm_ring = None
                    self.prewarm_sink = None
                    self.flv_splicer = None
                    self.announced = False
                    self.live_started_at = None
                    self.last_live_end = time.time()
                    self._sync_status(False)

                    why = f"\n• <b>السبب:</b> {html.escape(self.end_reason)}" if self.end_reason else ""
                    self.end_reason = ""
                    if CLUSTER_ACTIVE_EVENT.is_set():
                        notify_telegram(
                            f"⏹️ <b>انتهى البث المباشر لحساب @{self.user}.</b>\n"
                            f"تم إيقاف النقل إلى {self.platform}.{why}"
                        )
                    self.session_number = 0
                    offline_count = 0
                    offline_since = None
                    self.live_ended_event.clear()
                    self.stop_event.wait(LIVE_END_COOLDOWN)
                    continue

                is_streaming = self.pipeline_live or self.prewarming
                streams = self.fetch_source_streams(priority=is_streaming)

                if streams is None:
                    if is_streaming and offline_since is not None:
                        if (
                            self.ws_connected.is_set()
                            and time.monotonic() - offline_since >= WS_OFFLINE_FAILSAFE_SECONDS
                        ):
                            self.on_live_ended(
                                f"تعذّر التحقق من TikTok وتجاوزت مهلة الأمان ({WS_OFFLINE_FAILSAFE_SECONDS:.0f}s)"
                            )
                            continue
                    self.stop_event.wait(RESTART_DELAY if is_streaming else CHECK_INTERVAL)
                    continue

                if not streams:
                    if not is_streaming:
                        if self.last_wait_log == 0 or time.time() - self.last_wait_log >= 120:
                            log(f"[{self.user}] Offline; waiting for TikTok LIVE.")
                            self.last_wait_log = time.time()
                        self._sync_status(False)
                        self.stop_event.wait(CHECK_INTERVAL)
                        continue

                    if offline_since is None:
                        offline_since = time.monotonic()
                    offline_count += 1
                    log(f"[{self.user}] [CHECK] No streams returned: {offline_count}/{OFFLINE_CONFIRMATIONS}")

                    if not self.ws_connected.is_set() and offline_count >= OFFLINE_CONFIRMATIONS:
                        self.on_live_ended("تأكد توقف البث من TikTok عبر الفحص الدوري")
                        continue

                    if (
                        self.ws_connected.is_set()
                        and time.monotonic() - offline_since >= WS_OFFLINE_FAILSAFE_SECONDS
                    ):
                        self.on_live_ended("تجاوزت مهلة انتظار WebSocket مع استمرار انقطاع البث")
                        continue

                    self.stop_event.wait(OFFLINE_CONFIRM_INTERVAL)
                    continue

                offline_count = 0
                offline_since = None
                self.last_wait_log = 0.0

                chosen_q, stream_obj = select_stream(streams, self.needs_reencode)
                if stream_obj is None:
                    now = time.time()
                    if now - self.last_quality_log >= 30:
                        available = ", ".join(sorted(streams.keys()))
                        log(f"[{self.user}] [SOURCE] LIVE is up but {QUALITY} unavailable. Available: {available}")
                        self.last_quality_log = now
                    self.stop_event.wait(RESTART_DELAY)
                    continue

                if not self.pipeline_live and not self.prewarming:
                    self.live_ended_event.clear()
                    self.last_error = ""
                    self.session_number = 0
                    self.total_dup_skipped = 0

                    if not CLUSTER_ACTIVE_EVENT.is_set():
                        # STANDBY mode: pre-warm IDR-aligned GOP in RAM with zero RTMP push.
                        self.prewarm_ring = PrewarmRingBuffer()
                        self.prewarm_sink = PrewarmSink(self.prewarm_ring)
                        self.flv_splicer = FlvSessionSplicer(
                            self.prewarm_sink.write,
                            allow_hevc=self.needs_reencode,
                        )
                        self.prewarming = True
                        self.ws_should_connect.set()
                        self._sync_status(False)
                        log(f"[{self.user}] [PREWARM] Buffering IDR-aligned GOP in standby (quality={chosen_q}).")
                    else:
                        offline_for = (
                            None if self.last_live_end is None
                            else time.time() - self.last_live_end
                        )
                        use_slate = bool(
                            not self.needs_reencode
                            and FILLER_KBPS > 0
                            and SLATE_SEC > 0
                            and offline_for is not None
                            and offline_for >= SLATE_MIN_OFFLINE
                        )
                        if use_slate and self.slate_thread is not None and self.slate_thread.is_alive():
                            self.slate_thread.join(timeout=60)

                        self.flv_splicer = FlvSessionSplicer(
                            self.write_pipeline_bytes,
                            allow_hevc=self.needs_reencode,
                        )
                        try:
                            self.start_pipeline(use_slate=use_slate)
                        except Exception as exc:
                            self.last_error = str(exc)
                            log(f"[{self.user}] [PROC] Could not start persistent pipeline: {exc}")
                            self.stop_pipeline("pipeline startup failure")
                            self.flv_splicer = None
                            self.stop_event.wait(CRASH_DELAY)
                            continue

                        self.pipeline_live = True
                        self.live_started_at = time.time()
                        self.ws_should_connect.set()
                        self._sync_status(True)
                        log(f"[{self.user}] [LIVE] Persistent RTMP publisher open (quality={chosen_q}).")
                        if not self.announced:
                            self.announced = True
                            notify_telegram(
                                f"🔴 <b>بدأ البث المباشر لحساب @{self.user}!</b>\n"
                                f"• <b>المنصة:</b> {self.platform}\n"
                                f"🚀 جاري البث المستمر بأعلى ثبات!"
                            )

                if self.pipeline_live and self.proc is not None and self.proc.poll() is not None:
                    self.report_downstream_exit(self.proc.returncode)
                    self.stop_pipeline("downstream exited")
                    self.pipeline_live = False
                    self.flv_splicer = None
                    self._sync_status(False)
                    self.stop_event.wait(CRASH_DELAY)
                    continue

                self.session_number += 1
                productive = self.stream_session(stream_obj, self.session_number)

                fast_tries = 0
                while (
                    productive
                    and fast_tries < FAST_REOPEN_TRIES
                    and not self.stop_event.is_set()
                    and not self.live_ended_event.is_set()
                    and not SHUTDOWN_EVENT.is_set()
                    and (self.prewarming or (self.proc is not None and self.proc.poll() is None))
                ):
                    fast_tries += 1
                    self.session_number += 1
                    productive = self.stream_session(stream_obj, self.session_number)

                if self.stop_event.is_set() or self.live_ended_event.is_set() or SHUTDOWN_EVENT.is_set():
                    continue

                if self.pipeline_live and self.proc is not None and self.proc.poll() is not None:
                    self.report_downstream_exit(self.proc.returncode)
                    self.stop_pipeline("downstream exited")
                    self.pipeline_live = False
                    self.flv_splicer = None
                    self._sync_status(False)
                    self.stop_event.wait(CRASH_DELAY)
                    continue

                if self.prewarming and CLUSTER_ACTIVE_EVENT.is_set():
                    self.promote_from_standby()

                self._sync_status(self.pipeline_live)
                self.stop_event.wait(RESTART_DELAY)

        finally:
            self.ws_should_connect.clear()
            with self.current_fd_lock:
                fd = self.current_fd
            if fd is not None:
                try:
                    fd.close()
                except Exception:
                    pass
            self.stop_pipeline(
                "handoff cutover" if HANDOFF_QUIESCE_EVENT.is_set() else "relay stopped",
                abrupt_handoff=HANDOFF_QUIESCE_EVENT.is_set(),
            )
            self._sync_status(False)
            log(f"[{self.user}] Channel relay stopped.")


# ---------------------------------------------------------------------------
# Cluster activation, control HTTP server (port 9001), and supervisor loop
# ---------------------------------------------------------------------------

def activate_cluster(notify_takeover: bool = True) -> dict:
    """Switch from STANDBY to ACTIVE and immediately promote pre-warmed streams."""
    was_standby = not CLUSTER_ACTIVE_EVENT.is_set()
    try:
        if os.path.exists(STANDBY_FLAG_PATH):
            os.remove(STANDBY_FLAG_PATH)
    except OSError:
        pass

    CLUSTER_ACTIVE_EVENT.set()
    promoted = 0
    with WORKERS_LOCK:
        for entry in WORKERS.values():
            relay: ChannelRelay = entry["relay"]
            if relay.promote_from_standby():
                promoted += 1

    if was_standby:
        log(f"[CLUSTER] Activated node {MY_NAME} (promoted {promoted} prewarmed stream(s)).")
        if notify_takeover and promoted > 0:
            notify_telegram(
                f"🔄 <b>تم تسليم القيادة واستلام البثوث على {MY_NAME} بنجاح</b>\n"
                f"• <b>البثوث المنقولة فوراً:</b> <code>{promoted}</code>"
            )
    return {"activated": True, "was_standby": was_standby, "promoted": promoted}


def trigger_handoff_quiesce() -> None:
    """Abort local publishers without FCUnpublish for sub-second peer cutover."""
    log("[HANDOFF] Quiesce signal received; cutting local publishers cleanly.")
    HANDOFF_QUIESCE_EVENT.set()
    SHUTDOWN_EVENT.set()
    with WORKERS_LOCK:
        entries = list(WORKERS.values())
    for entry in entries:
        relay: ChannelRelay = entry["relay"]
        relay.stop_event.set()
        relay.on_live_ended("handoff quiesce")
        relay.stop_pipeline("handoff quiesce", abrupt_handoff=True)


def is_cluster_ready() -> tuple[bool, dict]:
    if not DAEMON_READY_EVENT.is_set():
        return False, {"ok": False, "reason": "restreamer initializing"}
    if REQUIRE_BOT_READY and not os.path.exists(BOT_READY_PATH):
        return False, {"ok": False, "reason": "bot initializing"}

    mode = "active" if CLUSTER_ACTIVE_EVENT.is_set() else "standby"
    with WORKERS_LOCK:
        relays = [e["relay"] for e in WORKERS.values()]

    prewarmed = [r.user for r in relays if r.is_prewarmed()]
    live = [r.user for r in relays if r.pipeline_live]

    if mode == "standby" and relays:
        # Give live channels up to PREWARM_READY_WAIT_SEC to buffer an IDR GOP before handoff.
        elapsed = time.monotonic() - DAEMON_BOOT_TIME
        if elapsed < PREWARM_READY_WAIT_SEC and len(prewarmed) < len(relays):
            # If all relays are already confirmed offline or prewarmed, don't wait unnecessarily.
            any_initializing = any(
                r.prewarming and not r.is_prewarmed()
                for r in relays
            )
            if any_initializing or elapsed < 4.0:
                return False, {
                    "ok": False,
                    "mode": mode,
                    "reason": "prewarming GOP buffers",
                    "prewarmed": prewarmed,
                }

    return True, {
        "ok": True,
        "node": MY_NAME,
        "mode": mode,
        "prewarmed": prewarmed,
        "live": live,
        "rss_mb": round(get_rss_mb(), 1),
        "uptime_sec": round(time.monotonic() - DAEMON_BOOT_TIME, 1),
    }


class _ControlHandler(http.server.BaseHTTPRequestHandler):
    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/ready":
            ready, info = is_cluster_ready()
            self._send_json(200 if ready else 503, info)
            return
        if self.path == "/status":
            _, info = is_cluster_ready()
            with WORKERS_LOCK:
                info["channels"] = [
                    {
                        "id": r.sid,
                        "user": r.user,
                        "platform": r.platform,
                        "layout": r.layout,
                        "live": r.pipeline_live,
                        "prewarming": r.prewarming,
                        "warm": r.is_prewarmed(),
                        "sessions": r.session_number,
                        "dup_skipped": r.total_dup_skipped,
                    }
                    for r in (e["relay"] for e in WORKERS.values())
                ]
            self._send_json(200, info)
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(min(length, 2 * 1024 * 1024)) if length > 0 else b""
        if self.path == "/sync":
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                if isinstance(payload.get("streams"), list):
                    save_streams(payload["streams"], touch_reload=True)
                if isinstance(payload.get("global"), dict):
                    save_global_config(payload["global"])
                self._send_json(200, {"synced": True})
            except Exception as exc:
                self._send_json(400, {"error": str(exc)})
            return

        if self.path == "/activate":
            res = activate_cluster(notify_takeover=True)
            self._send_json(200, res)
            return

        if self.path == "/quiesce":
            self._send_json(200, {"quiescing": True})
            threading.Thread(target=trigger_handoff_quiesce, daemon=True).start()
            return

        self._send_json(404, {"error": "not found"})

    def log_message(self, format: str, *args) -> None:
        return


class _ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def start_control_server() -> None:
    try:
        server = _ThreadingHTTPServer(("0.0.0.0", CONTROL_PORT), _ControlHandler)
        threading.Thread(
            target=server.serve_forever,
            name="control-http",
            daemon=True,
        ).start()
        log(f"[CTRL] HTTP control server listening on port {CONTROL_PORT}.")
    except Exception as exc:
        log(f"[CTRL] Failed to start HTTP control server on {CONTROL_PORT}: {exc}")


def main() -> None:
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    if len(sys.argv) >= 3 and sys.argv[1] in ("--filler", "--test-files"):
        from flv_engine import main_cli

        main_cli()
        return

    ensure_data_dir()
    if os.path.exists(STANDBY_FLAG_PATH):
        CLUSTER_ACTIVE_EVENT.clear()
        log(f"[{MY_NAME}] Starting in STANDBY/PREWARM mode (waiting for /activate cutover).")
    else:
        CLUSTER_ACTIVE_EVENT.set()
        log(f"[{MY_NAME}] Starting in ACTIVE mode.")

    slate_thread = None
    if COPY_VIDEO and FILLER_KBPS > 0 and SLATE_SEC > 0:
        slate_thread = threading.Thread(
            target=ensure_slate, name="slate-builder", daemon=True
        )
        slate_thread.start()

    start_control_server()

    def stop_channel(sid: str, join_timeout: float = 25.0) -> None:
        with WORKERS_LOCK:
            entry = WORKERS.pop(sid, None)
        if not entry:
            return
        relay: ChannelRelay = entry["relay"]
        th: threading.Thread = entry["thread"]
        try:
            relay.stop_event.set()
            relay.on_live_ended("stopped by supervisor")
            th.join(timeout=join_timeout)
            if th.is_alive():
                log(f"[MAIN] Channel {sid} did not stop in {join_timeout}s; killing pipeline.")
                relay.stop_pipeline("forced stop")
                th.join(timeout=5.0)
        except Exception as exc:
            log(f"[MAIN] stop_channel({sid}) error: {exc!r}")

    def handle_signal(_signum, _frame) -> None:
        SHUTDOWN_EVENT.set()
        with WORKERS_LOCK:
            sids = list(WORKERS.keys())
        for sid in sids:
            stop_channel(sid, join_timeout=12.0)
        sys.exit(0)

    def handle_handoff_signal(_signum, _frame) -> None:
        trigger_handoff_quiesce()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, handle_handoff_signal)

    standby_started_at = time.monotonic()

    while not SHUTDOWN_EVENT.is_set():
        try:
            # Failsafe: never stay stranded in standby if peer failed to send /activate.
            if not CLUSTER_ACTIVE_EVENT.is_set():
                if not os.path.exists(STANDBY_FLAG_PATH):
                    activate_cluster(notify_takeover=True)
                elif time.monotonic() - standby_started_at >= STANDBY_MAX_SEC:
                    log(f"[CLUSTER] Standby timeout ({STANDBY_MAX_SEC:.0f}s) reached; auto-activating.")
                    activate_cluster(notify_takeover=True)

            if os.path.exists("/tmp/reload_restreamer"):
                try:
                    os.remove("/tmp/reload_restreamer")
                except OSError:
                    pass

            if os.path.exists("/tmp/restart_all_streams"):
                try:
                    os.remove("/tmp/restart_all_streams")
                except OSError:
                    pass
                with WORKERS_LOCK:
                    sids = list(WORKERS.keys())
                for sid in sids:
                    stop_channel(sid)

            configured = load_streams()
            prune_statuses({s["id"] for s in configured})
            active_map = {s["id"]: s for s in configured if s.get("enabled", True)}

            with WORKERS_LOCK:
                existing_sids = list(WORKERS.keys())

            for sid in existing_sids:
                if sid not in active_map:
                    stop_channel(sid)

            for sid, sdata in active_map.items():
                with WORKERS_LOCK:
                    entry = WORKERS.get(sid)
                if entry is not None:
                    old = entry["relay"].data
                    if (
                        old.get("target_url") != sdata.get("target_url")
                        or old.get("layout") != sdata.get("layout")
                        or old.get("platform") != sdata.get("platform")
                    ):
                        log(f"[MAIN] Updating stream @{sdata.get('tiktok_user')} without interrupting other channels.")
                        stop_channel(sid)

            for sid, sdata in active_map.items():
                with WORKERS_LOCK:
                    entry = WORKERS.get(sid)
                    need_start = entry is None or not entry["thread"].is_alive()
                if need_start:
                    try:
                        relay = ChannelRelay(sdata, slate_thread=slate_thread)
                        th = threading.Thread(
                            target=relay.run,
                            daemon=True,
                            name=f"relay-{sid}",
                        )
                        with WORKERS_LOCK:
                            WORKERS[sid] = {"relay": relay, "thread": th}
                        th.start()
                    except Exception as exc:
                        log(f"[MAIN] Could not start channel {sid}: {exc!r}")

            if not DAEMON_READY_EVENT.is_set():
                try:
                    with open("/tmp/daemon_ready", "w", encoding="utf-8") as handle:
                        handle.write("ok")
                except OSError:
                    pass
                DAEMON_READY_EVENT.set()

        except Exception as exc:
            log(f"[MAIN] Supervisor loop error: {exc!r}")

        SHUTDOWN_EVENT.wait(2.0)


if __name__ == "__main__":
    main()
