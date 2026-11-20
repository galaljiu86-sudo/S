#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Channel TikTok LIVE -> YouTube / Meta RTMP Restreamer
with Session-Aware FLV Splicing & Persistent Downstream Pipelines.
"""

from __future__ import annotations

import asyncio
import gc
import json
import os
import queue
import re
import resource
import shlex
import signal
import struct
import subprocess
import sys
import threading
import time
from itertools import chain
from typing import BinaryIO, Optional

import requests
import streamlink

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import ConnectEvent, LiveEndEvent
    TIKTOKLIVE_IMPORT_ERROR = None
except Exception as exc:
    TikTokLiveClient = None
    TIKTOKLIVE_IMPORT_ERROR = exc


def env_value(name: str, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except (TypeError, ValueError):
        return default


DATA_DIR = os.path.abspath("data")
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
STATUS_PATH = os.path.join(DATA_DIR, "status.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

QUALITY = "h264_hd"
FILLER_KBPS = env_value("FILLER_KBPS", 3850, int)

# YouTube Copy Buffer
YT_BUF_MB = env_value("BUF_MB", 8, int)
YT_PREFILL_PCT = env_value("PREFILL_PCT", 2, int)

# Meta High Buffer (Facebook / Instagram)
META_BUF_MB = env_value("META_BUF_MB", 32, int)
META_PREFILL_PCT = env_value("META_PREFILL_PCT", 2, int)

# Meta / Re-encode Video Settings
META_BITRATE = "2500k"
FPS = 30
X264_PRESET = env_value("X264_PRESET", "superfast")
GOP = FPS * 2  # 60 (2 seconds)

CHECK_INTERVAL = env_value("CHECK_INTERVAL", 3.0, float)
LIVE_END_COOLDOWN = env_value("LIVE_END_COOLDOWN", 15, int)
RESTART_DELAY = env_value("RESTART_DELAY", 1, int)
CRASH_DELAY = env_value("CRASH_DELAY", 5, int)

RSS_LIMIT_MB = env_value("RSS_LIMIT_MB", 350, int)
SESSION_TTL_SEC = env_value("SESSION_TTL_SEC", 600, int)
MEMORY_CHECK_INTERVAL = env_value("MEMORY_CHECK_INTERVAL", 30, int)
MEMORY_CLEANUP_COOLDOWN = env_value("MEMORY_CLEANUP_COOLDOWN", 120, int)

FILLER_QUEUE_MAX_TAGS = env_value("FILLER_QUEUE_MAX_TAGS", 256, int)
MIN_RESERVE_SEC = 3.0

STREAM_STALL_SECONDS = env_value("STREAM_STALL_SECONDS", 15.0, float)
SOURCE_QUEUE_CHUNKS = env_value("SOURCE_QUEUE_CHUNKS", 16, int)
OFFLINE_CONFIRMATIONS = env_value("OFFLINE_CONFIRMATIONS", 4, int)
OFFLINE_CONFIRM_INTERVAL = env_value("OFFLINE_CONFIRM_INTERVAL", 2.5, float)
WS_OFFLINE_FAILSAFE_SECONDS = env_value("WS_OFFLINE_FAILSAFE_SECONDS", 120.0, float)

TIKTOK_API_LOCK = threading.Lock()
LAST_TIKTOK_API_CALL = 0.0
MIN_TIKTOK_API_GAP = 2.0

STATUS_LOCK = threading.Lock()
LAST_VALID_STREAMS = []
SHUTDOWN_EVENT = threading.Event()


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
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def notify_telegram(message: str) -> None:
    if not BOT_TOKEN or not os.path.exists(CONFIG_PATH):
        return
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        chat_id = cfg.get("chat_id")
        if chat_id:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=5)
    except Exception as exc:
        log(f"[NOTIFY_ERROR] {exc}")


def update_stream_live_status(stream_id: str, tiktok_user: str, is_live: bool) -> None:
    with STATUS_LOCK:
        statuses = {}
        if os.path.exists(STATUS_PATH):
            try:
                with open(STATUS_PATH, "r", encoding="utf-8") as f:
                    statuses = json.load(f)
            except Exception:
                statuses = {}

        cur = statuses.get(stream_id, {})
        if cur.get("is_live") == is_live and cur.get("tiktok_user") == tiktok_user:
            return

        statuses[stream_id] = {"is_live": is_live, "tiktok_user": tiktok_user, "updated_at": time.time()}
        tmp_path = STATUS_PATH + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(statuses, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, STATUS_PATH)
        except Exception as exc:
            log(f"[STATUS_SAVE_ERR] {exc}")


def load_streams() -> list:
    global LAST_VALID_STREAMS
    if os.path.exists(STREAMS_PATH):
        try:
            with open(STREAMS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for idx, s in enumerate(data):
                        if not s.get("id"):
                            s["id"] = f"{s.get('tiktok_user')}_{idx}"
                    LAST_VALID_STREAMS = data
                    return data
        except Exception:
            pass
    return list(LAST_VALID_STREAMS)


# ---------------------------------------------------------------------------
# FLV parsing, splicing, and filler
# ---------------------------------------------------------------------------

def iter_flv_tags(handle: BinaryIO):
    header = handle.read(13)
    if len(header) < 13 or header[:3] != b"FLV":
        return

    while True:
        tag_header = handle.read(11)
        if len(tag_header) < 11:
            return

        data_size = int.from_bytes(tag_header[1:4], "big")
        timestamp = (
            int.from_bytes(tag_header[4:7], "big")
            | (tag_header[7] << 24)
        )
        payload = handle.read(data_size)
        previous_tag_size = handle.read(4)
        if len(payload) != data_size or len(previous_tag_size) != 4:
            return

        yield tag_header[0], timestamp, payload


class StreamStalled(Exception):
    pass


class QueueStreamReader:
    def __init__(self, chunks: queue.Queue, stop_event: threading.Event,
                 live_ended_event: threading.Event, stall_seconds: float):
        self.chunks = chunks
        self.stop_event = stop_event
        self.live_ended_event = live_ended_event
        self.stall_seconds = stall_seconds
        self.buffer = bytearray()
        self.eof = False
        self.last_data = time.monotonic()

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size < 0:
            raise ValueError("QueueStreamReader only supports bounded reads")

        while len(self.buffer) < size and not self.eof:
            if self.stop_event.is_set() or self.live_ended_event.is_set():
                raise InterruptedError("Relay is stopping")

            try:
                kind, payload = self.chunks.get(timeout=1.0)
            except queue.Empty:
                if time.monotonic() - self.last_data >= self.stall_seconds:
                    raise StreamStalled(
                        f"No source bytes for {self.stall_seconds:.1f}s"
                    )
                continue

            if kind == "data":
                if payload:
                    self.buffer.extend(payload)
                    self.last_data = time.monotonic()
            elif kind == "eof":
                self.eof = True
            elif kind == "error":
                if self.stop_event.is_set() or self.live_ended_event.is_set():
                    raise InterruptedError("Relay is stopping")
                raise OSError(f"Streamlink read failed: {payload}")
            else:
                raise RuntimeError(f"Unknown source queue item: {kind}")

        if len(self.buffer) >= size:
            result = bytes(self.buffer[:size])
            del self.buffer[:size]
            return result

        result = bytes(self.buffer)
        self.buffer.clear()
        return result


def read_exact(handle, size: int, what: str) -> bytes:
    data = handle.read(size)
    if len(data) != size:
        raise EOFError(f"Truncated {what}: wanted {size}, received {len(data)}")
    return data


def read_flv_tag(handle):
    header = handle.read(11)
    if not header:
        return None
    if len(header) != 11:
        raise EOFError("Truncated FLV tag header")

    tag_type = header[0]
    data_size = int.from_bytes(header[1:4], "big")
    timestamp = int.from_bytes(header[4:7], "big") | (header[7] << 24)
    stream_id = header[8:11]

    if data_size > 0xFFFFFF:
        raise ValueError(f"Invalid FLV tag size: {data_size}")

    payload = read_exact(handle, data_size, "FLV tag payload")
    previous = read_exact(handle, 4, "PreviousTagSize")
    actual_previous = struct.unpack(">I", previous)[0]
    expected_previous = 11 + data_size
    if actual_previous != expected_previous:
        raise ValueError(
            f"PreviousTagSize mismatch: expected {expected_previous}, got {actual_previous}"
        )

    return tag_type, timestamp, stream_id, payload


def encode_flv_tag(tag_type: int, timestamp: int, stream_id: bytes, payload: bytes) -> bytes:
    size = len(payload)
    if size > 0xFFFFFF:
        raise ValueError(f"FLV tag exceeds 24-bit size limit: {size}")

    timestamp &= 0xFFFFFFFF
    header = (
        bytes([tag_type])
        + size.to_bytes(3, "big")
        + (timestamp & 0xFFFFFF).to_bytes(3, "big")
        + bytes([(timestamp >> 24) & 0xFF])
        + stream_id
    )
    return header + payload + struct.pack(">I", 11 + size)


def h264_packet_has_idr(payload: bytes, nal_length_size: int) -> bool:
    if len(payload) < 5 or payload[1] != 1:
        return False
    if not 1 <= nal_length_size <= 4:
        raise ValueError(f"Invalid AVC NAL length size: {nal_length_size}")

    pos = 5
    found = False
    while pos < len(payload):
        if pos + nal_length_size > len(payload):
            raise ValueError("Incomplete AVC NAL length field")

        nalu_size = int.from_bytes(payload[pos:pos + nal_length_size], "big")
        pos += nal_length_size

        if nalu_size <= 0 or pos + nalu_size > len(payload):
            raise ValueError("Invalid AVC NAL unit length")

        if (payload[pos] & 0x1F) == 5:
            found = True
        pos += nalu_size

    return found


class FlvSessionSplicer:
    def __init__(self, write_bytes):
        self.write_bytes = write_bytes
        self.header_written = False
        self.last_output_timestamp = -1
        self.session_count = 0

    def process_handle(self, handle, source_name: str = "source") -> dict:
        flv_header = read_exact(handle, 9, "FLV header")
        if flv_header[:3] != b"FLV":
            raise ValueError(f"{source_name}: source is not an FLV stream")

        data_offset = struct.unpack(">I", flv_header[5:9])[0]
        if data_offset < 9 or data_offset > 1024:
            raise ValueError(f"{source_name}: invalid FLV data offset {data_offset}")

        extra_header = read_exact(handle, data_offset - 9, "FLV extended header")
        previous0 = read_exact(handle, 4, "PreviousTagSize0")
        if struct.unpack(">I", previous0)[0] != 0:
            raise ValueError(f"{source_name}: invalid PreviousTagSize0")

        first_media = None
        while True:
            tag = read_flv_tag(handle)
            if tag is None:
                return {"empty": True}
            if tag[0] in (8, 9):
                first_media = tag
                break

        if not self.header_written:
            self.write_bytes(flv_header + extra_header + previous0)
            self.header_written = True

        if self.last_output_timestamp < 0:
            timestamp_offset = 0
        else:
            timestamp_offset = max(
                0, self.last_output_timestamp + 1 - first_media[1]
            )

        nal_length_size = None
        video_started = False
        dropped_video = 0
        video_written = 0
        audio_written = 0
        avc_headers = 0
        aac_headers = 0
        first_idr_timestamp = None
        last_session_ts = self.last_output_timestamp

        for tag in chain((first_media,), iter(lambda: read_flv_tag(handle), None)):
            tag_type, timestamp, stream_id, payload = tag

            if tag_type == 18:
                continue

            if tag_type == 8:
                if len(payload) >= 2 and (payload[0] >> 4) == 10 and payload[1] == 0:
                    aac_headers += 1
                audio_written += 1

            elif tag_type == 9:
                if len(payload) < 2 or (payload[0] & 0x0F) != 7:
                    raise ValueError(f"{source_name}: expected legacy H.264/AVC FLV tag")

                packet_type = payload[1]

                if packet_type == 0:
                    if len(payload) < 10:
                        raise ValueError(f"{source_name}: invalid AVC sequence header")
                    nal_length_size = (payload[9] & 0x03) + 1
                    avc_headers += 1

                elif packet_type == 1:
                    if nal_length_size is None:
                        raise ValueError(f"{source_name}: AVC media arrived before sequence header")
                    if not video_started:
                        if not h264_packet_has_idr(payload, nal_length_size):
                            dropped_video += 1
                            continue
                        video_started = True
                        first_idr_timestamp = timestamp + timestamp_offset

                elif packet_type == 2:
                    if not video_started:
                        continue
                else:
                    raise ValueError(f"{source_name}: unsupported AVC packet type {packet_type}")

                video_written += 1

            else:
                continue

            output_timestamp = timestamp + timestamp_offset
            encoded = encode_flv_tag(tag_type, output_timestamp, stream_id, payload)
            self.write_bytes(encoded)
            self.last_output_timestamp = max(self.last_output_timestamp, output_timestamp)
            last_session_ts = max(last_session_ts, output_timestamp)

        self.session_count += 1
        return {
            "empty": False,
            "session": self.session_count,
            "source": source_name,
            "offset": timestamp_offset,
            "nal_length_size": nal_length_size,
            "avc_headers": avc_headers,
            "aac_headers": aac_headers,
            "dropped_video": dropped_video,
            "video_written": video_written,
            "audio_written": audio_written,
            "first_idr": first_idr_timestamp,
            "last_timestamp": last_session_ts,
        }


def run_filler(kbps: int) -> None:
    if kbps <= 0:
        raise ValueError("FILLER_KBPS must be greater than zero")

    output = sys.stdout.buffer
    bytes_per_ms = kbps * 1000 / 8 / 1000.0
    tags: queue.Queue = queue.Queue(maxsize=max(32, FILLER_QUEUE_MAX_TAGS))

    def reader() -> None:
        try:
            for tag in iter_flv_tags(sys.stdin.buffer):
                tags.put(tag)
        except (BrokenPipeError, OSError):
            pass
        except Exception as exc:
            log(f"[FILLER] Reader error: {exc}")
        finally:
            tags.put(None)

    threading.Thread(target=reader, name="flv-reader", daemon=True).start()

    state = {
        "nal_len": 4,
        "last_video_ts": None,
        "audio_bytes": 0,
        "carry_bytes": 0.0,
        "last_output_ts": 0,
    }

    output.write(b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00")

    def make_filler_nal(kind: str, filler_bytes: int) -> bytes:
        filler_bytes = max(0, filler_bytes)
        if kind == "h264":
            return (
                struct.pack(">I", filler_bytes + 2)
                + b"\x0c"
                + (b"\xff" * filler_bytes)
                + b"\x80"
            )
        if kind == "hevc":
            return (
                struct.pack(">I", filler_bytes + 3)
                + b"\x4c\x01"
                + (b"\xff" * filler_bytes)
                + b"\x80"
            )
        return b""

    def classify_video(payload: bytes):
        if not payload:
            return None, False, False, False
        first = payload[0]
        if first & 0x80:
            packet_type = first & 0x0F
            fourcc = payload[1:5]
            codec = "hevc" if fourcc == b"hvc1" else "h264" if fourcc == b"avc1" else None
            is_keyframe = ((first >> 4) & 0x07) == 1
            return codec, packet_type == 0, packet_type in (1, 3), is_keyframe
        codec_id = first & 0x0F
        if codec_id in (7, 12) and len(payload) >= 2:
            codec = "h264" if codec_id == 7 else "hevc"
            packet_type = payload[1]
            is_keyframe = (first >> 4) == 1
            return codec, packet_type == 0, packet_type == 1, is_keyframe
        return None, False, False, False

    def emit(tag_type: int, timestamp: int, payload: bytes) -> None:
        data = payload
        data_size = len(data)

        if tag_type == 8:
            state["audio_bytes"] += data_size + 15
        elif tag_type == 9 and data_size > 5:
            codec, is_sequence, is_frame, _is_keyframe = classify_video(data)

            if codec and is_sequence:
                if codec == "hevc" and data_size > 26:
                    state["nal_len"] = (data[26] & 0x03) + 1
                elif codec == "h264" and data_size > 9:
                    state["nal_len"] = (data[9] & 0x03) + 1

            elif codec and is_frame and state["nal_len"] == 4:
                last_ts = state["last_video_ts"]
                if last_ts is not None and timestamp >= last_ts:
                    state["carry_bytes"] += (timestamp - last_ts) * bytes_per_ms
                state["last_video_ts"] = timestamp

                needed = int(state["carry_bytes"]) - (data_size + 15) - state["audio_bytes"]
                state["audio_bytes"] = 0

                if needed > 8:
                    extra = make_filler_nal(codec, min(needed - 7, 200000))
                    if extra:
                        data += extra
                        data_size = len(data)

                state["carry_bytes"] -= data_size + 15
                if state["carry_bytes"] < 0:
                    state["carry_bytes"] = 0.0

        timestamp &= 0xFFFFFFFF
        if data_size > 0xFFFFFF:
            raise ValueError(f"FLV tag too large: {data_size} bytes")

        output.write(
            bytes([tag_type])
            + data_size.to_bytes(3, "big")
            + (timestamp & 0xFFFFFF).to_bytes(3, "big")
            + bytes([timestamp >> 24])
            + b"\x00\x00\x00"
        )
        output.write(data)
        output.write(struct.pack(">I", 11 + data_size))

        if tag_type == 9:
            output.flush()
        state["last_output_ts"] = max(state["last_output_ts"], timestamp)

    timestamp_offset = None
    first_source_ts = None
    wall_clock_start = None
    seen_keyframe = False

    while True:
        tag = tags.get()
        if tag is None:
            break

        tag_type, timestamp, payload = tag
        if tag_type not in (8, 9) or len(payload) < 2:
            continue

        if tag_type == 9:
            _codec, _is_sequence, is_frame, is_keyframe = classify_video(payload)
            if is_frame and not seen_keyframe:
                if not is_keyframe:
                    continue
                seen_keyframe = True

        if timestamp_offset is None:
            prior_ts = state["last_output_ts"]
            timestamp_offset = prior_ts + (40 if prior_ts else 0) - timestamp
            first_source_ts = timestamp
            wall_clock_start = time.monotonic()

        target_elapsed = (timestamp - first_source_ts) / 1000.0
        actual_elapsed = time.monotonic() - wall_clock_start
        wait_seconds = target_elapsed - actual_elapsed
        if wait_seconds > 0 and state["last_output_ts"]:
            time.sleep(min(wait_seconds, 0.5))

        emit(tag_type, max(0, timestamp + timestamp_offset), payload)

    output.flush()


# ---------------------------------------------------------------------------
# بناء خط الأنابيب المستمر لكل منصة ونمط
# ---------------------------------------------------------------------------

def build_persistent_pipeline(target: str, platform: str, layout: str) -> str:
    is_meta = platform in ("Facebook", "Instagram")
    needs_reencode = is_meta or (layout in ("landscape_to_portrait_pad", "portrait_stable"))

    if not needs_reencode:
        # كود يوتيوب الطولي الأصلي بالضبط بدون أي تغيير
        buffer_cmd = ["mbuffer", "-q", "-m", f"{YT_BUF_MB}M", "-P", str(YT_PREFILL_PCT)]
        ffmpeg_1 = [
            "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
            "-fflags", "+genpts+discardcorrupt",
            "-i", "pipe:0",
            "-map", "0:v:0", "-map", "0:a:0?",
            "-c:v", "copy",
            "-c:a", "copy",
            "-max_muxing_queue_size", "2048",
            "-flvflags", "no_duration_filesize",
            "-f", "flv", "pipe:1",
        ]
        if FILLER_KBPS > 0 and layout != "landscape_raw":
            filler_cmd = [sys.executable, os.path.abspath(__file__), "--filler", str(FILLER_KBPS)]
            ffmpeg_2 = [
                "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
                "-f", "flv", "-i", "pipe:0",
                "-c", "copy",
                "-flvflags", "no_duration_filesize",
                "-f", "flv", target,
            ]
            cmds = [buffer_cmd, ffmpeg_1, filler_cmd, ffmpeg_2]
        else:
            ffmpeg_1[-1] = target
            cmds = [buffer_cmd, ffmpeg_1]
        return "set -o pipefail; " + " | ".join(shlex.join(c) for c in cmds)

    # إعادة التشفير (لمنصات Meta أو النمط الثابت أو العرضي 180%)
    buf_mb = META_BUF_MB if is_meta else YT_BUF_MB
    prefill = META_PREFILL_PCT if is_meta else YT_PREFILL_PCT
    buffer_cmd = ["mbuffer", "-q", "-m", f"{buf_mb}M", "-P", str(prefill)]

    if layout == "landscape_to_portrait_pad":
        # تكبير ثابت 180% (100% + 80%) مع قص آمن وتوسيط على خلفية سوداء 720x1280
        scale_w = 1296  # int(720 * 1.8)
        vf_filter = (
            f"fps={FPS},"
            f"scale=w={scale_w}:h=-2:flags=bicubic,"
            f"crop=w=min(iw\\,720):h=min(ih\\,1280):x=(iw-ow)/2:y=(ih-oh)/2,"
            f"pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
            f"format=yuv420p"
        )
    elif layout == "landscape_raw":
        vf_filter = (
            f"fps={FPS},"
            "scale=w=1280:h=720:force_original_aspect_ratio=decrease,"
            "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,"
            "format=yuv420p"
        )
    else:
        vf_filter = (
            f"fps={FPS},"
            "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
            "pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
            "format=yuv420p"
        )

    ffmpeg_reencode = [
        "nice", "-n", "10",
        "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
        "-analyzeduration", "10M", "-probesize", "10M",
        "-fflags", "+genpts+discardcorrupt",
        "-i", "pipe:0",
        "-map", "0:v:0", "-map", "0:a:0?",
        "-vf", vf_filter,
        "-c:v", "libx264", "-preset", X264_PRESET, "-threads", "2",
        "-profile:v", "high", "-level", "4.1",
        "-b:v", META_BITRATE, "-minrate", META_BITRATE, "-maxrate", META_BITRATE, "-bufsize", "5000k",
        "-x264-params", "nal-hrd=cbr:force-cfr=1:scenecut=0",
        "-g", str(GOP), "-keyint_min", str(GOP), "-sc_threshold", "0",
        "-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2",
        "-af", "aresample=async=1000:min_hard_comp=0.100000:first_pts=0",
        "-max_muxing_queue_size", "4096",
        "-flvflags", "no_duration_filesize",
        "-f", "flv", target,
    ]
    return "set -o pipefail; " + " | ".join(shlex.join(c) for c in [buffer_cmd, ffmpeg_reencode])


# ---------------------------------------------------------------------------
# Streamlink session and per-channel monitor
# ---------------------------------------------------------------------------

def make_session():
    session = streamlink.Streamlink()
    try:
        session.set_option("stream-timeout", 60)
        session.set_option("http-timeout", 30)
    except Exception as exc:
        log(f"[WARN] Could not set Streamlink options: {exc}")
    return session


class LiveEndMonitor(threading.Thread):
    def __init__(self, owner: "ChannelRelay"):
        super().__init__(daemon=True, name=f"ws-{owner.user}")
        self.owner = owner

    def run(self) -> None:
        asyncio.run(self._main())

    async def _main(self) -> None:
        while not self.owner.stop_event.is_set() and not SHUTDOWN_EVENT.is_set():
            try:
                client = TikTokLiveClient(unique_id=f"@{self.owner.user}")

                @client.on(ConnectEvent)
                async def on_connect(event):
                    self.owner.ws_connected.set()
                    log(f"[{self.owner.user}] [WS] Connected to TikTok LIVE monitor.")

                @client.on(LiveEndEvent)
                async def on_live_end(event):
                    log(f"[{self.owner.user}] [WS] LiveEndEvent; verifying...")
                    offline_confirmations = 0
                    for attempt in range(1, 6):
                        if self.owner.stop_event.is_set():
                            return
                        await asyncio.sleep(3)
                        streams = await asyncio.to_thread(self.owner.fetch_source_streams)
                        if streams is None:
                            continue
                        if streams:
                            return
                        offline_confirmations += 1
                        if offline_confirmations >= 3:
                            self.owner.on_live_ended()
                            return

                if await client.is_live():
                    await client.connect()
                self.owner.ws_connected.clear()
            except Exception:
                self.owner.ws_connected.clear()
            self.owner.stop_event.wait(5)


class ChannelRelay:
    def __init__(self, stream_data: dict):
        self.data = dict(stream_data)
        self.sid = stream_data["id"]
        self.user = stream_data["tiktok_user"]
        self.tiktok_url = f"https://www.tiktok.com/@{self.user}/live"
        self.target = stream_data["target_url"]
        self.platform = stream_data.get("platform", "YouTube")
        self.layout = stream_data.get("layout", "portrait_copy")

        self.stop_event = threading.Event()
        self.live_ended_event = threading.Event()
        self.session_lock = threading.Lock()
        self.session = make_session()
        self.session_created_at = time.monotonic()
        self.proc: Optional[subprocess.Popen] = None
        self.current_fd = None
        self.current_fd_lock = threading.Lock()
        self.ws_connected = threading.Event()
        self.flv_splicer: Optional[FlvSessionSplicer] = None
        self.last_memory_check = 0.0
        self.last_memory_cleanup = 0.0
        self.last_wait_log = 0.0
        self.last_status_error_log = 0.0

    def _rebuild_session_unlocked(self) -> None:
        old_session = self.session
        try:
            old_session.http.close()
        except Exception:
            pass
        self.session = make_session()
        self.session_created_at = time.monotonic()

    def fetch_source_streams(self):
        global LAST_TIKTOK_API_CALL
        with self.session_lock:
            now = time.monotonic()
            if now - self.session_created_at >= SESSION_TTL_SEC:
                self._rebuild_session_unlocked()
            with TIKTOK_API_LOCK:
                wait = MIN_TIKTOK_API_GAP - (time.monotonic() - LAST_TIKTOK_API_CALL)
                if wait > 0:
                    time.sleep(wait)
                LAST_TIKTOK_API_CALL = time.monotonic()
            try:
                return self.session.streams(self.tiktok_url)
            except Exception as exc:
                wall_now = time.time()
                if wall_now - self.last_status_error_log >= 60:
                    log(f"[{self.user}] [CHECK] Status request failed: {exc}")
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
            if get_rss_mb() > RSS_LIMIT_MB and now - self.last_memory_cleanup >= MEMORY_CLEANUP_COOLDOWN:
                self.last_memory_cleanup = now
                self._rebuild_session_unlocked()
                gc.collect()

    def start_pipeline(self) -> None:
        if self.proc is not None:
            return
        log(f"[{self.user} | {self.platform}] Starting persistent FFmpeg/RTMP pipeline ({self.layout})...")
        err_file = f"/tmp/pipeline_err_{self.sid}.log"
        ef = open(err_file, "w")
        try:
            self.proc = subprocess.Popen(
                build_persistent_pipeline(self.target, self.platform, self.layout),
                shell=True,
                executable="/bin/bash",
                stdin=subprocess.PIPE,
                stderr=ef,
                preexec_fn=os.setsid,
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

    def stop_pipeline(self, reason: str = "shutdown") -> None:
        process, self.proc = self.proc, None
        if process is None:
            return
        log(f"[{self.user} | {self.platform}] Closing persistent pipeline ({reason})...")
        try:
            if process.stdin:
                process.stdin.close()
        except (BrokenPipeError, OSError, ValueError):
            pass
        try:
            process.wait(timeout=15)
            log(f"[{self.user}] [PROC] Pipeline exited with code {process.returncode}.")
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=4)
        except Exception:
            try:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
            except Exception:
                pass

    def on_live_ended(self) -> None:
        self.live_ended_event.set()
        with self.current_fd_lock:
            fd = self.current_fd
        if fd is not None:
            try:
                fd.close()
            except Exception:
                pass

    def stream_session(self, stream, session_number: int) -> None:
        try:
            fd = stream.open()
        except Exception as exc:
            log(f"[{self.user}] [SOURCE] Could not open Streamlink session: {exc}")
            return

        with self.current_fd_lock:
            self.current_fd = fd

        chunks: queue.Queue = queue.Queue(maxsize=max(4, SOURCE_QUEUE_CHUNKS))

        def enqueue(item) -> bool:
            while not self.stop_event.is_set() and not self.live_ended_event.is_set() and not SHUTDOWN_EVENT.is_set():
                try:
                    chunks.put(item, timeout=0.5)
                    return True
                except queue.Full:
                    continue
            return False

        def reader_thread() -> None:
            try:
                while not self.stop_event.is_set() and not self.live_ended_event.is_set() and not SHUTDOWN_EVENT.is_set():
                    block = fd.read(64 * 1024)
                    if not block:
                        enqueue(("eof", None))
                        return
                    if not enqueue(("data", block)):
                        return
            except Exception as exc:
                if not self.stop_event.is_set() and not self.live_ended_event.is_set():
                    enqueue(("error", exc))

        threading.Thread(target=reader_thread, name=f"sl-reader-{self.sid}-{session_number}", daemon=True).start()

        reader = QueueStreamReader(chunks, self.stop_event, self.live_ended_event, STREAM_STALL_SECONDS)

        try:
            if self.flv_splicer is None:
                raise RuntimeError("FLV splicer not initialized")
            result = self.flv_splicer.process_handle(reader, source_name=f"{self.user} session {session_number}")
            if result.get("empty"):
                return
            log(
                f"[{self.user}] [SPLICER] S{session_number}: offset={result['offset']}ms, "
                f"video={result['video_written']}, audio={result['audio_written']}"
            )
        except StreamStalled as exc:
            log(f"[{self.user}] [SOURCE] {exc}; reconnecting session.")
        except (EOFError, OSError, ValueError, RuntimeError, InterruptedError) as exc:
            if not self.stop_event.is_set() and not self.live_ended_event.is_set():
                log(f"[{self.user}] [SOURCE] Session {session_number} ended: {exc}")
        except BrokenPipeError as exc:
            log(f"[{self.user}] [PROC] Downstream closed pipe: {exc}")
        finally:
            with self.current_fd_lock:
                if self.current_fd is fd:
                    self.current_fd = None
            try:
                fd.close()
            except Exception:
                pass

    def run(self) -> None:
        log(f"[{self.user} | {self.platform}] بدء مراقبة القناة (النمط: {self.layout})")
        if TikTokLiveClient and self.user:
            LiveEndMonitor(self).start()

        pipeline_live = False
        session_number = 0
        offline_count = 0
        offline_since = None

        try:
            while not self.stop_event.is_set() and not SHUTDOWN_EVENT.is_set():
                self.maintain_memory()

                if self.live_ended_event.is_set():
                    self.stop_pipeline("TikTok LIVE ended")
                    pipeline_live = False
                    update_stream_live_status(self.sid, self.user, False)
                    notify_telegram(f"⏹️ <b>انتهى البث المباشر لحساب @{self.user}.</b>\nتم إيقاف النقل إلى {self.platform}.")
                    self.flv_splicer = None
                    session_number = 0
                    offline_count = 0
                    offline_since = None
                    self.live_ended_event.clear()
                    self.stop_event.wait(LIVE_END_COOLDOWN)
                    continue

                streams = self.fetch_source_streams()

                if streams is None:
                    if pipeline_live and offline_since is not None:
                        if self.ws_connected.is_set() and time.monotonic() - offline_since >= WS_OFFLINE_FAILSAFE_SECONDS:
                            self.on_live_ended()
                            continue
                    self.stop_event.wait(RESTART_DELAY if pipeline_live else CHECK_INTERVAL)
                    continue

                if not streams:
                    if not pipeline_live:
                        update_stream_live_status(self.sid, self.user, False)
                        self.stop_event.wait(CHECK_INTERVAL)
                        continue
                    if offline_since is None:
                        offline_since = time.monotonic()
                    offline_count += 1
                    if not self.ws_connected.is_set() and offline_count >= OFFLINE_CONFIRMATIONS:
                        self.on_live_ended()
                        continue
                    if self.ws_connected.is_set() and time.monotonic() - offline_since >= WS_OFFLINE_FAILSAFE_SECONDS:
                        self.on_live_ended()
                        continue
                    self.stop_event.wait(OFFLINE_CONFIRM_INTERVAL)
                    continue

                offline_count = 0
                offline_since = None

                if QUALITY not in streams:
                    self.stop_event.wait(RESTART_DELAY)
                    continue

                if not pipeline_live:
                    self.live_ended_event.clear()
                    self.flv_splicer = FlvSessionSplicer(self.write_pipeline_bytes)
                    try:
                        self.start_pipeline()
                    except Exception as exc:
                        log(f"[{self.user}] [PROC] Failed to start pipeline: {exc}")
                        self.stop_pipeline("startup failure")
                        self.stop_event.wait(CRASH_DELAY)
                        continue
                    pipeline_live = True
                    session_number = 0
                    update_stream_live_status(self.sid, self.user, True)
                    notify_telegram(f"🔴 <b>بدأ البث المباشر لحساب @{self.user}!</b>\n• <b>المنصة:</b> {self.platform}\n🚀 جاري الإرسال المستمر!")

                if self.proc is not None and self.proc.poll() is not None:
                    log(f"[{self.user}] [PROC] Downstream exited ({self.proc.returncode}); restarting.")
                    self.stop_pipeline("downstream exited")
                    pipeline_live = False
                    self.flv_splicer = None
                    self.stop_event.wait(CRASH_DELAY)
                    continue

                session_number += 1
                self.stream_session(streams[QUALITY], session_number)

                if self.stop_event.is_set() or self.live_ended_event.is_set():
                    continue

                if self.proc is not None and self.proc.poll() is not None:
                    self.stop_pipeline("downstream exited")
                    pipeline_live = False
                    self.flv_splicer = None
                    self.stop_event.wait(CRASH_DELAY)
                    continue

                self.stop_event.wait(RESTART_DELAY)
        finally:
            self.on_live_ended()
            self.stop_pipeline("relay stopped")
            update_stream_live_status(self.sid, self.user, False)


def main() -> None:
    if len(sys.argv) >= 3 and sys.argv[1] == "--filler":
        try:
            run_filler(int(sys.argv[2]))
        except BrokenPipeError:
            pass
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    log("محرك البث المتعدد قيد التشغيل (Session-Aware Splicing | يوتيوب كود شاكر | ميتا 2500k CBR | عرضي 180%)...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    workers = {}  # stream_id -> {"relay": ChannelRelay, "thread": Thread}

    def stop_channel(sid: str):
        entry = workers.pop(sid, None)
        if not entry:
            return
        entry["relay"].stop_event.set()
        entry["relay"].on_live_ended()
        entry["thread"].join(timeout=8)

    def handle_signal(_signum, _frame):
        SHUTDOWN_EVENT.set()
        for sid in list(workers.keys()):
            stop_channel(sid)
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while not SHUTDOWN_EVENT.is_set():
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
            for sid in list(workers.keys()):
                stop_channel(sid)

        configured = load_streams()
        active_map = {s["id"]: s for s in configured if s.get("enabled", True)}

        for sid in list(workers.keys()):
            if sid not in active_map:
                stop_channel(sid)

        for sid, sdata in active_map.items():
            if sid in workers:
                old = workers[sid]["relay"].data
                if (
                    old.get("target_url") != sdata.get("target_url")
                    or old.get("layout") != sdata.get("layout")
                    or old.get("platform") != sdata.get("platform")
                ):
                    log(f"🔄 تحديث بث @{sdata['tiktok_user']} دون المساس ببقية البثوث...")
                    stop_channel(sid)

        for sid, sdata in active_map.items():
            if sid not in workers or not workers[sid]["thread"].is_alive():
                relay = ChannelRelay(sdata)
                th = threading.Thread(target=relay.run, daemon=True, name=f"relay-{sid}")
                th.start()
                workers[sid] = {"relay": relay, "thread": th}

        time.sleep(2)


if __name__ == "__main__":
    main()
