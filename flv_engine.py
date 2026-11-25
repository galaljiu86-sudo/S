#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Proven FLV parsing, session-aware GOP-deduplicating splicer, prewarm ring buffer,
and real-time NALU filler + pacing stage.
"""

from __future__ import annotations

from collections import deque
from itertools import chain
import os
import queue
import signal
import struct
import sys
import threading
import time
from typing import BinaryIO, Callable, Optional


def env_value(name: str, default, cast=str):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except (TypeError, ValueError):
        return default


PACE_BUFFER_SEC = env_value("PACE_BUFFER_SEC", 5.0, float)
PACE_PREFILL_TIMEOUT = env_value("PACE_PREFILL_TIMEOUT", 20.0, float)
FILLER_QUEUE_MAX_TAGS = env_value("FILLER_QUEUE_MAX_TAGS", 2000, int)
MIN_RESERVE_SEC = env_value("MIN_RESERVE_SEC", 3.0, float)
PREWARM_MAX_SPAN_MS = env_value("PREWARM_MAX_SPAN_MS", 4500, int)
PREWARM_MAX_BYTES = env_value("PREWARM_MAX_BYTES", 24 * 1024 * 1024, int)


class StreamStalled(Exception):
    """No bytes or insufficient throughput from the current Streamlink session."""


class QueueStreamReader:
    """Thread-decoupled stream reader with silence + 12s throughput stall checks."""

    def __init__(
        self,
        chunks: queue.Queue,
        stop_event: threading.Event,
        live_ended_event: threading.Event,
        stall_seconds: float,
    ):
        self.chunks = chunks
        self.stop_event = stop_event
        self.live_ended_event = live_ended_event
        self.stall_seconds = max(3.0, float(stall_seconds))
        self.buffer = bytearray()
        self.eof = False
        now = time.monotonic()
        self.last_data = now
        self.first_byte_at: Optional[float] = None
        self.window_start = now
        self.window_bytes = 0
        self.total_bytes = 0

    def _check_health(self) -> None:
        now = time.monotonic()
        if now - self.last_data >= self.stall_seconds:
            raise StreamStalled(
                f"No source bytes for {self.stall_seconds:.1f}s"
            )
        if (
            self.first_byte_at is not None
            and (now - self.first_byte_at) >= 5.0
            and (now - self.window_start) >= 12.0
        ):
            if self.window_bytes < 64 * 1024:
                raise StreamStalled(
                    f"Source throughput stalled ({self.window_bytes}B in 12s)"
                )
            self.window_start = now
            self.window_bytes = 0

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size < 0:
            raise ValueError("QueueStreamReader only supports bounded reads")

        while len(self.buffer) < size and not self.eof:
            if self.stop_event.is_set():
                raise InterruptedError("Relay is stopping")

            if self.live_ended_event.is_set():
                try:
                    kind, payload = self.chunks.get_nowait()
                except queue.Empty:
                    self.eof = True
                    break
            else:
                try:
                    kind, payload = self.chunks.get(timeout=1.0)
                except queue.Empty:
                    self._check_health()
                    continue

            if kind == "data":
                if payload:
                    now = time.monotonic()
                    if self.first_byte_at is None:
                        self.first_byte_at = now
                        self.window_start = now
                    self.buffer.extend(payload)
                    self.last_data = now
                    nbytes = len(payload)
                    self.window_bytes += nbytes
                    self.total_bytes += nbytes
                    self._check_health()
            elif kind == "eof":
                self.eof = True
            elif kind == "error":
                if self.stop_event.is_set():
                    raise InterruptedError("Relay is stopping")
                if self.live_ended_event.is_set():
                    self.eof = True
                else:
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


def read_exact(handle, size: int, what: str) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        part = handle.read(size - len(chunks))
        if not part:
            break
        chunks.extend(part)
    if len(chunks) != size:
        raise EOFError(f"Truncated {what}: wanted {size}, received {len(chunks)}")
    return bytes(chunks)


def read_flv_tag(handle):
    header = handle.read(11)
    if not header:
        return None
    if len(header) < 11:
        rest = read_exact(handle, 11 - len(header), "FLV tag header")
        header = header + rest

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


def classify_video_tag(payload: bytes):
    """Return (codec, is_sequence_header, is_frame, is_keyframe)."""
    if not payload:
        return None, False, False, False

    first = payload[0]
    if first & 0x80:
        if len(payload) < 5:
            return None, False, False, False
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


class FlvSessionSplicer:
    """Splice FLV sessions while deduplicating overlapping CDN GOP-cache packets.

    Uses independent audio/video raw timestamp watermarks so minor A/V interleave
    jitter near reconnect boundaries never causes a valid IDR keyframe to be dropped.
    """

    def __init__(self, write_bytes: Callable[[bytes], None], allow_hevc: bool = False):
        self.write_bytes = write_bytes
        self.allow_hevc = allow_hevc
        self.header_written = False
        self.last_output_timestamp = -1
        self.last_raw_timestamp: Optional[int] = None
        self.last_raw_audio_ts: Optional[int] = None
        self.last_raw_video_ts: Optional[int] = None
        self.current_offset = 0
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

        first_tag = None
        while True:
            tag = read_flv_tag(handle)
            if tag is None:
                return {"empty": True}
            if tag[0] in (8, 9):
                first_tag = tag
                break

        if not self.header_written:
            self.write_bytes(flv_header + extra_header + previous0)
            self.header_written = True

        nal_length_size = None
        video_codec = None
        video_started = False
        dropped_video = 0
        dropped_dup = 0
        video_written = 0
        audio_written = 0
        avc_headers = 0
        aac_headers = 0
        first_idr_timestamp = None
        last_session_ts = self.last_output_timestamp
        timestamp_offset: Optional[int] = None
        same_timeline = False
        pending_seq_headers = []

        for tag in chain((first_tag,), iter(lambda: read_flv_tag(handle), None)):
            tag_type, timestamp, stream_id, payload = tag

            if tag_type == 18:
                continue

            is_seq_header = False

            if tag_type == 8:
                if len(payload) >= 2 and (payload[0] >> 4) == 10 and payload[1] == 0:
                    aac_headers += 1
                    is_seq_header = True

            elif tag_type == 9:
                if len(payload) < 2:
                    raise ValueError(f"{source_name}: truncated FLV video tag")

                codec_id = payload[0] & 0x0F
                if payload[0] & 0x80:
                    if not self.allow_hevc:
                        raise ValueError(
                            f"{source_name}: expected legacy H.264/AVC FLV tag"
                        )
                    codec, is_seq, is_frame, _ = classify_video_tag(payload)
                    if codec is None:
                        raise ValueError(f"{source_name}: unsupported enhanced video tag")
                    video_codec = codec
                    is_seq_header = is_seq
                    if is_seq:
                        avc_headers += 1
                else:
                    if codec_id != 7 and not (self.allow_hevc and codec_id == 12):
                        raise ValueError(
                            f"{source_name}: expected legacy H.264/AVC FLV tag (got codec_id={codec_id})"
                        )
                    video_codec = "h264" if codec_id == 7 else "hevc"
                    packet_type = payload[1]

                    if packet_type == 0:
                        if video_codec == "h264":
                            if len(payload) < 10:
                                raise ValueError(f"{source_name}: invalid AVC sequence header")
                            nal_length_size = (payload[9] & 0x03) + 1
                        else:
                            if len(payload) < 27:
                                raise ValueError(f"{source_name}: invalid HEVC sequence header")
                            nal_length_size = (payload[26] & 0x03) + 1
                        avc_headers += 1
                        is_seq_header = True

                    elif packet_type == 1:
                        if nal_length_size is None:
                            raise ValueError(
                                f"{source_name}: video media arrived before sequence header"
                            )
                    elif packet_type == 2:
                        if not video_started:
                            continue
                    else:
                        raise ValueError(
                            f"{source_name}: unsupported video packet type {packet_type}"
                        )
            else:
                continue

            if is_seq_header and timestamp_offset is None:
                pending_seq_headers.append((tag_type, stream_id, payload))
                continue

            if timestamp_offset is None:
                if self.last_raw_timestamp is None:
                    timestamp_offset = 0
                    self.current_offset = 0
                    same_timeline = True
                elif (
                    self.last_raw_timestamp - 30000
                    <= timestamp
                    <= self.last_raw_timestamp + 120000
                ):
                    # Continuous TikTok CDN clock: preserve offset and deduplicate overlap.
                    timestamp_offset = self.current_offset
                    same_timeline = True
                else:
                    # Source clock reset or jumped unexpectedly; re-anchor cleanly.
                    timestamp_offset = max(
                        0, self.last_output_timestamp + 1 - timestamp
                    )
                    self.current_offset = timestamp_offset
                    same_timeline = False
                    self.last_raw_timestamp = None
                    self.last_raw_audio_ts = None
                    self.last_raw_video_ts = None

            # Drop overlapping GOP-cache packets already transmitted in prior session.
            if not is_seq_header and same_timeline:
                track_watermark = (
                    self.last_raw_audio_ts if tag_type == 8 else self.last_raw_video_ts
                )
                if track_watermark is not None and timestamp <= track_watermark:
                    dropped_dup += 1
                    continue

            if pending_seq_headers:
                first_ts = max(
                    self.last_output_timestamp, timestamp + timestamp_offset
                )
                for seq_type, seq_stream_id, seq_payload in pending_seq_headers:
                    encoded_seq = encode_flv_tag(
                        seq_type, first_ts, seq_stream_id, seq_payload
                    )
                    self.write_bytes(encoded_seq)
                    self.last_output_timestamp = max(
                        self.last_output_timestamp, first_ts
                    )
                    last_session_ts = max(last_session_ts, first_ts)
                pending_seq_headers.clear()

            if tag_type == 9 and not is_seq_header:
                _, _, is_frame, is_keyframe = classify_video_tag(payload)
                if is_frame and not video_started:
                    if video_codec == "h264" and not (payload[0] & 0x80):
                        has_idr = h264_packet_has_idr(payload, nal_length_size)
                    else:
                        has_idr = is_keyframe
                    if not has_idr:
                        dropped_video += 1
                        self.last_raw_video_ts = max(
                            self.last_raw_video_ts if self.last_raw_video_ts is not None else -1,
                            timestamp,
                        )
                        self.last_raw_timestamp = max(
                            self.last_raw_timestamp if self.last_raw_timestamp is not None else -1,
                            timestamp,
                        )
                        continue
                    video_started = True

            if is_seq_header:
                output_timestamp = max(
                    self.last_output_timestamp, timestamp + timestamp_offset
                )
            else:
                output_timestamp = max(0, timestamp + timestamp_offset)
                self.last_raw_timestamp = max(
                    self.last_raw_timestamp if self.last_raw_timestamp is not None else -1,
                    timestamp,
                )
                if tag_type == 8:
                    self.last_raw_audio_ts = max(
                        self.last_raw_audio_ts if self.last_raw_audio_ts is not None else -1,
                        timestamp,
                    )
                elif tag_type == 9:
                    self.last_raw_video_ts = max(
                        self.last_raw_video_ts if self.last_raw_video_ts is not None else -1,
                        timestamp,
                    )

            if tag_type == 8 and not is_seq_header:
                audio_written += 1
            elif tag_type == 9 and not is_seq_header:
                video_written += 1
                _, _, is_frame, _ = classify_video_tag(payload)
                if is_frame and first_idr_timestamp is None:
                    first_idr_timestamp = output_timestamp

            encoded = encode_flv_tag(
                tag_type, output_timestamp, stream_id, payload
            )
            self.write_bytes(encoded)
            self.last_output_timestamp = max(
                self.last_output_timestamp, output_timestamp
            )
            last_session_ts = max(last_session_ts, output_timestamp)

        if audio_written == 0 and video_written == 0:
            return {"empty": True}

        self.session_count += 1
        return {
            "empty": False,
            "session": self.session_count,
            "source": source_name,
            "offset": timestamp_offset or 0,
            "nal_length_size": nal_length_size,
            "avc_headers": avc_headers,
            "aac_headers": aac_headers,
            "dropped_dup": dropped_dup,
            "dropped_video": dropped_video,
            "video_written": video_written,
            "audio_written": audio_written,
            "first_idr": first_idr_timestamp,
            "last_timestamp": last_session_ts,
        }


class PrewarmRingBuffer:
    """IDR-aligned rolling FLV GOP buffer for sub-second standby -> active handoffs."""

    def __init__(
        self,
        max_span_ms: int = PREWARM_MAX_SPAN_MS,
        max_bytes: int = PREWARM_MAX_BYTES,
    ):
        self.max_span_ms = max(1500, int(max_span_ms))
        self.max_bytes = max(2 * 1024 * 1024, int(max_bytes))
        self.lock = threading.Lock()
        self.flv_header: Optional[bytes] = None
        self.last_video_seq: Optional[bytes] = None
        self.last_audio_seq: Optional[bytes] = None
        # Each entry: (timestamp_ms, tag_type, is_idr, raw_tag_bytes)
        self.tags: deque[tuple[int, int, bool, bytes]] = deque()
        self.total_bytes = 0
        self.idr_count = 0

    def ingest_bytes(self, chunk: bytes) -> None:
        """Accept raw FLV bytes emitted by FlvSessionSplicer."""
        with self.lock:
            offset = 0
            if self.flv_header is None and len(chunk) >= 13 and chunk[:3] == b"FLV":
                data_offset = struct.unpack(">I", chunk[5:9])[0]
                hdr_len = data_offset + 4
                if len(chunk) >= hdr_len:
                    self.flv_header = chunk[:hdr_len]
                    offset = hdr_len

            while offset + 15 <= len(chunk):
                tag_type = chunk[offset]
                data_size = int.from_bytes(chunk[offset + 1:offset + 4], "big")
                tag_len = 11 + data_size + 4
                if offset + tag_len > len(chunk):
                    break

                raw_tag = chunk[offset:offset + tag_len]
                timestamp = (
                    int.from_bytes(chunk[offset + 4:offset + 7], "big")
                    | (chunk[offset + 7] << 24)
                )
                payload = chunk[offset + 11:offset + 11 + data_size]
                offset += tag_len

                if tag_type == 8 and len(payload) >= 2 and (payload[0] >> 4) == 10 and payload[1] == 0:
                    self.last_audio_seq = raw_tag
                    continue

                is_idr = False
                if tag_type == 9:
                    _, is_seq, is_frame, is_keyframe = classify_video_tag(payload)
                    if is_seq:
                        self.last_video_seq = raw_tag
                        continue
                    is_idr = bool(is_frame and is_keyframe)

                if tag_type in (8, 9):
                    self.tags.append((timestamp, tag_type, is_idr, raw_tag))
                    self.total_bytes += tag_len
                    if is_idr:
                        self.idr_count += 1
                    self._trim_unlocked()

    def _trim_unlocked(self) -> None:
        if not self.tags:
            return
        newest_ts = self.tags[-1][0]
        # Keep trimming up to a newer IDR whenever buffer exceeds span or byte cap.
        while len(self.tags) > 2:
            oldest_ts = self.tags[0][0]
            span = newest_ts - oldest_ts
            if span <= self.max_span_ms and self.total_bytes <= self.max_bytes:
                break

            # Find next IDR after index 0
            next_idr_idx = None
            for i in range(1, len(self.tags)):
                if self.tags[i][2]:
                    next_idr_idx = i
                    break

            if next_idr_idx is None:
                break
            if (
                span > self.max_span_ms
                and (newest_ts - self.tags[next_idr_idx][0]) < 1500
                and self.total_bytes <= self.max_bytes
            ):
                break

            for _ in range(next_idr_idx):
                _, _, was_idr, popped = self.tags.popleft()
                self.total_bytes = max(0, self.total_bytes - len(popped))
                if was_idr:
                    self.idr_count = max(0, self.idr_count - 1)

    def is_warm(self, min_span_ms: int = 1200) -> bool:
        with self.lock:
            if self.flv_header is None or self.last_video_seq is None or not self.tags:
                return False
            first_idr_idx = next((i for i, item in enumerate(self.tags) if item[2]), None)
            if first_idr_idx is None:
                return False
            return (self.tags[-1][0] - self.tags[first_idr_idx][0]) >= min_span_ms

    def drain_aligned(self) -> list[bytes]:
        """Return FLV header, seq headers (re-stamped to first IDR), and IDR-aligned tags."""
        with self.lock:
            if self.flv_header is None:
                return []

            out = [self.flv_header]
            first_idr_idx = next((i for i, item in enumerate(self.tags) if item[2]), None)
            if first_idr_idx is None:
                self.tags.clear()
                self.total_bytes = 0
                self.idr_count = 0
                return out

            first_ts = self.tags[first_idr_idx][0]
            for seq_tag in (self.last_video_seq, self.last_audio_seq):
                if seq_tag is not None:
                    tag_type = seq_tag[0]
                    data_size = int.from_bytes(seq_tag[1:4], "big")
                    stream_id = seq_tag[8:11]
                    payload = seq_tag[11:11 + data_size]
                    out.append(encode_flv_tag(tag_type, first_ts, stream_id, payload))

            for idx, (ts, _, _, raw_tag) in enumerate(self.tags):
                if idx >= first_idr_idx:
                    out.append(raw_tag)

            self.tags.clear()
            self.total_bytes = 0
            self.idr_count = 0
            return out


class PrewarmSink:
    """Thread-safe sink switching cleanly from PrewarmRingBuffer to live pipeline."""

    def __init__(self, ring: Optional[PrewarmRingBuffer] = None):
        self.lock = threading.Lock()
        self.ring = ring
        self.live_writer: Optional[Callable[[bytes], None]] = None

    def write(self, data: bytes) -> None:
        with self.lock:
            if self.live_writer is not None:
                self.live_writer(data)
            elif self.ring is not None:
                self.ring.ingest_bytes(data)

    def activate(self, live_writer: Callable[[bytes], None]) -> int:
        """Flush buffered IDR-aligned GOP into live_writer and switch atomically."""
        with self.lock:
            flushed = 0
            if self.ring is not None:
                for chunk in self.ring.drain_aligned():
                    live_writer(chunk)
                    flushed += len(chunk)
                self.ring = None
            self.live_writer = live_writer
            return flushed


def run_filler(
    kbps: int,
    slate_path: Optional[str] = None,
    slate_min: float = 0,
    stdin_stream: Optional[BinaryIO] = None,
    stdout_stream: Optional[BinaryIO] = None,
) -> None:
    """Real-time FLV pacing + optional H.264/HEVC NALU bitrate filler.

    When kbps > 0: injects H.264/HEVC filler NALU bytes up to target kbps AND paces
    tags in real time after buffering PACE_BUFFER_SEC.
    When kbps == 0: performs pure real-time pacing + PACE_BUFFER_SEC cushioning
    without altering video NALU payloads (ideal for CBR re-encode & landscape_raw).
    """
    if kbps < 0:
        raise ValueError("FILLER_KBPS must be >= 0")

    def flog(message: str) -> None:
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}",
            file=sys.stderr,
            flush=True,
        )

    in_stream = stdin_stream if stdin_stream is not None else sys.stdin.buffer
    output = stdout_stream if stdout_stream is not None else sys.stdout.buffer
    bytes_per_ms = (kbps * 1000 / 8 / 1000.0) if kbps > 0 else 0.0
    tags: queue.Queue = queue.Queue(maxsize=max(32, FILLER_QUEUE_MAX_TAGS))
    reader_done = threading.Event()
    source_info = {"base": None, "newest": 0, "consumed": None}

    def reader() -> None:
        try:
            for tag in iter_flv_tags(in_stream):
                tag_type, timestamp, _payload = tag
                if tag_type in (8, 9):
                    if source_info["base"] is None:
                        source_info["base"] = timestamp
                    source_info["newest"] = max(source_info["newest"], timestamp)
                tags.put(tag)
        except (BrokenPipeError, OSError):
            pass
        except Exception as exc:
            flog(f"[FILLER] Reader error: {exc}")
        finally:
            reader_done.set()
            tags.put(None)

    def reserve_seconds() -> float:
        anchor = source_info["consumed"]
        if anchor is None:
            anchor = source_info["base"]
        if anchor is None:
            return 0.0
        return max(0.0, (source_info["newest"] - anchor) / 1000.0)

    threading.Thread(target=reader, name="flv-reader", daemon=True).start()

    state = {
        "nal_len": 4,
        "last_video_ts": None,
        "audio_bytes": 0,
        "carry_bytes": 0.0,
        "last_output_ts": 0,
    }

    flv_header = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00"
    header_written = {"done": False}

    def write_header_once() -> None:
        if not header_written["done"]:
            output.write(flv_header)
            header_written["done"] = True

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
        return classify_video_tag(payload)

    def emit(tag_type: int, timestamp: int, payload: bytes) -> None:
        write_header_once()
        data = payload
        data_size = len(data)

        if kbps > 0:
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

                    needed = (
                        int(state["carry_bytes"])
                        - (data_size + 15)
                        - state["audio_bytes"]
                    )
                    state["audio_bytes"] = 0

                    max_allowed_extra = max(0, 0xFFFFFF - data_size - 16)
                    if needed > 8 and max_allowed_extra > 8:
                        target_fill = min(needed - 7, 200000, max_allowed_extra)
                        extra = make_filler_nal(codec, target_fill)
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

    if slate_path and slate_min > 0 and os.path.isfile(slate_path):
        try:
            with open(slate_path, "rb") as handle:
                slate_tags = [tag for tag in iter_flv_tags(handle) if tag[0] in (8, 9)]
        except Exception as exc:
            flog(f"[SLATE] Could not read slate: {exc}")
            slate_tags = []

        start = time.monotonic()
        index = 0
        while slate_tags:
            elapsed = time.monotonic() - start
            source_ready = reserve_seconds() >= MIN_RESERVE_SEC
            if elapsed >= slate_min and source_ready:
                break
            if index >= len(slate_tags):
                break

            tag_type, timestamp, payload = slate_tags[index]
            if timestamp / 1000.0 <= elapsed:
                emit(tag_type, timestamp, payload)
                index += 1
            else:
                time.sleep(0.005)

            if reader_done.is_set() and tags.empty():
                break

    if PACE_BUFFER_SEC > 0:
        prefill_start = time.monotonic()
        while (
            reserve_seconds() < PACE_BUFFER_SEC
            and not reader_done.is_set()
            and time.monotonic() - prefill_start < PACE_PREFILL_TIMEOUT
        ):
            time.sleep(0.05)
        flog(
            f"[PACE] Pre-buffered {reserve_seconds():.1f}s "
            f"(target {PACE_BUFFER_SEC:.1f}s, filler={kbps}k); starting steady output."
        )

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

        source_info["consumed"] = timestamp

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

        assert first_source_ts is not None and wall_clock_start is not None
        target_elapsed = (timestamp - first_source_ts) / 1000.0

        now = time.monotonic()
        wait_seconds = target_elapsed - (now - wall_clock_start)
        if wait_seconds > 10.0:
            wall_clock_start -= wait_seconds
            wait_seconds = 0.0
        elif wait_seconds < -2.0:
            # Prevent burst-dumping the entire reserve buffer after a temporary source gap.
            wall_clock_start += (-wait_seconds - 1.0)
            wait_seconds = 0.0

        while wait_seconds > 0:
            time.sleep(min(wait_seconds, 0.05))
            wait_seconds = target_elapsed - (time.monotonic() - wall_clock_start)

        emit(tag_type, max(0, timestamp + timestamp_offset), payload)

    output.flush()


def main_cli() -> None:
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)

    if len(sys.argv) >= 4 and sys.argv[1] == "--test-files":
        output_path = sys.argv[2]
        input_paths = sys.argv[3:]
        with open(output_path, "wb") as output:
            splicer = FlvSessionSplicer(output.write)
            for number, path in enumerate(input_paths, 1):
                with open(path, "rb") as handle:
                    result = splicer.process_handle(handle, source_name=path)
                print(f"[TEST] Session {number}: {result}", flush=True)
        print(f"[TEST] Output written: {output_path}", flush=True)
        return

    if len(sys.argv) >= 3 and sys.argv[1] == "--filler":
        try:
            if len(sys.argv) >= 6 and sys.argv[3] == "--slate":
                run_filler(int(sys.argv[2]), sys.argv[4], float(sys.argv[5]))
            else:
                run_filler(int(sys.argv[2]))
        except BrokenPipeError:
            pass
        return


if __name__ == "__main__":
    main_cli()
