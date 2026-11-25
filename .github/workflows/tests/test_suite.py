#!/usr/bin/env python3
"""Comprehensive unit and integration tests for flv_engine, state_store, restreamer, and bot."""

from __future__ import annotations

import io
import os
import py_compile
import queue
import shutil
import struct
import sys
import tempfile
import threading
import types
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import flv_engine  # noqa: E402
import state_store  # noqa: E402


def _ensure_optional_stubs() -> None:
    """Provide minimal stubs only when runtime packages are absent in local test Python."""
    if "requests" not in sys.modules:
        try:
            __import__("requests")
        except ImportError:
            mod = types.ModuleType("requests")
            mod.post = lambda *a, **kw: None
            sys.modules["requests"] = mod

    if "streamlink" not in sys.modules:
        try:
            __import__("streamlink")
        except ImportError:
            mod = types.ModuleType("streamlink")

            class _FakeSession:
                def set_option(self, *a, **kw):
                    pass

                def streams(self, *a, **kw):
                    return {}

                class http:
                    @staticmethod
                    def close():
                        pass

            mod.Streamlink = _FakeSession
            sys.modules["streamlink"] = mod

    if "psutil" not in sys.modules:
        try:
            __import__("psutil")
        except ImportError:
            mod = types.ModuleType("psutil")
            mod.net_io_counters = lambda: types.SimpleNamespace(bytes_sent=0, bytes_recv=0)
            mod.cpu_percent = lambda interval=0: 0.0
            mod.virtual_memory = lambda: types.SimpleNamespace(percent=0.0, used=0, total=1024 * 1024)
            sys.modules["psutil"] = mod

    if "telebot" not in sys.modules:
        try:
            __import__("telebot")
        except ImportError:
            mod = types.ModuleType("telebot")
            tmod = types.ModuleType("telebot.types")

            class _Markup:
                def __init__(self, row_width=2):
                    self.row_width = row_width
                    self.buttons = []

                def add(self, *items):
                    self.buttons.extend(items)

            class _Btn:
                def __init__(self, text, callback_data=None):
                    self.text = text
                    self.callback_data = callback_data

            tmod.InlineKeyboardMarkup = _Markup
            tmod.ReplyKeyboardMarkup = _Markup
            tmod.InlineKeyboardButton = _Btn
            tmod.KeyboardButton = _Btn
            tmod.ReplyKeyboardRemove = lambda: types.SimpleNamespace(remove=True)

            class _TeleBot:
                def __init__(self, *a, **kw):
                    pass

                def message_handler(self, *a, **kw):
                    return lambda fn: fn

                def callback_query_handler(self, *a, **kw):
                    return lambda fn: fn

            mod.TeleBot = _TeleBot
            mod.types = tmod
            sys.modules["telebot"] = mod
            sys.modules["telebot.types"] = tmod


def make_flv_header() -> bytes:
    return b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00"


def make_aac_seq_header(ts: int = 0) -> bytes:
    payload = b"\xaf\x00\x12\x10"
    return flv_engine.encode_flv_tag(8, ts, b"\x00\x00\x00", payload)


def make_aac_frame(ts: int, marker: bytes = b"AUD") -> bytes:
    payload = b"\xaf\x01" + marker
    return flv_engine.encode_flv_tag(8, ts, b"\x00\x00\x00", payload)


def make_avc_seq_header(ts: int = 0) -> bytes:
    # lengthSizeMinusOne = 3 -> 4-byte NALU lengths (byte 9 = 0xFF)
    payload = bytes([0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, 0x00, 0x1F, 0xFF, 0xE1])
    return flv_engine.encode_flv_tag(9, ts, b"\x00\x00\x00", payload)


def make_avc_frame(ts: int, is_idr: bool, tag_data: bytes = b"") -> bytes:
    frame_type = 0x17 if is_idr else 0x27
    nal_header = 0x65 if is_idr else 0x41
    nal_body = bytes([nal_header]) + tag_data + b"\x01\x02"
    nalu = struct.pack(">I", len(nal_body)) + nal_body
    payload = bytes([frame_type, 0x01, 0x00, 0x00, 0x00]) + nalu
    return flv_engine.encode_flv_tag(9, ts, b"\x00\x00\x00", payload)


def parse_all_tags(raw_flv: bytes) -> list[tuple[int, int, bytes]]:
    return list(flv_engine.iter_flv_tags(io.BytesIO(raw_flv)))


class TestSyntaxCompilation(unittest.TestCase):
    def test_all_python_files_compile(self):
        for fname in ("flv_engine.py", "state_store.py", "restreamer.py", "bot.py"):
            path = os.path.join(ROOT_DIR, fname)
            py_compile.compile(path, doraise=True)


class TestFlvSessionSplicer(unittest.TestCase):
    def test_dedup_continuous_timeline_and_interleaved_idr_preserved(self):
        # Session 1 ends with Audio at 5000ms and Video at 4967ms.
        s1 = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(4900, is_idr=True, tag_data=b"S1_IDR")
            + make_aac_frame(4930, b"A1")
            + make_avc_frame(4967, is_idr=False, tag_data=b"S1_P1")
            + make_aac_frame(5000, b"A2")
        )
        # Session 2 reconnects with GOP cache starting at 4900ms, including:
        # - Audio 4930, 5000 (duplicates -> must skip)
        # - Video IDR at 4999 (> 4967 video watermark, < 5000 audio watermark -> MUST BE KEPT!)
        # - Audio 5030, Video P at 5033 (new)
        s2 = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(4900, is_idr=True, tag_data=b"S1_IDR_DUP")
            + make_aac_frame(4930, b"A1_DUP")
            + make_avc_frame(4967, is_idr=False, tag_data=b"S1_P1_DUP")
            + make_avc_frame(4999, is_idr=True, tag_data=b"S2_IDR_NEW")
            + make_aac_frame(5000, b"A2_DUP")
            + make_aac_frame(5030, b"A3_NEW")
            + make_avc_frame(5033, is_idr=False, tag_data=b"S2_P1_NEW")
        )

        out = bytearray()
        splicer = flv_engine.FlvSessionSplicer(out.extend)
        res1 = splicer.process_handle(io.BytesIO(s1), "s1")
        res2 = splicer.process_handle(io.BytesIO(s2), "s2")

        self.assertFalse(res1["empty"])
        self.assertEqual(res1["offset"], 0)
        self.assertFalse(res2["empty"])
        self.assertEqual(res2["offset"], 0)
        self.assertEqual(res2["dropped_dup"], 4)  # 4900v, 4930a, 4967v, 5000a
        self.assertEqual(res2["video_written"], 2)  # 4999 IDR + 5033 P
        self.assertEqual(res2["audio_written"], 1)  # 5030 audio

        tags = parse_all_tags(bytes(out))
        video_markers = [p for t, ts, p in tags if t == 9 and p[1] == 1]
        self.assertTrue(any(b"S2_IDR_NEW" in p for p in video_markers))
        self.assertFalse(any(b"S1_IDR_DUP" in p for p in video_markers))

    def test_drops_non_idr_until_first_idr_and_anchors_seq_headers(self):
        # Seq headers at ts=0, media starts at ts=3600000 (1 hour in) with P-frame first!
        s1 = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(3600000, is_idr=False, tag_data=b"PRE_P")
            + make_avc_frame(3600033, is_idr=True, tag_data=b"FIRST_IDR")
            + make_aac_frame(3600040, b"A_OK")
        )
        out = bytearray()
        splicer = flv_engine.FlvSessionSplicer(out.extend)
        res = splicer.process_handle(io.BytesIO(s1), "s1")

        self.assertEqual(res["dropped_video"], 1)
        self.assertEqual(res["first_idr"], 3600033)
        tags = parse_all_tags(bytes(out))
        # Sequence headers must NOT remain at ts=0 when media is at 3,600,000ms!
        seq_ts = [ts for t, ts, p in tags if p[1] == 0]
        self.assertEqual(seq_ts, [3600000, 3600000])

    def test_clock_reset_updates_new_clock_watermarks_for_session3(self):
        s1 = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(500000, is_idr=True, tag_data=b"S1")
            + make_aac_frame(500020, b"A1")
        )
        # Session 2 clock resets back to 100ms (outside -30s..+120s window)
        s2 = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(100, is_idr=True, tag_data=b"S2")
            + make_aac_frame(120, b"A2")
        )
        # Session 3 reconnects on clock B (GOP cache 100..120, new 160..200)
        s3 = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(100, is_idr=True, tag_data=b"S2_DUP")
            + make_aac_frame(120, b"A2_DUP")
            + make_avc_frame(160, is_idr=True, tag_data=b"S3_NEW")
            + make_aac_frame(200, b"A3_NEW")
        )

        out = bytearray()
        splicer = flv_engine.FlvSessionSplicer(out.extend)
        splicer.process_handle(io.BytesIO(s1), "s1")
        r2 = splicer.process_handle(io.BytesIO(s2), "s2")
        r3 = splicer.process_handle(io.BytesIO(s3), "s3")

        self.assertEqual(r2["offset"], 500021 - 100)
        self.assertEqual(r3["offset"], r2["offset"])
        self.assertEqual(r3["dropped_dup"], 2)
        self.assertEqual(r3["video_written"], 1)
        self.assertEqual(r3["audio_written"], 1)


class TestQueueStreamReader(unittest.TestCase):
    def test_drains_on_live_end_and_detects_stall(self):
        q = queue.Queue()
        stop = threading.Event()
        live_ended = threading.Event()
        q.put(("data", b"hello "))
        q.put(("data", b"world"))
        live_ended.set()

        reader = flv_engine.QueueStreamReader(q, stop, live_ended, stall_seconds=3.0)
        self.assertEqual(reader.read(11), b"hello world")
        self.assertEqual(reader.read(4), b"")

    def test_silence_stall_raises(self):
        q = queue.Queue()
        stop = threading.Event()
        live_ended = threading.Event()
        reader = flv_engine.QueueStreamReader(q, stop, live_ended, stall_seconds=3.0)
        reader.last_data -= 3.5
        with self.assertRaises(flv_engine.StreamStalled):
            reader.read(10)

    def test_low_throughput_stall_raises(self):
        q = queue.Queue()
        stop = threading.Event()
        live_ended = threading.Event()
        reader = flv_engine.QueueStreamReader(q, stop, live_ended, stall_seconds=10.0)
        reader.first_byte_at = reader.last_data - 6.0
        reader.window_start = reader.last_data - 12.5
        reader.window_bytes = 1024
        q.put(("data", b"x"))
        with self.assertRaises(flv_engine.StreamStalled):
            reader.read(10)


class TestPrewarmAndFiller(unittest.TestCase):
    def test_prewarm_ring_trims_old_gop_and_activates(self):
        ring = flv_engine.PrewarmRingBuffer(max_span_ms=2000)
        sink = flv_engine.PrewarmSink(ring)

        sample = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(1000, is_idr=True, tag_data=b"OLD_GOP")
            + make_avc_frame(1500, is_idr=False, tag_data=b"OLD_P")
            + make_avc_frame(3000, is_idr=True, tag_data=b"MID_GOP")
            + make_aac_frame(3020, b"MID_A")
            + make_avc_frame(4500, is_idr=True, tag_data=b"NEW_GOP")
            + make_aac_frame(4520, b"NEW_A")
            + make_avc_frame(5800, is_idr=False, tag_data=b"NEW_P")
        )
        splicer = flv_engine.FlvSessionSplicer(sink.write)
        splicer.process_handle(io.BytesIO(sample), "prewarm")

        self.assertTrue(ring.is_warm(min_span_ms=1000))
        live_out = bytearray()
        flushed = sink.activate(live_out.extend)
        self.assertGreater(flushed, 0)

        tags = parse_all_tags(bytes(live_out))
        video_payloads = [p for t, ts, p in tags if t == 9 and p[1] == 1]
        self.assertFalse(any(b"OLD_GOP" in p for p in video_payloads))
        self.assertTrue(any(b"NEW_GOP" in p for p in video_payloads))
        # First emitted media frame after seq headers must be IDR
        self.assertEqual(video_payloads[0][0], 0x17)

    def test_run_filler_injects_nalu_and_pace_only_preserves_payload(self):
        raw = (
            make_flv_header()
            + make_avc_seq_header(0)
            + make_aac_seq_header(0)
            + make_avc_frame(0, is_idr=True, tag_data=b"V0")
            + make_aac_frame(20, b"A0")
            + make_avc_frame(33, is_idr=False, tag_data=b"V1")
            + make_avc_frame(66, is_idr=False, tag_data=b"V2")
        )

        old_pace = flv_engine.PACE_BUFFER_SEC
        try:
            flv_engine.PACE_BUFFER_SEC = 0.0

            out_fill = io.BytesIO()
            flv_engine.run_filler(
                3850,
                stdin_stream=io.BytesIO(raw),
                stdout_stream=out_fill,
            )
            filled_tags = parse_all_tags(out_fill.getvalue())
            v_filled = [p for t, ts, p in filled_tags if t == 9 and p[1] == 1]
            self.assertEqual(len(v_filled), 3)
            # Subsequent frames must have filler NALUs appended (larger than original)
            self.assertGreater(len(v_filled[1]), len(make_avc_frame(33, False, b"V1")) - 15)
            self.assertIn(b"\x0c\xff", v_filled[1])

            out_pace = io.BytesIO()
            flv_engine.run_filler(
                0,
                stdin_stream=io.BytesIO(raw),
                stdout_stream=out_pace,
            )
            paced_tags = parse_all_tags(out_pace.getvalue())
            v_paced = [p for t, ts, p in paced_tags if t == 9 and p[1] == 1]
            self.assertEqual(len(v_paced), 3)
            self.assertNotIn(b"\x0c\xff", v_paced[1])
        finally:
            flv_engine.PACE_BUFFER_SEC = old_pace


class TestStateStoreAndIntegrations(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="stream_test_")
        state_store.DATA_DIR = self.tmpdir
        state_store.STREAMS_PATH = os.path.join(self.tmpdir, "streams.json")
        state_store.STATUS_PATH = os.path.join(self.tmpdir, "status.json")
        state_store.CONFIG_PATH = os.path.join(self.tmpdir, "global.json")
        state_store._LAST_VALID_STREAMS = []

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_concurrent_mutations_preserve_all_entries(self):
        errors = []

        def worker(i: int):
            try:
                def mut(streams):
                    streams.append({
                        "id": f"id_{i}",
                        "tiktok_user": f"user_{i}",
                        "platform": "YouTube",
                        "target_url": f"rtmp://example/live/{i}",
                    })
                state_store.mutate_streams(mut, touch_reload=False)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(15)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        loaded = state_store.load_streams()
        self.assertEqual(len(loaded), 15)

        state_store.update_stream_status("id_0", "user_0", True, {"sessions": 2})
        merged = state_store.load_streams(include_status=True)
        by_id = {s["id"]: s for s in merged}
        self.assertTrue(by_id["id_0"]["is_live"])
        self.assertEqual(by_id["id_0"]["runtime_status"]["sessions"], 2)

        state_store.prune_statuses({"id_1"})
        self.assertEqual(state_store.load_statuses(), {})

    def test_pipeline_and_bot_helpers(self):
        _ensure_optional_stubs()
        import bot  # noqa: E402
        import restreamer  # noqa: E402

        yt_copy = restreamer.build_persistent_pipeline(
            "rtmp://a.rtmp.youtube.com/live2/abc", "YouTube", "portrait_copy"
        )
        self.assertIn("-c:v copy", yt_copy)
        self.assertIn("aresample=async=1:min_hard_comp=0.100", yt_copy)
        self.assertIn("--filler 3850", yt_copy)

        yt_raw = restreamer.build_persistent_pipeline(
            "rtmp://a.rtmp.youtube.com/live2/abc", "YouTube", "landscape_raw"
        )
        self.assertIn("-c:v copy", yt_raw)
        self.assertIn("--filler 0", yt_raw)

        zoom_180 = restreamer.build_persistent_pipeline(
            "rtmps://live-api-s.facebook.com:443/rtmp/xyz",
            "Facebook",
            "landscape_to_portrait_pad",
        )
        self.assertIn("scale=w=1296", zoom_180)
        self.assertIn("pad=720:1280", zoom_180)
        self.assertIn("nal-hrd=cbr:force-cfr=1", zoom_180)
        self.assertIn("aresample=async=1:min_hard_comp=0.100", zoom_180)
        self.assertIn("--filler 0", zoom_180)

        self.assertEqual(
            bot.parse_tiktok_username("https://www.tiktok.com/@d.shakertawfiqalaroury/live"),
            "d.shakertawfiqalaroury",
        )
        self.assertEqual(bot.parse_tiktok_username("@someone_99"), "someone_99")
        self.assertEqual(bot.parse_tiktok_username("   "), "")
        self.assertTrue(bot.validate_rtmp_url("rtmps://live-api-s.facebook.com:443/rtmp/abc"))
        self.assertFalse(bot.validate_rtmp_url("http://example.com"))
        self.assertTrue(bot.validate_stream_key("50vb-qw3d-zvth"))
        self.assertFalse(bot.validate_stream_key("bad key with space"))

        msg, markup = bot.build_stream_card(0, {
            "id": "abc123",
            "tiktok_user": "demo",
            "platform": "YouTube",
            "layout": "portrait_copy",
            "stream_key": "50vb-qw3d-zvth",
            "enabled": True,
            "is_live": True,
            "runtime_status": {"sessions": 3, "dup_skipped": 42},
        })
        self.assertIn("@demo", msg)
        self.assertIn("42", msg)
        self.assertTrue(any(b.callback_data == "chlog_abc123" for b in markup.buttons))

    def test_control_handler_ready_sync_activate_and_cli_test_files(self):
        _ensure_optional_stubs()
        import json
        import restreamer

        def call_handler(method: str, path: str, body: bytes = b""):
            handler = object.__new__(restreamer._ControlHandler)
            handler.path = path
            handler.headers = {"Content-Length": str(len(body))}
            handler.rfile = io.BytesIO(body)
            handler.wfile = io.BytesIO()
            captured = {}

            def send_response(code):
                captured["code"] = code

            handler.send_response = send_response
            handler.send_header = lambda k, v: None
            handler.end_headers = lambda: None
            if method == "GET":
                handler.do_GET()
            else:
                handler.do_POST()
            return captured["code"], json.loads(handler.wfile.getvalue().decode("utf-8"))

        old_require_bot = restreamer.REQUIRE_BOT_READY
        old_standby_flag = restreamer.STANDBY_FLAG_PATH
        try:
            restreamer.REQUIRE_BOT_READY = 0
            restreamer.STANDBY_FLAG_PATH = os.path.join(self.tmpdir, "standby_mode")
            with open(restreamer.STANDBY_FLAG_PATH, "w", encoding="utf-8") as f:
                f.write("1")
            restreamer.CLUSTER_ACTIVE_EVENT.clear()
            restreamer.DAEMON_READY_EVENT.set()

            code, data = call_handler("GET", "/ready")
            self.assertEqual(code, 200)
            self.assertEqual(data["mode"], "standby")

            sync_body = json.dumps({
                "streams": [{
                    "id": "sync1",
                    "tiktok_user": "synced_user",
                    "platform": "YouTube",
                    "target_url": "rtmp://a.rtmp.youtube.com/live2/k1",
                }],
                "global": {"chat_id": 123456},
            }).encode("utf-8")
            code, sync_res = call_handler("POST", "/sync", sync_body)
            self.assertEqual(code, 200)
            self.assertTrue(sync_res["synced"])
            self.assertEqual(state_store.load_streams()[0]["tiktok_user"], "synced_user")
            self.assertEqual(state_store.load_global_config()["chat_id"], 123456)

            code, status_res = call_handler("GET", "/status")
            self.assertEqual(code, 200)
            self.assertIn("channels", status_res)

            code, act_data = call_handler("POST", "/activate")
            self.assertEqual(code, 200)
            self.assertTrue(act_data["activated"])
            self.assertTrue(act_data["was_standby"])
            self.assertTrue(restreamer.CLUSTER_ACTIVE_EVENT.is_set())
            self.assertFalse(os.path.exists(restreamer.STANDBY_FLAG_PATH))

            # Also verify flv_engine --test-files CLI
            in1 = os.path.join(self.tmpdir, "s1.flv")
            in2 = os.path.join(self.tmpdir, "s2.flv")
            out_flv = os.path.join(self.tmpdir, "out.flv")
            with open(in1, "wb") as f:
                f.write(
                    make_flv_header()
                    + make_avc_seq_header(0)
                    + make_aac_seq_header(0)
                    + make_avc_frame(1000, True, b"CLI1")
                )
            with open(in2, "wb") as f:
                f.write(
                    make_flv_header()
                    + make_avc_seq_header(0)
                    + make_aac_seq_header(0)
                    + make_avc_frame(1000, True, b"CLI1_DUP")
                    + make_avc_frame(1040, True, b"CLI2_NEW")
                )
            old_argv = sys.argv
            try:
                sys.argv = ["flv_engine.py", "--test-files", out_flv, in1, in2]
                flv_engine.main_cli()
            finally:
                sys.argv = old_argv

            with open(out_flv, "rb") as f:
                cli_tags = parse_all_tags(f.read())
            cli_video = [p for t, _, p in cli_tags if t == 9 and p[1] == 1]
            self.assertEqual(len(cli_video), 2)
            self.assertIn(b"CLI1", cli_video[0])
            self.assertIn(b"CLI2_NEW", cli_video[1])
        finally:
            restreamer.REQUIRE_BOT_READY = old_require_bot
            restreamer.STANDBY_FLAG_PATH = old_standby_flag

    def test_shell_and_python_files_use_lf_line_endings(self):
        for rel in (
            "start.sh",
            "requirements.txt",
            "flv_engine.py",
            "state_store.py",
            "restreamer.py",
            "bot.py",
            os.path.join(".github", "workflows", "run.yml"),
        ):
            full = os.path.join(ROOT_DIR, rel)
            with open(full, "rb") as f:
                raw = f.read()
            self.assertNotIn(b"\r\n", raw, f"CRLF detected in {rel}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
