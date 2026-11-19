#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
High-Performance Multi-Channel TikTok Restreamer with H.264 Filler NAL Padding.
Pads stream bitrate to 2000 - 2500 Kbps without re-encoding (-c:v copy).
Zero CPU re-encode overhead.
"""

import json
import os
import queue
import shlex
import struct
import subprocess
import sys
import threading
import time
import requests

DATA_DIR = os.path.abspath("data")
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

FILLER_KBPS = int(os.environ.get("FILLER_KBPS", 2000))  # رفع البت ريت لـ 2000k بدون إعادة ترميز

ACTIVE_PROCESSES = {}  # tiktok_user -> subprocess.Popen

def log(msg: str):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)

# ---------------------------------------------------------------------------
# محرك حشو البت ريت (H.264 Filler NAL Padding) بدون إعادة ترميز
# ---------------------------------------------------------------------------

def run_filler(target_kbps: int) -> None:
    """قراءة FLV وحقن باكتات NAL حشو H.264 (Type 12) لرفع البت ريت للرقم المطلوب."""
    output = sys.stdout.buffer
    input_stream = sys.stdin.buffer
    bytes_per_ms = target_kbps * 1000.0 / 8.0 / 1000.0

    header = input_stream.read(13)
    if len(header) < 13 or header[:3] != b"FLV":
        return
    output.write(header)
    output.flush()

    carry_bytes = 0.0
    last_video_ts = None
    audio_bytes = 0

    def make_filler(filler_bytes: int) -> bytes:
        filler_bytes = max(0, filler_bytes)
        # NAL length (4 bytes) + NAL header 0x0c (Type 12 Filler) + 0xff padding + 0x80 stop bit
        return (
            struct.pack(">I", filler_bytes + 2)
            + b"\x0c"
            + (b"\xff" * filler_bytes)
            + b"\x80"
        )

    while True:
        tag_header = input_stream.read(11)
        if len(tag_header) < 11:
            break

        tag_type = tag_header[0]
        data_size = int.from_bytes(tag_header[1:4], "big")
        timestamp = int.from_bytes(tag_header[4:7], "big") | (tag_header[7] << 24)
        stream_id = tag_header[8:11]

        payload = input_stream.read(data_size)
        prev_tag_size = input_stream.read(4)
        if len(payload) != data_size or len(prev_tag_size) != 4:
            break

        if tag_type == 8:  # Audio
            audio_bytes += data_size + 15
        elif tag_type == 9 and data_size > 5:  # Video
            first = payload[0]
            codec_id = first & 0x0F
            packet_type = payload[1]

            if codec_id == 7 and packet_type == 1:  # H.264 NALU frame
                if last_video_ts is not None:
                    delta = timestamp - last_video_ts
                    if 0 <= delta <= 300:
                        carry_bytes += delta * bytes_per_ms
                    else:
                        carry_bytes = 0.0
                last_video_ts = timestamp

                needed = int(carry_bytes) - (data_size + 15) - audio_bytes
                audio_bytes = 0

                carry_bytes = min(carry_bytes, bytes_per_ms * 50)

                if needed > 16:
                    filler_len = min(needed - 8, 30000)
                    extra = make_filler(filler_len)
                    payload += extra
                    data_size = len(payload)

                carry_bytes -= data_size + 15
                if carry_bytes < 0:
                    carry_bytes = 0.0

        ts = timestamp & 0xFFFFFFFF
        new_header = (
            bytes([tag_type])
            + data_size.to_bytes(3, "big")
            + (ts & 0xFFFFFF).to_bytes(3, "big")
            + bytes([ts >> 24])
            + stream_id
        )
        output.write(new_header + payload + struct.pack(">I", 11 + data_size))
        if tag_type == 9:
            output.flush()

    output.flush()

# ---------------------------------------------------------------------------
# إدارة وتشغيل البث المتعدد
# ---------------------------------------------------------------------------

def notify_telegram(message: str):
    if not BOT_TOKEN or not os.path.exists(CONFIG_PATH):
        return
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        chat_id = cfg.get("chat_id")
        if chat_id:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=5)
    except Exception as e:
        log(f"[NOTIFY_ERROR] {e}")

def load_streams() -> list:
    if os.path.exists(STREAMS_PATH):
        try:
            with open(STREAMS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def update_stream_live_status(tiktok_user: str, is_live: bool):
    streams = load_streams()
    updated = False
    for s in streams:
        if s.get("tiktok_user") == tiktok_user:
            if s.get("is_live") != is_live:
                s["is_live"] = is_live
                updated = True
    if updated:
        with open(STREAMS_PATH, "w", encoding="utf-8") as f:
            json.dump(streams, f, indent=2, ensure_ascii=False)

def check_is_live(tiktok_url: str) -> bool:
    try:
        cmd = ["streamlink", "--json", tiktok_url]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
        if res.returncode == 0 and res.stdout:
            data = json.loads(res.stdout)
            if "streams" in data and data["streams"]:
                return True
    except Exception:
        pass
    return False

def stream_worker(stream_data: dict, stop_event: threading.Event):
    user = stream_data["tiktok_user"]
    target = stream_data["target_url"]
    platform = stream_data["platform"]
    tiktok_url = f"https://www.tiktok.com/@{user}/live"

    log(f"[{user}] بدء مراقبة الحساب للبث إلى {platform} ({tiktok_url}) مع فلتر رفع البت ريت لـ {FILLER_KBPS}k")

    # مسار البث مع فلتر رفع البت ريت لـ 2000k بدون إعادة ترميز
    pipeline_cmd = (
        f"set -o pipefail; "
        f"streamlink --stdout --retry-streams 3 --retry-open 3 {shlex.quote(tiktok_url)} h264_hd,720p,best | "
        f"mbuffer -q -m 8M -P 2 | "
        f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err -fflags +genpts+discardcorrupt "
        f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
        f"-c:v copy -c:a aac -b:a 128k -ar 44100 -af aresample=async=1000 "
        f"-avoid_negative_ts make_zero -max_interleave_delta 0 -f flv pipe:1 | "
        f"python3 restreamer.py --filler {FILLER_KBPS} | "
        f"ffmpeg -hide_banner -nostdin -loglevel warning -stats -stats_period 15 -progress /tmp/prog_ch1.txt "
        f"-f flv -i pipe:0 -c copy -flvflags no_duration_filesize -f flv {shlex.quote(target)}"
    )

    last_notify_start = 0.0

    while not stop_event.is_set():
        if not check_is_live(tiktok_url):
            update_stream_live_status(user, False)
            stop_event.wait(5)
            continue

        now = time.time()
        if now - last_notify_start > 60:
            update_stream_live_status(user, True)
            notify_telegram(f"🔴 <b>بدأ البث المباشر لحساب @{user}!</b>\nجاري الإرسال المباشر فوراً إلى <b>{platform}</b> بت ريت {FILLER_KBPS}k 🚀")
            last_notify_start = now

        log(f"[{user}] إطلاق خط الأنابيب (سحب + فلتر بت ريت {FILLER_KBPS}k) إلى: {target}")

        err_file = f"/tmp/pipeline_err_{user}.log"
        with open(err_file, "w") as ef:
            proc = subprocess.Popen(
                pipeline_cmd,
                shell=True,
                executable="/bin/bash",
                stderr=ef,
                preexec_fn=os.setsid
            )
        ACTIVE_PROCESSES[user] = proc
        start_time = time.time()

        while not stop_event.is_set():
            ret = proc.poll()
            if ret is not None:
                duration = time.time() - start_time
                log(f"[{user}] انتهت العملية بعد {duration:.1f} ثانية كود: {ret}")
                break
            time.sleep(2)

        try:
            if proc.poll() is None:
                os.killpg(os.getpgid(proc.pid), 15)
        except Exception:
            pass
        ACTIVE_PROCESSES.pop(user, None)

        duration = time.time() - start_time
        if duration > 15:
            update_stream_live_status(user, False)
            notify_telegram(f"⏹️ <b>انتهى البث المباشر لحساب @{user}.</b>\nتم إيقاف النقل إلى {platform}.")
            stop_event.wait(5)
        else:
            log(f"[{user}] توقفت العملية سريعاً، انتظار 10 ثوان...")
            stop_event.wait(10)

def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--filler":
        run_filler(int(sys.argv[2]))
        return

    log(f"محرك إدارة البثوث المتعددة قيد التشغيل (الفيلر مفعل بمقدار: {FILLER_KBPS}k)...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    workers = {}

    while True:
        if os.path.exists("/tmp/reload_restreamer"):
            try:
                os.remove("/tmp/reload_restreamer")
            except OSError:
                pass
            log("🔄 وصل أمر تحديث البثوث من البوت، جاري التطبيق فوراً...")
            for u, w in list(workers.items()):
                w["stop"].set()
                if u in ACTIVE_PROCESSES:
                    try:
                        os.killpg(os.getpgid(ACTIVE_PROCESSES[u].pid), 15)
                    except Exception:
                        pass
            workers.clear()
            ACTIVE_PROCESSES.clear()
            time.sleep(1)

        configured = load_streams()
        active_users = {s["tiktok_user"]: s for s in configured if s.get("enabled", True)}

        for u in list(workers.keys()):
            if u not in active_users:
                log(f"إيقاف مراقبة الحساب: @{u}")
                workers[u]["stop"].set()
                if u in ACTIVE_PROCESSES:
                    try:
                        os.killpg(os.getpgid(ACTIVE_PROCESSES[u].pid), 15)
                    except Exception:
                        pass
                del workers[u]

        for u, sdata in active_users.items():
            if u not in workers or not workers[u]["thread"].is_alive():
                stop_evt = threading.Event()
                th = threading.Thread(target=stream_worker, args=(sdata, stop_evt), daemon=True)
                th.start()
                workers[u] = {"thread": th, "stop": stop_evt}

        time.sleep(2)

if __name__ == "__main__":
    main()
