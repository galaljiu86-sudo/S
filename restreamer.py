#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Channel Concurrent TikTok Restreamer.
Supports individual channel pausing, resume, deletion, and auto-recovery.
"""

import json
import os
import shlex
import subprocess
import threading
import time
import requests
import streamlink

DATA_DIR = os.path.abspath("data")
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

ACTIVE_WORKERS = {}  # tiktok_user -> dict(thread, stop_event)

def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

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

def build_pipeline(target_url: str) -> str:
    commands = [
        ["mbuffer", "-q", "-m", "8M", "-P", "2"],
        [
            "ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "warning",
            "-stats", "-stats_period", "15",
            "-progress", "/tmp/prog_ch1.txt",
            "-err_detect", "ignore_err",
            "-fflags", "+genpts+discardcorrupt",
            "-i", "pipe:0",
            "-map", "0:v:0", "-map", "0:a:0?",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "128k", "-ar", "44100",
            "-af", "aresample=async=1000",
            "-avoid_negative_ts", "make_zero",
            "-max_interleave_delta", "0",
            "-max_muxing_queue_size", "4096",
            "-flvflags", "no_duration_filesize",
            "-f", "flv", target_url,
        ]
    ]
    return "set -o pipefail; " + " | ".join(shlex.join(cmd) for cmd in commands)

def stream_worker(stream_data: dict, stop_event: threading.Event):
    user = stream_data["tiktok_user"]
    target = stream_data["target_url"]
    platform = stream_data["platform"]
    tiktok_url = f"https://www.tiktok.com/@{user}/live"

    session = streamlink.Streamlink()
    session.set_option("stream-timeout", 25)
    session.set_option("http-timeout", 20)

    log(f"[{user}] بدء مراقبة الحساب للبث إلى {platform}...")

    while not stop_event.is_set():
        try:
            streams = session.streams(tiktok_url)
        except Exception:
            streams = None

        if not streams:
            update_stream_live_status(user, False)
            stop_event.wait(4)
            continue

        selected = streams.get("h264_hd") or streams.get("best")
        if not selected:
            stop_event.wait(4)
            continue

        # الحساب لايف الآن!
        update_stream_live_status(user, True)
        notify_telegram(f"🔴 <b>بدأ البث المباشر لحساب @{user}!</b>\nجاري الإرسال المباشر إلى <b>{platform}</b> 🚀")
        log(f"[{user}] البث المباشر بدأ! جاري الضخ إلى: {target}")

        pipeline_cmd = build_pipeline(target)
        proc = subprocess.Popen(
            pipeline_cmd,
            shell=True,
            executable="/bin/bash",
            stdin=subprocess.PIPE,
            preexec_fn=os.setsid
        )

        try:
            with selected.open() as fd:
                while not stop_event.is_set():
                    chunk = fd.read(64 * 1024)
                    if not chunk:
                        break
                    proc.stdin.write(chunk)
                    proc.stdin.flush()
        except Exception as exc:
            log(f"[{user}] انقطع السحب: {exc}")
        finally:
            try:
                if proc.stdin:
                    proc.stdin.close()
                proc.wait(timeout=10)
            except Exception:
                subprocess.run(["pkill", "-P", str(proc.pid)], check=False)

        update_stream_live_status(user, False)
        notify_telegram(f"⏹️ <b>انتهى البث المباشر لحساب @{user}.</b>\nتم إيقاف النقل إلى {platform} بنجاح.")
        stop_event.wait(5)

def main():
    log("محرك إدارة البثوث المتعددة قيد التشغيل...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    while True:
        # فحص إشارة إعادة التحميل من البوت (عند إيقاف أو تشغيل أو حذف بث)
        if os.path.exists("/tmp/reload_restreamer"):
            try:
                os.remove("/tmp/reload_restreamer")
            except OSError:
                pass
            for u, w in list(ACTIVE_WORKERS.items()):
                w["stop"].set()
            ACTIVE_WORKERS.clear()
            time.sleep(1)

        configured_streams = load_streams()
        # نأخذ فقط البثوث المفعلة enabled
        current_active = {s["tiktok_user"]: s for s in configured_streams if s.get("enabled", True)}

        # إيقاف أي بث تم تعطيله مؤقتاً أو حذفه
        for u in list(ACTIVE_WORKERS.keys()):
            if u not in current_active:
                log(f"إيقاف/تعطيل مراقبة الحساب: @{u}")
                ACTIVE_WORKERS[u]["stop"].set()
                del ACTIVE_WORKERS[u]

        # تشغيل وركر لأي حساب مفعّل جديد
        for u, sdata in current_active.items():
            if u not in ACTIVE_WORKERS or not ACTIVE_WORKERS[u]["thread"].is_alive():
                stop_evt = threading.Event()
                th = threading.Thread(target=stream_worker, args=(sdata, stop_evt), daemon=True)
                th.start()
                ACTIVE_WORKERS[u] = {"thread": th, "stop": stop_evt}

        time.sleep(3)

if __name__ == "__main__":
    main()
