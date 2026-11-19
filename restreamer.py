#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
High-Performance Multi-Channel TikTok Restreamer.
Direct CLI Pipeline: streamlink -> mbuffer -> ffmpeg -> YouTube/Facebook/Instagram.
Zero latency, zero Python memory bloat, instant launch.
"""

import json
import os
import shlex
import subprocess
import threading
import time
import requests

DATA_DIR = os.path.abspath("data")
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

ACTIVE_PROCESSES = {}  # tiktok_user -> subprocess.Popen

def log(msg: str):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)

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
    """فحص فوري عبر streamlink CLI مباشرة"""
    try:
        cmd = ["streamlink", "--json", tiktok_url]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
        if res.returncode == 0 and res.stdout:
            data = json.loads(res.stdout)
            if "streams" in data and data["streams"]:
                return True
    except Exception as exc:
        log(f"Check error: {exc}")
    return False

def stream_worker(stream_data: dict, stop_event: threading.Event):
    user = stream_data["tiktok_user"]
    target = stream_data["target_url"]
    platform = stream_data["platform"]
    tiktok_url = f"https://www.tiktok.com/@{user}/live"

    log(f"[{user}] بدء مراقبة الحساب للبث إلى {platform} ({tiktok_url})")

    # أمر البث المتكامل المباشر (أعلى أداء وسرعة قصوى)
    pipeline_cmd = (
        f"set -o pipefail; "
        f"streamlink --stdout --retry-streams 3 --retry-max 0 {shlex.quote(tiktok_url)} h264_hd,best | "
        f"mbuffer -q -m 8M -P 2 | "
        f"ffmpeg -hide_banner -nostdin -loglevel warning -stats -stats_period 15 "
        f"-progress /tmp/prog_ch1.txt -err_detect ignore_err -fflags +genpts+discardcorrupt "
        f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
        f"-c:v copy -c:a aac -b:a 128k -ar 44100 -af aresample=async=1000 "
        f"-avoid_negative_ts make_zero -max_interleave_delta 0 -max_muxing_queue_size 4096 "
        f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
    )

    while not stop_event.is_set():
        if not check_is_live(tiktok_url):
            update_stream_live_status(user, False)
            stop_event.wait(4)
            continue

        # الحساب فاتح لايف الآن!
        update_stream_live_status(user, True)
        notify_telegram(f"🔴 <b>بدأ البث المباشر لحساب @{user}!</b>\nجاري الإرسال المباشر فوراً إلى <b>{platform}</b> 🚀")
        log(f"[{user}] البث المباشر بدأ! جاري إطلاق خط الأنابيب إلى: {target}")

        proc = subprocess.Popen(
            pipeline_cmd,
            shell=True,
            executable="/bin/bash",
            preexec_fn=os.setsid
        )
        ACTIVE_PROCESSES[user] = proc

        # انتظار انتهاء البث
        while not stop_event.is_set():
            ret = proc.poll()
            if ret is not None:
                log(f"[{user}] انتهى مسار البث كود: {ret}")
                break
            time.sleep(2)

        # تنظيف العملية
        try:
            if proc.poll() is None:
                os.killpg(os.getpgid(proc.pid), 15)
        except Exception:
            pass
        ACTIVE_PROCESSES.pop(user, None)

        update_stream_live_status(user, False)
        notify_telegram(f"⏹️ <b>انتهى البث المباشر لحساب @{user}.</b>\nتم إيقاف النقل إلى {platform}.")
        stop_event.wait(4)

def main():
    log("محرك إدارة البثوث المتعددة المباشر قيد التشغيل...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    workers = {}  # user -> dict(thread, stop)

    while True:
        # فحص إشارة إعادة التحميل
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

        # إيقاف المعطل أو المحذوف
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

        # تشغيل أي بث جديد
        for u, sdata in active_users.items():
            if u not in workers or not workers[u]["thread"].is_alive():
                stop_evt = threading.Event()
                th = threading.Thread(target=stream_worker, args=(sdata, stop_evt), daemon=True)
                th.start()
                workers[u] = {"thread": th, "stop": stop_evt}

        time.sleep(2)

if __name__ == "__main__":
    main()
