#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
High-Performance Multi-Channel TikTok Restreamer.
Features:
- Facebook & Instagram (Meta): Strict 2500k CBR, 30fps CFR (fps=30 filter), 2s keyframes, 128k AAC audio.
- YouTube Live: Dr. Shaker verified direct copy (0% CPU) OR ultra-stable 30fps/2500k for static/low-motion screens.
- Zero-Downtime Hot-Reload: Modifying, adding, or removing one stream NEVER interrupts running streams.
- Multi-Stage Verification: 45s smart check prevents false ends during TikTok battles/guest co-hosts.
- Smart Crash Backoff: Prevents network choking & rate limits if a single stream experiences repeated errors.
"""

import json
import os
import shlex
import subprocess
import sys
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
    try:
        cmd = ["streamlink", "--no-config", "--json", tiktok_url]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if res.returncode == 0 and res.stdout:
            data = json.loads(res.stdout)
            if "streams" in data and data["streams"]:
                return True
    except Exception:
        pass
    return False

def verify_live_ended(tiktok_url: str, stop_event: threading.Event) -> bool:
    """
    تحقق ذكي متعدد المراحل (45 ثانية) للتأكد من أن البث انتهى فعلياً
    وليس مجرد تبديل قست أو جولة باتل أو تأخر لحظي في سيرفرات تيك توك.
    """
    for attempt in range(5):
        if stop_event.is_set():
            return True
        stop_event.wait(8)
        if check_is_live(tiktok_url):
            return False
    return True

def build_pipeline_command(tiktok_url: str, target: str, platform: str, layout: str) -> str:
    # نقاء H.264 التام بدون HEVC إطلاقاً
    H264_QUALITY = "h264_hd,h264_sd,720p,h264_origin"

    # منصات Meta (Facebook & Instagram) أو خيارات التشفير العالي/الشاشات الثابتة:
    # تتطلب بث CFR ثابت 30fps وبت ريت 2500k ثابت وصوت 128k لتفادي هبوط الـ 169k أو الـ 508k
    needs_transcode = (platform in ["Facebook", "Instagram"]) or (layout in ["landscape_to_portrait_pad", "portrait_stable", "landscape_raw"])

    if needs_transcode:
        if layout == "landscape_to_portrait_pad":
            vf_filter = (
                "fps=30,"
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)*0.38:black,"
                "format=yuv420p"
            )
        elif layout == "landscape_raw" and platform == "Facebook":
            vf_filter = (
                "fps=30,"
                "scale=w=1280:h=720:force_original_aspect_ratio=decrease,"
                "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p"
            )
        else:
            vf_filter = (
                "fps=30,"
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p"
            )

        # متوافق مع كافة نسخ FFmpeg عبر -vsync 1 و -r 30 (تجنب -fps_mode لعدم دعمه في ffmpeg 4.4)
        return (
            f"set -o pipefail; "
            f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
            f"mbuffer -q -m 32M -P 2 | "
            f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
            f"-fflags +genpts+discardcorrupt "
            f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
            f"-vf '{vf_filter}' "
            f"-c:v libx264 -preset ultrafast -tune zerolatency "
            f"-b:v 2500k -minrate 2000k -maxrate 3000k -bufsize 5000k "
            f"-r 30 -vsync 1 -g 60 -keyint_min 60 -sc_threshold 0 "
            f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
            f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
            f"-max_muxing_queue_size 2048 "
            f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
        )
    else:
        # يوتيوب والروابط المخصصة في النمط الطولي الأصلي (كود دكتور شاكر المباشر 0% معالج):
        return (
            f"set -o pipefail; "
            f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
            f"mbuffer -q -m 32M -P 2 | "
            f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
            f"-fflags +genpts+discardcorrupt "
            f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
            f"-c:v copy "
            f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
            f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
            f"-max_muxing_queue_size 2048 "
            f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
        )

def stream_worker(stream_data: dict, stop_event: threading.Event):
    user = stream_data["tiktok_user"]
    target = stream_data["target_url"]
    platform = stream_data.get("platform", "YouTube")
    layout = stream_data.get("layout", "portrait_copy")
    tiktok_url = f"https://www.tiktok.com/@{user}/live"

    layout_desc = {
        "portrait_copy": "📱 طولي أصلي (نسخ مباشر)",
        "portrait_stable": "⚡ طولي فائق الثبات (30fps & 2500k)",
        "landscape_to_portrait_pad": "🎬 طولي بشاشة سوداء وتكبير 80%",
        "landscape_raw": "🖥️ عرضي كامل كما هو"
    }.get(layout, "📱 طولي")

    log(f"[{user}] بدء مراقبة الحساب للبث إلى {platform} ({tiktok_url}) - النمط: {layout_desc}")

    is_currently_live = False
    consecutive_fast_crashes = 0

    while not stop_event.is_set():
        is_live = check_is_live(tiktok_url)

        if not is_live:
            if is_currently_live:
                log(f"[{user}] فحص ذكي (45 ثانية) للتأكد من انتهاء البث...")
                really_ended = verify_live_ended(tiktok_url, stop_event)
                if really_ended:
                    is_currently_live = False
                    update_stream_live_status(user, False)
                    notify_telegram(f"⏹️ <b>انتهى البث المباشر لحساب @{user}.</b>\nتم إيقاف النقل إلى {platform}.")
                else:
                    log(f"[{user}] البث ما زال حياً (كان مجرد تبديل قست)، استمرار البث فوراً!")
                    continue
            update_stream_live_status(user, False)
            stop_event.wait(4)
            continue

        if not is_currently_live:
            is_currently_live = True
            update_stream_live_status(user, True)
            notify_telegram(
                f"🔴 <b>بدأ البث المباشر لحساب @{user}!</b>\n"
                f"• <b>المنصة:</b> {platform}\n"
                f"• <b>النمط:</b> {layout_desc}\n"
                f"🚀 جاري الإرسال المباشر فوراً بأعلى درجات الاستقرار!"
            )

        pipeline_cmd = build_pipeline_command(tiktok_url, target, platform, layout)
        log(f"[{user}] إطلاق خط الأنابيب إلى: {target}")

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
                log(f"[{user}] خط الأنابيب توقف بعد {duration:.1f} ثانية كود: {ret}")
                break
            time.sleep(2)

        try:
            if proc.poll() is None:
                os.killpg(os.getpgid(proc.pid), 15)
        except Exception:
            pass
        ACTIVE_PROCESSES.pop(user, None)

        duration = time.time() - start_time
        if duration < 25:
            consecutive_fast_crashes += 1
            backoff = min(20, 3 * consecutive_fast_crashes)
            log(f"[{user}] خط الأنابيب توقف سريعاً ({duration:.1f} ث). تهدئة {backoff} ثوانٍ لتفادي خنق باقي البثوث...")
            stop_event.wait(backoff)
        else:
            consecutive_fast_crashes = 0
            stop_event.wait(1)

def main():
    log("محرك إدارة البثوث قيد التشغيل (ثبات فائق، دعم Meta 2500k، وتحديث ذكي صامت بدون مقاطعة)...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    workers = {}  # user -> {"thread": th, "stop": stop_evt, "data": dict}

    while True:
        if os.path.exists("/tmp/reload_restreamer"):
            try:
                os.remove("/tmp/reload_restreamer")
            except OSError:
                pass

        configured = load_streams()
        active_users = {s["tiktok_user"]: s for s in configured if s.get("enabled", True)}

        # 1. إيقاف البثوث التي تم حذفها أو تعطيلها فقط
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

        # 2. فحص البثوث الحالية: هل تغيرت إعدادات أي بث (مثل المفتاح أو النمط أو المنصة)؟
        for u, sdata in active_users.items():
            if u in workers:
                old_data = workers[u].get("data", {})
                if (old_data.get("target_url") != sdata.get("target_url") or
                    old_data.get("layout") != sdata.get("layout") or
                    old_data.get("platform") != sdata.get("platform")):
                    log(f"🔄 تم رصد تحديث إعدادات @{u}، جاري إعادة تشغيل هذا البث فقط دون لمس البقية...")
                    workers[u]["stop"].set()
                    if u in ACTIVE_PROCESSES:
                        try:
                            os.killpg(os.getpgid(ACTIVE_PROCESSES[u].pid), 15)
                        except Exception:
                            pass
                    del workers[u]

        # 3. تشغيل أي بث جديد أو محدّث فقط (البثوث الشغالة بدون تغيير لا يتم لمسها إطلاقاً!)
        for u, sdata in active_users.items():
            if u not in workers or not workers[u]["thread"].is_alive():
                stop_evt = threading.Event()
                th = threading.Thread(target=stream_worker, args=(sdata, stop_evt), daemon=True)
                th.start()
                workers[u] = {"thread": th, "stop": stop_evt, "data": dict(sdata)}

        time.sleep(2)

if __name__ == "__main__":
    main()
