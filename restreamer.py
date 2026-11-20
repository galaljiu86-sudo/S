#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
High-Performance Multi-Channel TikTok Restreamer.
Features:
- YouTube: Pure H.264 (Dr. Shaker standards), 48MB buffer, filler 2000k/3500k, aresample sync.
- Facebook & Instagram: Strict Meta Live API Compliance (2500k CBR, 30fps CFR, Keyframe GOP=2s, AAC 128k, 720x1280).
- Graceful Hot-Reload: Modifying/adding/deleting a stream NEVER interrupts running streams on ANY platform!
- Multi-check 45s verification to eliminate false stream endings during TikTok co-host/battle transitions.
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

FILLER_KBPS = int(os.environ.get("FILLER_KBPS", 2000))
MAX_CAP_KBPS = 3500  # سقف أمان صارم لليوتيوب

ACTIVE_PROCESSES = {}  # tiktok_user -> subprocess.Popen

def log(msg: str):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)

# ---------------------------------------------------------------------------
# محرك حشو البت ريت (H.264 Filler NAL Padding) لليوتيوب
# ---------------------------------------------------------------------------

def run_filler(target_kbps: int, max_cap_kbps: int = 3500) -> None:
    output = sys.stdout.buffer
    input_stream = sys.stdin.buffer
    bytes_per_ms = target_kbps * 1000.0 / 8.0 / 1000.0
    MAX_BYTES_PER_FRAME = int((max_cap_kbps * 1000.0 / 8.0) / 30.0)

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
                    if 0 <= delta <= 100:
                        carry_bytes += delta * bytes_per_ms
                    else:
                        carry_bytes = 0.0
                last_video_ts = timestamp

                needed = int(carry_bytes) - (data_size + 15) - audio_bytes
                audio_bytes = 0

                carry_bytes = min(carry_bytes, bytes_per_ms * 33.0)
                max_filler_allowed = max(0, MAX_BYTES_PER_FRAME - data_size)

                if needed > 16 and max_filler_allowed > 16:
                    filler_len = min(needed - 8, max_filler_allowed)
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
# إدارة البث المتعدد والإشعارات الذكية
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
    تحقق صارم متعدد المراحل (45 ثانية) للتأكد من أن البث انتهى فعلياً،
    وليس مجرد تبديل قست (Co-host switch) أو بطء لحظي في سيرفرات تيك توك.
    """
    for attempt in range(5):
        if stop_event.is_set():
            return True
        stop_event.wait(8)
        if check_is_live(tiktok_url):
            return False  # البث لا زال حياً!
    return True

def build_pipeline_command(tiktok_url: str, target: str, platform: str, layout: str) -> str:
    # جودات H.264 الصريحة فقط (حظر HEVC تماماً)
    H264_QUALITY = "h264_hd,h264_sd,720p,h264_origin"

    if platform in ["Facebook", "Instagram"]:
        # منصات Meta (Facebook & Instagram):
        # بت ريت 2500k ثابت | إطارات 30fps CFR | فاصل Keyframe كل 2 ثانية (GOP=60) | صوت AAC 128k
        if layout == "landscape_to_portrait_pad":
            vf_filter = (
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)*0.38:black,"
                "format=yuv420p"
            )
        elif layout == "landscape_raw" and platform == "Facebook":
            vf_filter = (
                "scale=w=1280:h=720:force_original_aspect_ratio=decrease,"
                "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p"
            )
        else:
            vf_filter = (
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p"
            )

        return (
            f"set -o pipefail; "
            f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
            f"mbuffer -q -m 32M -P 2 | "
            f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
            f"-fflags +genpts+discardcorrupt "
            f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
            f"-vf '{vf_filter}' "
            f"-c:v libx264 -preset ultrafast -tune zerolatency "
            f"-b:v 2500k -minrate 1800k -maxrate 3200k -bufsize 5000k "
            f"-r 30 -fps_mode cfr -g 60 -keyint_min 60 -sc_threshold 0 "
            f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
            f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
            f"-max_muxing_queue_size 2048 "
            f"-rtmp_live live -rtmp_buffer 1000 "
            f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
        )

    else:
        # منصة YouTube والروابط المخصصة:
        if layout == "landscape_to_portrait_pad":
            vf_filter = (
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)*0.38:black,"
                "format=yuv420p"
            )
            return (
                f"set -o pipefail; "
                f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
                f"mbuffer -q -m 32M -P 2 | "
                f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
                f"-fflags +genpts+discardcorrupt "
                f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
                f"-vf '{vf_filter}' "
                f"-c:v libx264 -preset ultrafast -tune zerolatency "
                f"-b:v 2500k -minrate 1800k -maxrate 3500k -bufsize 5000k "
                f"-r 30 -fps_mode cfr -g 60 -keyint_min 60 -sc_threshold 0 "
                f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
                f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
                f"-max_muxing_queue_size 2048 "
                f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )
        else:
            # كود دكتور شاكر المعتمد والمثبت 100%:
            return (
                f"set -o pipefail; "
                f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
                f"mbuffer -q -m 48M -P 2 | "
                f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
                f"-fflags +genpts+discardcorrupt "
                f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
                f"-c:v copy "
                f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
                f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
                f"-max_muxing_queue_size 2048 "
                f"-flvflags no_duration_filesize -f flv pipe:1 | "
                f"python3 restreamer.py --filler {FILLER_KBPS} | "
                f"ffmpeg -hide_banner -nostdin -loglevel warning "
                f"-f flv -i pipe:0 -c copy -flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )

def stream_worker(stream_data: dict, stop_event: threading.Event):
    user = stream_data["tiktok_user"]
    target = stream_data["target_url"]
    platform = stream_data.get("platform", "YouTube")
    layout = stream_data.get("layout", "portrait_copy")
    tiktok_url = f"https://www.tiktok.com/@{user}/live"

    layout_desc = {
        "portrait_copy": "📱 طولي أصلي (نسخ كودك شاكر)",
        "landscape_to_portrait_pad": "🎬 طولي بشاشة سوداء وتكبير 80%",
        "landscape_raw": "🖥️ عرضي كامل كما هو"
    }.get(layout, "📱 طولي أصلي")

    log(f"[{user}] بدء مراقبة الحساب للبث إلى {platform} ({tiktok_url}) - النمط: {layout_desc}")

    is_currently_live = False

    while not stop_event.is_set():
        is_live = check_is_live(tiktok_url)

        if not is_live:
            if is_currently_live:
                log(f"[{user}] إشارة أولية لتوقف البث، فحص صارم لـ 45 ثانية لتأكيد انتهاء البث...")
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
                f"🚀 جاري الإرسال المباشر فوراً (معايير دكتور شاكر المعتمدة)!"
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

        # إعادة تشغيل فورية (1 ثانية فقط) زي كود دكتور شاكر لو البث ما زال حياً:
        stop_event.wait(1)

def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--filler":
        run_filler(int(sys.argv[2]), MAX_CAP_KBPS)
        return

    log(f"محرك إدارة البثوث قيد التشغيل (التحديث الصامت الذكي نشط، معايير دكتور شاكر، ثبات Meta)...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    workers = {}

    while True:
        # نظام التحديث الصامت الذكي (Graceful Hot-Reload):
        # يمنع إغلاق أي بث شغال على الإطلاق عند إضافة/تعديل أي بث آخر!
        if os.path.exists("/tmp/reload_restreamer"):
            try:
                os.remove("/tmp/reload_restreamer")
            except OSError:
                pass
            log("🔄 جاري مزامنة البثوث صامتاً بدون مقاطعة أي بث يعمل حالياً...")

            configured = load_streams()
            active_users = {s["tiktok_user"]: s for s in configured if s.get("enabled", True)}

            # 1. إيقاف البثوث التي تم حذفها أو تعطيلها فقط
            for u in list(workers.keys()):
                if u not in active_users:
                    log(f"🛑 تم إيقاف أو حذف الحساب @{u}، جاري إيقاف مراقبته فقط.")
                    workers[u]["stop"].set()
                    if u in ACTIVE_PROCESSES:
                        try:
                            os.killpg(os.getpgid(ACTIVE_PROCESSES[u].pid), 15)
                        except Exception:
                            pass
                    del workers[u]

            # 2. فحص البثوث التي تم تعديل إعداداتها فقط (مثل تغيير المفتاح أو المقاس)
            for u, sdata in active_users.items():
                if u in workers:
                    old_cfg = workers[u].get("data", {})
                    if (old_cfg.get("target_url") != sdata.get("target_url") or
                        old_cfg.get("platform") != sdata.get("platform") or
                        old_cfg.get("layout") != sdata.get("layout")):
                        log(f"🔄 تم تحديث إعدادات @{u}، جاري إعادة تشغيل هذا البث فقط...")
                        workers[u]["stop"].set()
                        if u in ACTIVE_PROCESSES:
                            try:
                                os.killpg(os.getpgid(ACTIVE_PROCESSES[u].pid), 15)
                            except Exception:
                                pass
                        del workers[u]

            # 3. تشغيل البثوث الجديدة أو المحدثة فقط (البثوث الشغالة التانية تفضل مستمرة تماماً)
            for u, sdata in active_users.items():
                if u not in workers or not workers[u]["thread"].is_alive():
                    stop_evt = threading.Event()
                    th = threading.Thread(target=stream_worker, args=(sdata, stop_evt), daemon=True)
                    th.start()
                    workers[u] = {"thread": th, "stop": stop_evt, "data": dict(sdata)}

        else:
            # الفحص الدوري العادي في الخلفية
            configured = load_streams()
            active_users = {s["tiktok_user"]: s for s in configured if s.get("enabled", True)}

            for u in list(workers.keys()):
                if u not in active_users:
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
                    workers[u] = {"thread": th, "stop": stop_evt, "data": dict(sdata)}

        time.sleep(2)

if __name__ == "__main__":
    main()
