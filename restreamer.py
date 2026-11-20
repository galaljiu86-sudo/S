#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
High-Performance Multi-Channel TikTok Restreamer (Hardened Enterprise Edition).
Key Architecture & Hardening:
- Full Stream Isolation: Process Groups (os.setsid), SIGTERM -> SIGKILL escalation with proc.wait().
- YouTube Live: Exact Dr. Shaker copy pipeline (pure H.264 -c:v copy, 48M buffer, H.264 NAL filler 2000k-3500k).
- Meta (Facebook & Instagram): Strict 2500k CBR (nal-hrd=cbr), 30fps CFR, 128k AAC audio, 32M buffer with instant startup.
- Dynamic Live Zoom System: Adjust zoom from 50% up to 200% (in 5% steps) ON THE FLY while stream is live!
- State Decoupling: is_live status written to status.json atomically to eliminate race conditions with bot.py.
- Staggered Rate-Limiting: TIKTOK_CHECK_LOCK + 15s offline interval to protect IP from TikTok bans.
- Zero Cross-Stream Interference: Updating, zooming, or stopping one stream NEVER touches or interrupts others.
"""

import json
import os
import shlex
import signal
import struct
import subprocess
import sys
import threading
import time
import requests

DATA_DIR = os.path.abspath("data")
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
STATUS_PATH = os.path.join(DATA_DIR, "status.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

FILLER_KBPS = int(os.environ.get("FILLER_KBPS", 2000))
MAX_CAP_KBPS = 3500

TIKTOK_CHECK_LOCK = threading.Lock()
TIKTOK_START_LOCK = threading.Lock()
LAST_TIKTOK_CHECK_TIME = 0.0

LAST_VALID_STREAMS = []

def log(msg: str):
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)

# ---------------------------------------------------------------------------
# إدارة العمليات وإغلاقها بأمان تام (منع Zombies والعمليات اليتيمة)
# ---------------------------------------------------------------------------

def kill_process_group(proc: subprocess.Popen):
    """
    إغلاق آمن وحاسم لمجموعة العمليات (streamlink + mbuffer + ffmpeg):
    إرسال SIGTERM أولاً، انتظار ثانيتين، ثم تصعيد لـ SIGKILL مع proc.wait().
    """
    if proc is None:
        return
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
        proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        try:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGKILL)
            proc.wait(timeout=2.0)
        except Exception:
            pass
    except Exception:
        pass

# ---------------------------------------------------------------------------
# محرك حشو البت ريت الأصلي الخاص بكود دكتور شاكر (لليوتيوب)
# ---------------------------------------------------------------------------

def run_filler(target_kbps: int, max_cap_kbps: int = 3500) -> None:
    output = sys.stdout.buffer
    input_stream = sys.stdin.buffer
    bytes_per_ms = target_kbps * 1000.0 / 8.0 / 1000.0
    MAX_BYTES_PER_FRAME = int((max_cap_kbps * 1000.0 / 8.0) / 30.0)

    try:
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
                        if 0 <= delta <= 500:
                            carry_bytes += delta * bytes_per_ms
                        else:
                            carry_bytes += 33.0 * bytes_per_ms
                    else:
                        carry_bytes += 33.0 * bytes_per_ms
                    last_video_ts = timestamp

                    needed = int(carry_bytes) - (data_size + 15) - audio_bytes
                    audio_bytes = 0

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
    except (BrokenPipeError, IOError):
        pass

# ---------------------------------------------------------------------------
# إدارة الحالة والإشعارات (فصل الحالة اللحظية لمنع Race Conditions)
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

def load_streams_safe() -> list:
    """
    قراءة آمنة لملف streams.json مع الاحتفاظ بآخر حالة صالحة عند تعذر القراءة.
    """
    global LAST_VALID_STREAMS
    if os.path.exists(STREAMS_PATH):
        try:
            with open(STREAMS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for idx, s in enumerate(data):
                        if not s.get("id"):
                            s["id"] = f"{s.get('tiktok_user')}_{s.get('platform', 'yt')}_{idx}"
                    LAST_VALID_STREAMS = data
                    return data
        except Exception:
            pass
    return list(LAST_VALID_STREAMS)

def update_stream_live_status(stream_id: str, tiktok_user: str, is_live: bool):
    """
    كتابة ذرية مستقلة في data/status.json لحماية streams.json من التلف.
    """
    try:
        current = {}
        if os.path.exists(STATUS_PATH):
            try:
                with open(STATUS_PATH, "r", encoding="utf-8") as f:
                    current = json.load(f)
            except Exception:
                current = {}

        current[stream_id] = {"is_live": is_live, "user": tiktok_user, "ts": time.time()}
        current[tiktok_user] = {"is_live": is_live, "ts": time.time()}

        tmp_path = STATUS_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, STATUS_PATH)
    except Exception as e:
        log(f"[STATUS_ERROR] {e}")

# ---------------------------------------------------------------------------
# فحص حالة تيك توك مع قفل وتوزيع زمني لمنع حظر الـ IP
# ---------------------------------------------------------------------------

def check_is_live(tiktok_url: str) -> bool:
    global LAST_TIKTOK_CHECK_TIME
    with TIKTOK_CHECK_LOCK:
        now = time.time()
        elapsed = now - LAST_TIKTOK_CHECK_TIME
        if elapsed < 2.5:
            time.sleep(2.5 - elapsed)
        LAST_TIKTOK_CHECK_TIME = time.time()

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
    for attempt in range(4):
        if stop_event.is_set():
            return True
        stop_event.wait(10)
        if check_is_live(tiktok_url):
            return False
    return True

# ---------------------------------------------------------------------------
# فلتر التكبير الديناميكي الهندسي (من 50% حتى 200%)
# ---------------------------------------------------------------------------

def get_zoom_filter(zoom_pct: int = 80, pos_y: float = 0.38) -> str:
    """
    توليد فلتر FFmpeg ديناميكي للتكبير والشاشة السوداء متوافق مع FFmpeg 4.4:
    - أبعاد زوجية مضمونة 100% لمنع كود 255.
    - ثبات الموضع العمودي pos_y في الحالتين لمنع القفزات المفاجئة للصورة.
    - إحداثيات قص (crop) غير سالبة دائماً: max(0\,...).
    """
    scale_w = int(720 * (zoom_pct / 100.0))
    if scale_w % 2 != 0:
        scale_w += 1
    scale_w = max(360, min(1440, scale_w))

    if zoom_pct <= 100:
        return (
            f"fps=30,"
            f"scale=w={scale_w}:h=-2:flags=bicubic,"
            f"pad=720:1280:(ow-iw)/2:(oh-ih)*{pos_y}:black,"
            f"format=yuv420p"
        )
    else:
        return (
            f"fps=30,"
            f"scale=w={scale_w}:h=-2:flags=bicubic,"
            f"crop=w=min(iw\\,720):h=min(ih\\,1280):max(0\\,(iw-720)/2):max(0\\,(ih-1280)*{pos_y}),"
            f"pad=720:1280:(ow-iw)/2:(oh-ih)*{pos_y}:black,"
            f"format=yuv420p"
        )

# ---------------------------------------------------------------------------
# بناء أوامر البث (يوتيوب زي دكتور شاكر | فيسبوك وإنستا 2500k ببافر عريض)
# ---------------------------------------------------------------------------

def build_pipeline_command(tiktok_url: str, target: str, platform: str, layout: str, zoom_pct: int = 80) -> str:
    H264_QUALITY = "h264_hd,h264_sd,720p,h264_origin"

    if platform in ["Facebook", "Instagram"]:
        # منصات Meta (Facebook & Instagram):
        # تشفير 2500k CBR صارم مع حشو داخلي nal-hrd=cbr وإطارات 30fps ثابتة
        # بافر 32M -P 2 مع probesize 10M لسرعة البدء الفوري دون انتظار 85 ثانية
        if layout == "landscape_raw":
            vf_filter = (
                "fps=30,"
                "scale=w=1280:h=720:force_original_aspect_ratio=decrease,"
                "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p"
            )
        elif layout == "landscape_to_portrait_pad":
            vf_filter = get_zoom_filter(zoom_pct)
        else:
            # طولي أصلي
            vf_filter = (
                "fps=30,"
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p"
            )

        return (
            f"set -o pipefail; "
            f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
            f"mbuffer -q -m 32M -P 2 | "
            f"nice -n 10 ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
            f"-analyzeduration 10M -probesize 10M "
            f"-fflags +genpts+discardcorrupt "
            f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
            f"-vf '{vf_filter}' "
            f"-c:v libx264 -preset superfast -threads 2 "
            f"-b:v 2500k -minrate 2500k -maxrate 2500k -bufsize 8000k "
            f"-x264-params 'nal-hrd=cbr:force-cfr=1:scenecut=0' "
            f"-g 60 -keyint_min 60 -sc_threshold 0 "
            f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
            f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
            f"-max_muxing_queue_size 4096 "
            f"-rw_timeout 15000000 "
            f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
        )

    else:
        # منصة YouTube والروابط المخصصة:
        if layout == "landscape_to_portrait_pad":
            vf_filter = get_zoom_filter(zoom_pct)
            return (
                f"set -o pipefail; "
                f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
                f"mbuffer -q -m 48M -P 2 | "
                f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
                f"-analyzeduration 10M -probesize 10M "
                f"-fflags +genpts+discardcorrupt "
                f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
                f"-vf '{vf_filter}' "
                f"-c:v libx264 -preset superfast -threads 2 "
                f"-b:v 2500k -minrate 2000k -maxrate 3500k -bufsize 5000k "
                f"-r 30 -vsync 1 -g 60 -keyint_min 60 -sc_threshold 0 "
                f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
                f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
                f"-max_muxing_queue_size 2048 "
                f"-rw_timeout 15000000 "
                f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )
        elif layout == "landscape_raw":
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
                f"-rw_timeout 15000000 "
                f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )
        else:
            # كود دكتور شاكر المعتمد والمثبت للطولي الأصلي (نسخ مباشر 0% معالج مع فلتر الحشو):
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
                f"python3 -u restreamer.py --filler {FILLER_KBPS} | "
                f"ffmpeg -hide_banner -nostdin -loglevel warning "
                f"-f flv -i pipe:0 -c copy -rw_timeout 15000000 -flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )

# ---------------------------------------------------------------------------
# تشغيل كل بث في مسار معزول تماماً
# ---------------------------------------------------------------------------

def stream_worker(stream_data: dict, stop_event: threading.Event, worker_ref: dict):
    user = stream_data["tiktok_user"]
    stream_id = stream_data.get("id") or f"{user}_{stream_data.get('platform')}"
    target = stream_data["target_url"]
    platform = stream_data.get("platform", "YouTube")
    layout = stream_data.get("layout", "portrait_copy")
    zoom_pct = int(stream_data.get("zoom_pct", 80))
    tiktok_url = f"https://www.tiktok.com/@{user}/live"

    layout_desc = {
        "portrait_copy": "📱 طولي أصلي (نسخ مباشر كود شاكر)",
        "landscape_to_portrait_pad": f"🎬 طولي بشاشة سوداء وتكبير ({zoom_pct}%)",
        "landscape_raw": "🖥️ عرضي كامل كما هو"
    }.get(layout, "📱 طولي")

    log(f"[{user}|{stream_id}] بدء مراقبة الحساب للبث إلى {platform} ({tiktok_url}) - النمط: {layout_desc}")

    is_currently_live = False
    consecutive_fast_crashes = 0

    while not stop_event.is_set():
        is_live = check_is_live(tiktok_url)

        if not is_live:
            if is_currently_live:
                log(f"[{user}|{stream_id}] فحص ذكي للتأكد من انتهاء البث...")
                really_ended = verify_live_ended(tiktok_url, stop_event)
                if really_ended:
                    is_currently_live = False
                    update_stream_live_status(stream_id, user, False)
                    notify_telegram(f"⏹️ <b>انتهى البث المباشر لحساب @{user}.</b>\nتم إيقاف النقل إلى {platform}.")
                else:
                    log(f"[{user}|{stream_id}] البث ما زال حياً، استمرار البث فوراً!")
                    continue

            update_stream_live_status(stream_id, user, False)
            # فاصل زمني 15 ثانية لحماية حسابات الأوفلاين من حظر تيك توك
            stop_event.wait(15)
            continue

        if not is_currently_live:
            is_currently_live = True
            update_stream_live_status(stream_id, user, True)
            notify_telegram(
                f"🔴 <b>بدأ البث المباشر لحساب @{user}!</b>\n"
                f"• <b>المنصة:</b> {platform}\n"
                f"• <b>النمط:</b> {layout_desc}\n"
                f"🚀 جاري الإرسال المباشر فوراً!"
            )

        with TIKTOK_START_LOCK:
            time.sleep(3.0)

        pipeline_cmd = build_pipeline_command(tiktok_url, target, platform, layout, zoom_pct)
        log(f"[{user}|{stream_id}] إطلاق خط الأنابيب إلى: {target}")

        err_file = f"/tmp/pipeline_err_{stream_id}.log"
        with open(err_file, "w") as ef:
            proc = subprocess.Popen(
                ["/bin/bash", "-o", "pipefail", "-c", pipeline_cmd],
                stderr=ef,
                preexec_fn=os.setsid
            )
        worker_ref["proc"] = proc
        start_time = time.time()

        while not stop_event.is_set():
            ret = proc.poll()
            if ret is not None:
                duration = time.time() - start_time
                log(f"[{user}|{stream_id}] خط الأنابيب توقف بعد {duration:.1f} ثانية كود: {ret}")
                break
            time.sleep(2)

        kill_process_group(proc)
        worker_ref["proc"] = None

        duration = time.time() - start_time
        if duration < 25:
            consecutive_fast_crashes += 1
            backoff = min(20, 4 * consecutive_fast_crashes)
            log(f"[{user}|{stream_id}] خط الأنابيب توقف سريعاً ({duration:.1f} ث). تهدئة {backoff} ثوانٍ لعزل القناة وحماية باقي البثوث...")
            stop_event.wait(backoff)
        else:
            consecutive_fast_crashes = 0
            stop_event.wait(2)

def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--filler":
        run_filler(int(sys.argv[2]), MAX_CAP_KBPS)
        return

    log("محرك إدارة البثوث قيد التشغيل (إصدار الحماية القصوى | يوتيوب شاكر | ميتا 2500k | تحكم ديناميكي بالحجم)...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    workers = {}  # stream_id -> {"thread": th, "stop": stop_evt, "data": dict, "proc": Popen}

    def sig_handler(sig, frame):
        log("تم استقبال إشارة إيقاف النظام (SIGTERM/SIGINT)، جاري تنظيف وإغلاق كل العمليات...")
        for sid, w in list(workers.items()):
            w["stop"].set()
            if w.get("proc"):
                kill_process_group(w["proc"])
        sys.exit(0)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    while True:
        restart_all = False
        if os.path.exists("/tmp/restart_all_streams"):
            try:
                os.remove("/tmp/restart_all_streams")
                restart_all = True
                log("🔄 تم استقبال أمر إعادة تشغيل آمن لكل البثوث من البوت...")
            except OSError:
                pass

        if os.path.exists("/tmp/reload_restreamer"):
            try:
                os.remove("/tmp/reload_restreamer")
            except OSError:
                pass

        configured = load_streams_safe()
        active_streams = {s["id"]: s for s in configured if s.get("enabled", True)}

        # 1. إيقاف البثوث المحذوفة أو المعطلة أو في حالة إعادة التشغيل الشامل
        for sid in list(workers.keys()):
            if sid not in active_streams or restart_all:
                log(f"إيقاف البث: [{sid}]")
                workers[sid]["stop"].set()
                if workers[sid].get("proc"):
                    kill_process_group(workers[sid]["proc"])
                workers[sid]["thread"].join(timeout=2.0)
                del workers[sid]

        # 2. فحص البثوث: هل تغير مفتاح أو مقاس أو نسبة التكبير؟ (Granular Hot-Reload)
        for sid, sdata in active_streams.items():
            if sid in workers:
                old_data = workers[sid].get("data", {})
                if (old_data.get("target_url") != sdata.get("target_url") or
                    old_data.get("layout") != sdata.get("layout") or
                    old_data.get("platform") != sdata.get("platform") or
                    old_data.get("zoom_pct", 80) != sdata.get("zoom_pct", 80)):
                    log(f"🔄 تم رصد تعديل إعدادات/حجم البث [{sdata.get('tiktok_user')}|{sid}] (تكبير: {sdata.get('zoom_pct', 80)}%)، جاري تطبيق التغيير فوراً دون لمس باقي البثوث...")
                    workers[sid]["stop"].set()
                    if workers[sid].get("proc"):
                        kill_process_group(workers[sid]["proc"])
                    workers[sid]["thread"].join(timeout=2.0)
                    del workers[sid]

        # 3. تشغيل أي بث جديد (البثوث المستقرة تظل تعمل بلا أي انقطاع)
        for sid, sdata in active_streams.items():
            if sid not in workers or not workers[sid]["thread"].is_alive():
                stop_evt = threading.Event()
                w_ref = {"thread": None, "stop": stop_evt, "data": dict(sdata), "proc": None}
                th = threading.Thread(target=stream_worker, args=(sdata, stop_evt, w_ref), daemon=True)
                w_ref["thread"] = th
                th.start()
                workers[sid] = w_ref

        time.sleep(2)

if __name__ == "__main__":
    main()
