#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
High-Performance Multi-Channel TikTok Restreamer (Hardened Edition).
- YouTube Live: Dr. Shaker copy pipeline (0% CPU video, 16M buffer, H.264 NAL filler 2000k-3500k, flush_packets).
- Meta (Facebook & Instagram): Strict 2500k CBR (nal-hrd=cbr, bufsize 5000k), 30fps CFR, 128k AAC, nice -n 10.
- Dynamic Live Zoom (50% - 200%): Safe crop+pad for all aspect ratios & instant hot-reload.
- Full Process Group isolation (os.setsid + SIGTERM/SIGKILL + thread.join) & atomic status.json.
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

TIKTOK_API_LOCK = threading.Lock()
LAST_TIKTOK_REQ = 0.0
MIN_TIKTOK_GAP = 4.0

STATUS_LOCK = threading.Lock()
LAST_VALID_STREAMS = []
ACTIVE_PROCS = {}  # stream_id -> subprocess.Popen
ACTIVE_PROCS_LOCK = threading.Lock()
SHUTDOWN_EVENT = threading.Event()


def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# محرك حشو البت ريت الأصلي (كود دكتور شاكر لليوتيوب)
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
# إدارة الحالة الذرية، الإشعارات، وحماية اتصالات تيك توك
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


def update_stream_live_status(stream_id: str, tiktok_user: str, is_live: bool):
    """كتابة الحالة اللحظية في status.json فقط دون لمس streams.json أبداً."""
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
        except Exception as e:
            log(f"[STATUS_SAVE_ERR] {e}")


def acquire_tiktok_slot():
    global LAST_TIKTOK_REQ
    with TIKTOK_API_LOCK:
        now = time.monotonic()
        wait = MIN_TIKTOK_GAP - (now - LAST_TIKTOK_REQ)
        if wait > 0:
            time.sleep(wait)
        LAST_TIKTOK_REQ = time.monotonic()


def check_is_live(tiktok_url: str) -> bool:
    acquire_tiktok_slot()
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
    for _ in range(4):
        if stop_event.is_set() or SHUTDOWN_EVENT.is_set():
            return True
        stop_event.wait(10)
        if check_is_live(tiktok_url):
            return False
    return True


def kill_process_group(proc: subprocess.Popen):
    if proc is None or proc.poll() is not None:
        return
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=2)
        except Exception:
            pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# فلتر التكبير الديناميكي (من 50% حتى 200% لأي أبعاد بدون انهيار)
# ---------------------------------------------------------------------------

def get_zoom_filter(zoom_pct: int = 80, pos_y: float = 0.38) -> str:
    scale_w = int(720 * (zoom_pct / 100.0))
    if scale_w % 2 != 0:
        scale_w += 1
    scale_w = max(360, min(1440, scale_w))

    return (
        f"fps=30,"
        f"scale=w={scale_w}:h=-2:flags=bicubic,"
        f"crop=w=min(iw\\,720):h=min(ih\\,1280):x=(iw-ow)/2:y=(ih-oh)/2,"
        f"pad=720:1280:(ow-iw)/2:(oh-ih)*{pos_y}:black,"
        f"format=yuv420p"
    )


# ---------------------------------------------------------------------------
# بناء أوامر البث (متوافقة 100% مع FFmpeg 4.4)
# ---------------------------------------------------------------------------

def build_pipeline_command(tiktok_url: str, target: str, platform: str, layout: str, zoom_pct: int = 80) -> str:
    H264_QUALITY = "h264_hd,h264_sd,720p,h264_origin"

    if platform in ["Facebook", "Instagram"]:
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
            vf_filter = (
                "fps=30,"
                "scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                "pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,"
                "format=yuv420p"
            )

        return (
            f"set -o pipefail; "
            f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
            f"mbuffer -q -m 16M -P 0 | "
            f"nice -n 10 ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
            f"-analyzeduration 10M -probesize 10M "
            f"-fflags +genpts+discardcorrupt "
            f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
            f"-vf '{vf_filter}' "
            f"-c:v libx264 -preset superfast -threads 2 "
            f"-profile:v high -level 4.1 "
            f"-b:v 2500k -minrate 2500k -maxrate 2500k -bufsize 5000k "
            f"-x264-params 'nal-hrd=cbr:force-cfr=1:scenecut=0' "
            f"-g 60 -keyint_min 60 -sc_threshold 0 "
            f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
            f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
            f"-max_muxing_queue_size 4096 "
            f"-flush_packets 1 -rw_timeout 15000000 "
            f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
        )

    else:
        if layout == "landscape_to_portrait_pad" or layout == "portrait_stable":
            vf_filter = (
                get_zoom_filter(zoom_pct)
                if layout == "landscape_to_portrait_pad"
                else "fps=30,scale=w=720:h=1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p"
            )
            return (
                f"set -o pipefail; "
                f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
                f"mbuffer -q -m 16M -P 0 | "
                f"nice -n 10 ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
                f"-analyzeduration 10M -probesize 10M "
                f"-fflags +genpts+discardcorrupt "
                f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
                f"-vf '{vf_filter}' "
                f"-c:v libx264 -preset superfast -threads 2 "
                f"-b:v 2500k -minrate 2500k -maxrate 2500k -bufsize 5000k "
                f"-x264-params 'nal-hrd=cbr:force-cfr=1:scenecut=0' "
                f"-g 60 -keyint_min 60 -sc_threshold 0 "
                f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
                f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
                f"-max_muxing_queue_size 2048 "
                f"-flush_packets 1 -rw_timeout 15000000 "
                f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )
        elif layout == "landscape_raw":
            return (
                f"set -o pipefail; "
                f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
                f"mbuffer -q -m 16M -P 0 | "
                f"ffmpeg -hide_banner -nostdin -loglevel warning -err_detect ignore_err "
                f"-fflags +genpts+discardcorrupt "
                f"-i pipe:0 -map 0:v:0 -map 0:a:0? "
                f"-c:v copy "
                f"-c:a aac -b:a 128k -ar 44100 -ac 2 "
                f"-af 'aresample=async=1000:min_hard_comp=0.100000:first_pts=0' "
                f"-max_muxing_queue_size 2048 "
                f"-flush_packets 1 -rw_timeout 15000000 "
                f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )
        else:
            # كود دكتور شاكر المعتمد للطولي الأصلي (نسخ مباشر + حشو NAL)
            return (
                f"set -o pipefail; "
                f"streamlink --no-config --stdout --stream-timeout 60 --http-timeout 30 --retry-streams 5 {shlex.quote(tiktok_url)} {H264_QUALITY} | "
                f"mbuffer -q -m 16M -P 0 | "
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
                f"-f flv -i pipe:0 -c copy -flush_packets 1 -rw_timeout 15000000 "
                f"-flvflags no_duration_filesize -f flv {shlex.quote(target)}"
            )


# ---------------------------------------------------------------------------
# مسار العمل المعزول لكل بث
# ---------------------------------------------------------------------------

def stream_worker(stream_data: dict, stop_event: threading.Event):
    sid = stream_data["id"]
    user = stream_data["tiktok_user"]
    target = stream_data["target_url"]
    platform = stream_data.get("platform", "YouTube")
    layout = stream_data.get("layout", "portrait_copy")
    zoom_pct = int(stream_data.get("zoom_pct", 80))
    tiktok_url = f"https://www.tiktok.com/@{user}/live"

    log(f"[{user} | {platform}] بدء المراقبة - النمط: {layout} (تكبير: {zoom_pct}%)")

    is_currently_live = False
    consecutive_fast_crashes = 0

    while not stop_event.is_set() and not SHUTDOWN_EVENT.is_set():
        is_live = check_is_live(tiktok_url)

        if not is_live:
            if is_currently_live:
                log(f"[{user}] فحص تأكيدي (40 ثانية) لانتهاء البث...")
                really_ended = verify_live_ended(tiktok_url, stop_event)
                if really_ended:
                    is_currently_live = False
                    update_stream_live_status(sid, user, False)
                    notify_telegram(f"⏹️ <b>انتهى البث المباشر لحساب @{user}.</b>\nتم إيقاف النقل إلى {platform}.")
                else:
                    log(f"[{user}] البث ما زال حياً، استمرار النقل!")
                    continue
            update_stream_live_status(sid, user, False)
            stop_event.wait(15)
            continue

        if not is_currently_live:
            is_currently_live = True
            update_stream_live_status(sid, user, True)
            notify_telegram(
                f"🔴 <b>بدأ البث المباشر لحساب @{user}!</b>\n"
                f"• <b>المنصة:</b> {platform}\n"
                f"🚀 جاري الإرسال المباشر فوراً!"
            )

        if stop_event.is_set() or SHUTDOWN_EVENT.is_set():
            break

        pipeline_cmd = build_pipeline_command(tiktok_url, target, platform, layout, zoom_pct)
        log(f"[{user} | {platform}] إطلاق خط الأنابيب...")

        err_file = f"/tmp/pipeline_err_{sid}.log"
        proc = None
        start_time = time.time()
        try:
            with open(err_file, "w") as ef:
                proc = subprocess.Popen(
                    ["/bin/bash", "-o", "pipefail", "-c", pipeline_cmd],
                    stderr=ef,
                    preexec_fn=os.setsid
                )
            with ACTIVE_PROCS_LOCK:
                ACTIVE_PROCS[sid] = proc

            while not stop_event.is_set() and not SHUTDOWN_EVENT.is_set():
                ret = proc.poll()
                if ret is not None:
                    duration = time.time() - start_time
                    log(f"[{user} | {platform}] توقف خط الأنابيب بعد {duration:.1f}ث (كود: {ret})")
                    break
                stop_event.wait(1.5)
        finally:
            if proc is not None:
                kill_process_group(proc)
            with ACTIVE_PROCS_LOCK:
                if ACTIVE_PROCS.get(sid) is proc:
                    ACTIVE_PROCS.pop(sid, None)

        if stop_event.is_set() or SHUTDOWN_EVENT.is_set():
            break

        duration = time.time() - start_time
        if duration < 25:
            consecutive_fast_crashes += 1
            backoff = min(120, 5 * (2 ** min(consecutive_fast_crashes - 1, 4)))
            log(f"[{user} | {platform}] توقف سريع ({duration:.1f}ث). تهدئة {backoff}ث لحماية باقي البثوث...")
            stop_event.wait(backoff)
        else:
            consecutive_fast_crashes = 0
            stop_event.wait(2)


def stop_worker(worker_entry: dict):
    worker_entry["stop"].set()
    sid = worker_entry["data"]["id"]
    with ACTIVE_PROCS_LOCK:
        proc = ACTIVE_PROCS.get(sid)
    if proc:
        kill_process_group(proc)
    worker_entry["thread"].join(timeout=5)


def handle_shutdown(signum, frame):
    log(f"استلام إشارة إغلاق ({signum})، جاري تنظيف كافة البثوث بأمان...")
    SHUTDOWN_EVENT.set()
    with ACTIVE_PROCS_LOCK:
        procs = list(ACTIVE_PROCS.values())
    for p in procs:
        kill_process_group(p)
    sys.exit(0)


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--filler":
        run_filler(int(sys.argv[2]), MAX_CAP_KBPS)
        return

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    log("محرك إدارة البثوث قيد التشغيل (يوتيوب كود شاكر | ميتا 2500k CBR | تحكم ديناميكي معزول)...")
    with open("/tmp/daemon_ready", "w") as f:
        f.write("ok")

    workers = {}  # stream_id -> {"thread": th, "stop": stop_evt, "data": dict}

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
            log("🔄 تنفيذ إعادة تشغيل آمنة لكافة البثوث...")
            for sid in list(workers.keys()):
                stop_worker(workers.pop(sid))

        configured = load_streams()
        active_map = {s["id"]: s for s in configured if s.get("enabled", True)}

        # 1. إيقاف البثوث المحذوفة أو المعطلة
        for sid in list(workers.keys()):
            if sid not in active_map:
                log(f"إيقاف البث: {workers[sid]['data'].get('tiktok_user')} ({sid})")
                stop_worker(workers.pop(sid))

        # 2. التحديث الحصري (Granular Hot-Reload) للبث الذي تغيرت إعداداته فقط
        for sid, sdata in active_map.items():
            if sid in workers:
                old = workers[sid]["data"]
                if (
                    old.get("target_url") != sdata.get("target_url")
                    or old.get("layout") != sdata.get("layout")
                    or old.get("platform") != sdata.get("platform")
                    or old.get("zoom_pct", 80) != sdata.get("zoom_pct", 80)
                ):
                    log(f"🔄 تحديث فوري لبث @{sdata['tiktok_user']} ({sid}) دون المساس ببقية البثوث...")
                    stop_worker(workers.pop(sid))

        # 3. تشغيل البثوث الجديدة أو المتوقفة
        for sid, sdata in active_map.items():
            if sid not in workers or not workers[sid]["thread"].is_alive():
                stop_evt = threading.Event()
                th = threading.Thread(target=stream_worker, args=(sdata, stop_evt), daemon=True)
                th.start()
                workers[sid] = {"thread": th, "stop": stop_evt, "data": dict(sdata)}

        time.sleep(2)


if __name__ == "__main__":
    main()
