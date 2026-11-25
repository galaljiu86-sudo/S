#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Multi-Stream Management & Monitoring Telegram Bot.

Improvements:
- Atomic flock-protected state mutations via state_store (zero race conditions).
- Standby awareness: waits cleanly while peer handoff is finishing (no 409 Conflict).
- Inline cancel & delete confirmation, duplicate detection, URL/key validation.
- Per-channel diagnostic logs and live in-place card updates.
"""

from __future__ import annotations

import html
import os
import re
import time
import uuid

import psutil
import telebot
from telebot import types

from state_store import (
    DATA_DIR,
    ensure_data_dir,
    load_global_config,
    load_streams,
    mutate_streams,
    save_global_config,
)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "").lstrip("@").lower().strip()
ADMIN_IDS = {
    item.strip()
    for item in os.environ.get("ADMIN_ID", "").split(",")
    if item.strip().isdigit()
}
MY_NAME = os.environ.get("MY_NAME", "Runner")
RESTREAMER_LOG = os.path.join(DATA_DIR, "restreamer.log")
STANDBY_FLAG_PATH = os.environ.get("STANDBY_FLAG_PATH", "/tmp/standby_mode")
BOT_READY_PATH = os.environ.get("BOT_READY_PATH", "/tmp/bot_ready")

KNOWN_LAYOUTS = (
    "portrait_copy",
    "portrait_stable",
    "landscape_to_portrait_pad",
    "landscape_raw",
)

PLATFORM_SERVERS = {
    "fb": ("Facebook", "rtmps://live-api-s.facebook.com:443/rtmp/"),
    "ig": ("Instagram", "rtmps://live-upload.instagram.com:443/rtmp/"),
    "yt": ("YouTube", "rtmp://a.rtmp.youtube.com/live2/"),
}

USER_STATES: dict[int, dict] = {}
bot: telebot.TeleBot | None = None


def is_admin(user) -> bool:
    if not ADMIN_USERNAME and not ADMIN_IDS:
        return True
    if not user:
        return False
    if ADMIN_IDS and str(user.id) in ADMIN_IDS:
        return True
    if ADMIN_USERNAME and user.username and user.username.lower() == ADMIN_USERNAME:
        return True
    return False


def get_layout_title(layout_code: str) -> str:
    mapping = {
        "portrait_copy": "📱 طولي أصلي (نسخ مباشر مستمر)",
        "portrait_stable": "⚡ طولي فائق الثبات (30fps & 2500k)",
        "landscape_to_portrait_pad": "🎬 طولي بشاشة سوداء وتكبير 180%",
        "landscape_raw": "🖥️ عرضي كامل كما هو",
    }
    return mapping.get(layout_code, "📱 طولي")


def parse_tiktok_username(raw_text: str) -> str:
    text = (raw_text or "").strip()
    match = re.search(r"tiktok\.com/@([A-Za-z0-9._]+)", text)
    candidate = match.group(1) if match else text.lstrip("@").strip()
    candidate = candidate.split("/")[0].split("?")[0].strip()
    if not re.fullmatch(r"[A-Za-z0-9._]{2,64}", candidate):
        return ""
    return candidate


def validate_rtmp_url(url: str) -> bool:
    return bool(re.match(r"^rtmps?://[^\s]+$", (url or "").strip(), re.IGNORECASE))


def validate_stream_key(key: str) -> bool:
    cleaned = (key or "").strip()
    return bool(cleaned) and not any(ch.isspace() for ch in cleaned)


def mask_key(stream_data: dict) -> str:
    key = stream_data.get("stream_key", "")
    if not key:
        url = stream_data.get("target_url", "")
        tail = url.rstrip("/").rsplit("/", 1)[-1]
        if len(tail) > 8:
            return tail[:4] + "..." + tail[-4:]
        return "مخصص"
    if len(key) <= 8:
        return key[:3] + "..."
    return key[:6] + "..." + key[-3:]


def find_stream_idx(identifier: str, streams: list) -> int:
    if not streams:
        return -1
    raw_str = str(identifier).strip()
    for idx, stream in enumerate(streams):
        if str(stream.get("id")) == raw_str:
            return idx
    if raw_str.isdigit():
        idx = int(raw_str)
        if 0 <= idx < len(streams):
            return idx
    return -1


def format_duration(seconds: float) -> str:
    secs = max(0, int(seconds))
    hours, rem = divmod(secs, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}س {minutes}د"
    if minutes:
        return f"{minutes}د {secs}ث"
    return f"{secs}ث"


def tail_file(path: str, lines: int = 30, max_chars: int = 3400) -> str:
    if not os.path.exists(path):
        return ""
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as handle:
            if size > 32768:
                handle.seek(-32768, os.SEEK_END)
            chunk = handle.read().decode("utf-8", errors="replace")
        tailed = "\n".join(chunk.splitlines()[-lines:]).strip()
        return tailed[-max_chars:]
    except Exception:
        return ""


def build_stream_card(idx: int, s: dict):
    sid = s["id"]
    is_enabled = s.get("enabled", True)
    runtime = s.get("runtime_status") or {}

    if not is_enabled:
        status_txt = "⏸️ متوقف مؤقتاً"
    elif s.get("is_live"):
        live_since = runtime.get("live_since")
        uptime_part = f" ({format_duration(time.time() - live_since)})" if live_since else ""
        sessions = runtime.get("sessions", 1)
        dups = runtime.get("dup_skipped", 0)
        status_txt = f"🔴 لايف شغال الآن{uptime_part} | جلسات: {sessions} | تكرار محذوف: {dups}"
    elif runtime.get("prewarming"):
        status_txt = "🟡 جاهز للتسليم (Pre-warm)"
    else:
        status_txt = "⚪ في الانتظار (أوفلاين)"

    layout_str = get_layout_title(s.get("layout", "portrait_copy"))
    key_preview = mask_key(s)
    last_err = runtime.get("last_error")
    err_line = f"\n• <b>آخر تنبيه:</b> <code>{html.escape(str(last_err)[:90])}</code>" if last_err else ""

    msg = (
        f"<b>#{idx + 1} - @{html.escape(s['tiktok_user'])}</b>\n"
        f"• <b>المنصة:</b> {html.escape(s.get('platform', 'YouTube'))}\n"
        f"• <b>نمط العرض:</b> {layout_str}\n"
        f"• <b>المفتاح:</b> <code>{html.escape(key_preview)}</code>\n"
        f"• <b>الحالة:</b> {status_txt}"
        f"{err_line}\n"
    )
    markup = types.InlineKeyboardMarkup(row_width=2)
    toggle_btn = (
        types.InlineKeyboardButton("⏸️ إيقاف مؤقت", callback_data=f"pause_{sid}")
        if is_enabled
        else types.InlineKeyboardButton("▶️ تشغيل البث", callback_data=f"resume_{sid}")
    )
    layout_btn = types.InlineKeyboardButton("📐 تغيير المقاس", callback_data=f"chlayout_{sid}")
    key_btn = types.InlineKeyboardButton("🔑 تغيير المفتاح", callback_data=f"chkey_{sid}")
    log_btn = types.InlineKeyboardButton("📜 سجل القناة", callback_data=f"chlog_{sid}")
    refresh_btn = types.InlineKeyboardButton("🔄 تحديث", callback_data=f"refresh_{sid}")
    del_btn = types.InlineKeyboardButton("🗑️ حذف", callback_data=f"delask_{sid}")
    markup.add(toggle_btn, layout_btn, key_btn, log_btn, refresh_btn, del_btn)
    return msg, markup


def main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("➕ إضافة بث جديد"),
        types.KeyboardButton("📋 قائمة وإدارة البثوث"),
        types.KeyboardButton("📊 إحصائيات البثوث"),
        types.KeyboardButton("📈 استهلاك وسرعة السيرفر"),
        types.KeyboardButton("📜 سجل التشغيل (Logs)"),
        types.KeyboardButton("🔄 إعادة تشغيل البثوث"),
    )
    return markup


def cancel_inline_markup():
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("❌ إلغاء والعودة", callback_data="cancel_flow"))
    return markup


def safe_edit_message(text: str, chat_id: int, message_id: int, reply_markup=None):
    assert bot is not None
    try:
        bot.edit_message_text(
            text,
            chat_id,
            message_id,
            reply_markup=reply_markup,
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            raise


def safe_answer_callback(call, text: str = ""):
    assert bot is not None
    try:
        bot.answer_callback_query(call.id, text)
    except Exception:
        pass


def register_handlers(tg_bot: telebot.TeleBot) -> None:
    @tg_bot.message_handler(commands=["start", "cancel"])
    def handle_commands(message):
        if not is_admin(message.from_user):
            tg_bot.send_message(message.chat.id, "⛔ <b>عذراً، هذا البوت مخصص لمدير السيرفر فقط.</b>")
            return
        save_global_config({"chat_id": message.chat.id})
        USER_STATES.pop(message.chat.id, None)
        if message.text and message.text.startswith("/cancel"):
            tg_bot.send_message(
                message.chat.id,
                "🔙 <b>تم الإلغاء والعودة للشاشة الرئيسية.</b>",
                reply_markup=main_keyboard(),
            )
            return
        username = html.escape(message.from_user.username or str(message.from_user.id))
        tg_bot.send_message(
            message.chat.id,
            f"👋 مرحباً بك يا @{username} في لوحة التحكم!\n"
            f"🖥️ السيرفر النشط الآن: <code>{html.escape(MY_NAME)}</code>",
            reply_markup=main_keyboard(),
        )

    @tg_bot.message_handler(func=lambda msg: True)
    def handle_text(message):
        if not is_admin(message.from_user):
            tg_bot.send_message(message.chat.id, "⛔ <b>عذراً، هذا البوت مخصص لمدير السيرفر فقط.</b>")
            return
        chat_id = message.chat.id
        text = (message.text or "").strip()
        save_global_config({"chat_id": chat_id})

        if text in ("111", "إلغاء", "❌ إلغاء"):
            USER_STATES.pop(chat_id, None)
            tg_bot.send_message(
                chat_id,
                "🔙 <b>تم الإلغاء والعودة للشاشة الرئيسية.</b>",
                reply_markup=main_keyboard(),
            )
            return

        if text == "📊 إحصائيات البثوث":
            streams = load_streams(include_status=True)
            total = len(streams)
            active_enabled = sum(1 for s in streams if s.get("enabled", True))
            live_now = sum(1 for s in streams if s.get("is_live") and s.get("enabled", True))
            paused = total - active_enabled
            offline = active_enabled - live_now

            details = []
            for s in streams:
                rt = s.get("runtime_status") or {}
                if not s.get("enabled", True):
                    icon = "⏸️"
                elif s.get("is_live"):
                    icon = "🔴"
                else:
                    icon = "⚪"
                extra = ""
                if s.get("is_live"):
                    extra = f" (جلسة {rt.get('sessions', 1)}, تكرار محذوف {rt.get('dup_skipped', 0)})"
                details.append(f"{icon} @{html.escape(s['tiktok_user'])} → {html.escape(s.get('platform', ''))}{extra}")

            detail_block = ("\n\n<b>التفاصيل:</b>\n" + "\n".join(details)) if details else ""
            res = (
                f"<b>📊 تقرير حالة البثوث المباشرة ({html.escape(MY_NAME)}):</b>\n\n"
                f"• <b>إجمالي البثوث:</b> <code>{total}</code>\n"
                f"• <b>تعمل لايف الآن:</b> 🔴 <code>{live_now}</code>\n"
                f"• <b>في الانتظار (أوفلاين):</b> ⚪ <code>{offline}</code>\n"
                f"• <b>متوقفة مؤقتاً:</b> ⏸️ <code>{paused}</code>"
                f"{detail_block}"
            )
            tg_bot.send_message(chat_id, res, reply_markup=main_keyboard())
            return

        if text == "📈 استهلاك وسرعة السيرفر":
            net_start = psutil.net_io_counters()
            cpu = psutil.cpu_percent(interval=1.0)
            net_end = psutil.net_io_counters()
            ram = psutil.virtual_memory()
            up_kb = (net_end.bytes_sent - net_start.bytes_sent) / 1024.0
            down_kb = (net_end.bytes_recv - net_start.bytes_recv) / 1024.0
            msg = (
                f"📈 <b>بيانات السيرفر ({html.escape(MY_NAME)}):</b>\n\n"
                f"🖥️ <b>المعالج:</b> <code>{cpu}%</code> | 🧠 <b>الرام:</b> <code>{ram.percent}%</code> "
                f"(<code>{ram.used // (1024 * 1024)}/{ram.total // (1024 * 1024)} MB</code>)\n"
                f"⬆️ <b>الرفع:</b> <code>{(up_kb * 8 / 1000):.2f} Mbps</code> | "
                f"⬇️ <b>السحب:</b> <code>{(down_kb * 8 / 1000):.2f} Mbps</code>"
            )
            tg_bot.send_message(chat_id, msg, reply_markup=main_keyboard())
            return

        if text == "📜 سجل التشغيل (Logs)":
            logs_txt = tail_file(RESTREAMER_LOG, lines=30) or "ملف السجل فارغ أو غير موجود."
            escaped = html.escape(logs_txt)
            tg_bot.send_message(
                chat_id,
                f"📜 <b>آخر 30 سطر ({html.escape(MY_NAME)}):</b>\n\n<pre>{escaped}</pre>",
                reply_markup=main_keyboard(),
            )
            return

        if text == "📋 قائمة وإدارة البثوث":
            streams = load_streams(include_status=True)
            if not streams:
                tg_bot.send_message(chat_id, "ℹ️ لا توجد بثوث مضافة.", reply_markup=main_keyboard())
                return
            for idx, s in enumerate(streams):
                msg, markup = build_stream_card(idx, s)
                tg_bot.send_message(chat_id, msg, reply_markup=markup)
            return

        if text == "🔄 إعادة تشغيل البثوث":
            with open("/tmp/restart_all_streams", "w", encoding="utf-8") as handle:
                handle.write(str(time.time()))
            tg_bot.send_message(
                chat_id,
                "🔄 تم إرسال أمر إعادة تشغيل آمن لكل البثوث.",
                reply_markup=main_keyboard(),
            )
            return

        if text == "➕ إضافة بث جديد":
            USER_STATES[chat_id] = {"step": "AWAIT_TIKTOK_USER"}
            tg_bot.send_message(
                chat_id,
                "📌 <b>أرسل اسم حساب تيك توك أو رابطه:</b>\n"
                "<i>مثال: @username أو https://www.tiktok.com/@username/live</i>",
                reply_markup=cancel_inline_markup(),
            )
            return

        state = USER_STATES.get(chat_id, {}).get("step")

        if state == "AWAIT_NEW_KEY":
            new_key = text.strip()
            target_sid = USER_STATES[chat_id].get("stream_id")

            def _update_key(streams: list):
                idx = find_stream_idx(target_sid, streams)
                if idx == -1:
                    return None
                s = streams[idx]
                if s.get("platform") == "Custom RTMP":
                    if not validate_rtmp_url(new_key):
                        return "INVALID_URL"
                    s["target_url"] = new_key
                    s["server_url"] = new_key
                    s["stream_key"] = ""
                else:
                    if not validate_stream_key(new_key):
                        return "INVALID_KEY"
                    s["stream_key"] = new_key
                    server_url = s.get("server_url", "")
                    s["target_url"] = (
                        f"{server_url.rstrip('/')}/{new_key.lstrip('/')}"
                        if server_url else new_key
                    )
                return dict(s)

            res, _ = mutate_streams(_update_key)
            if res == "INVALID_URL":
                tg_bot.send_message(
                    chat_id,
                    "⚠️ <b>رابط RTMP غير صالح.</b> يجب أن يبدأ بـ <code>rtmp://</code> أو <code>rtmps://</code>.",
                    reply_markup=cancel_inline_markup(),
                )
                return
            if res == "INVALID_KEY":
                tg_bot.send_message(
                    chat_id,
                    "⚠️ <b>مفتاح البث غير صالح</b> (يجب ألا يحتوي على مسافات).",
                    reply_markup=cancel_inline_markup(),
                )
                return
            USER_STATES.pop(chat_id, None)
            if res:
                tg_bot.send_message(
                    chat_id,
                    f"✅ تم تحديث مفتاح @{html.escape(res['tiktok_user'])} بنجاح.",
                    reply_markup=main_keyboard(),
                )
            else:
                tg_bot.send_message(chat_id, "⚠️ لم يتم العثور على البث.", reply_markup=main_keyboard())
            return

        if state == "AWAIT_TIKTOK_USER":
            username = parse_tiktok_username(text)
            if not username:
                tg_bot.send_message(
                    chat_id,
                    "⚠️ <b>اسم حساب تيك توك غير صالح.</b>\nأرسل اسم المستخدم أو رابط البث الصحيح:",
                    reply_markup=cancel_inline_markup(),
                )
                return
            USER_STATES[chat_id] = {"step": "CHOOSE_PLATFORM", "tiktok_user": username}
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("🔵 Facebook Live", callback_data="plat_fb"),
                types.InlineKeyboardButton("🟣 Instagram Live", callback_data="plat_ig"),
                types.InlineKeyboardButton("🔴 YouTube Live", callback_data="plat_yt"),
                types.InlineKeyboardButton("🌐 Custom RTMP", callback_data="plat_custom"),
                types.InlineKeyboardButton("❌ إلغاء", callback_data="cancel_flow"),
            )
            tg_bot.send_message(
                chat_id,
                f"تم تحديد الحساب: <b>@{html.escape(username)}</b>\n\nاختر المنصة:",
                reply_markup=markup,
            )
            return

        if state == "AWAIT_STREAM_KEY":
            stream_key = text.strip()
            if not validate_stream_key(stream_key):
                tg_bot.send_message(
                    chat_id,
                    "⚠️ <b>مفتاح البث غير صالح</b> (أرسله بدون مسافات):",
                    reply_markup=cancel_inline_markup(),
                )
                return
            data = USER_STATES[chat_id]
            data["stream_key"] = stream_key
            data["target_url"] = f"{data['server_url'].rstrip('/')}/{stream_key.lstrip('/')}"
            data["step"] = "CHOOSE_ORIENTATION"
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("📱 بالطول (افتراضي)", callback_data="orient_portrait"),
                types.InlineKeyboardButton("🖥️ بالعرض", callback_data="orient_landscape"),
                types.InlineKeyboardButton("❌ إلغاء", callback_data="cancel_flow"),
            )
            tg_bot.send_message(
                chat_id,
                "📐 <b>هل البث الأساسي على تيك توك بالطول ولا بالعرض؟</b>",
                reply_markup=markup,
            )
            return

        if state == "AWAIT_CUSTOM_URL":
            target_url = text.strip()
            if not validate_rtmp_url(target_url):
                tg_bot.send_message(
                    chat_id,
                    "⚠️ <b>رابط RTMP غير صالح.</b> يجب أن يبدأ بـ <code>rtmp://</code> أو <code>rtmps://</code>.",
                    reply_markup=cancel_inline_markup(),
                )
                return
            data = USER_STATES[chat_id]
            data["server_url"] = target_url
            data["stream_key"] = ""
            data["target_url"] = target_url
            data["step"] = "CHOOSE_ORIENTATION"
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("📱 بالطول (افتراضي)", callback_data="orient_portrait"),
                types.InlineKeyboardButton("🖥️ بالعرض", callback_data="orient_landscape"),
                types.InlineKeyboardButton("❌ إلغاء", callback_data="cancel_flow"),
            )
            tg_bot.send_message(
                chat_id,
                "📐 <b>هل البث الأساسي على تيك توك بالطول ولا بالعرض؟</b>",
                reply_markup=markup,
            )
            return

    @tg_bot.callback_query_handler(func=lambda call: call.data == "cancel_flow")
    def handle_cancel_flow(call):
        if not is_admin(call.from_user):
            return
        safe_answer_callback(call, "تم الإلغاء")
        USER_STATES.pop(call.message.chat.id, None)
        safe_edit_message("🔙 <b>تم الإلغاء.</b>", call.message.chat.id, call.message.message_id)
        tg_bot.send_message(call.message.chat.id, "القائمة الرئيسية:", reply_markup=main_keyboard())

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("plat_"))
    def handle_plat(call):
        if not is_admin(call.from_user):
            return
        safe_answer_callback(call)
        chat_id = call.message.chat.id
        if chat_id not in USER_STATES:
            return
        code = call.data.split("_", 1)[1]
        if code in PLATFORM_SERVERS:
            plat_name, server_url = PLATFORM_SERVERS[code]
            USER_STATES[chat_id].update({
                "step": "AWAIT_STREAM_KEY",
                "platform": plat_name,
                "server_url": server_url,
            })
            safe_edit_message(
                f"📡 <b>{plat_name} Live</b>\n🔑 أرسل مفتاح البث:",
                chat_id,
                call.message.message_id,
                reply_markup=cancel_inline_markup(),
            )
        elif code == "custom":
            USER_STATES[chat_id].update({
                "step": "AWAIT_CUSTOM_URL",
                "platform": "Custom RTMP",
            })
            safe_edit_message(
                "🌐 أرسل رابط RTMP كاملاً:",
                chat_id,
                call.message.message_id,
                reply_markup=cancel_inline_markup(),
            )

    @tg_bot.callback_query_handler(func=lambda call: call.data in ("orient_portrait", "orient_landscape"))
    def handle_orient(call):
        if not is_admin(call.from_user):
            return
        safe_answer_callback(call)
        chat_id = call.message.chat.id
        if chat_id not in USER_STATES:
            return
        data = USER_STATES[chat_id]
        if call.data == "orient_portrait":
            data["layout"] = "portrait_copy"
            finalize_add_stream(tg_bot, chat_id, call.message.message_id, data)
        else:
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("🎬 طولي بشاشة سوداء وتكبير 180%", callback_data="zoom_yes"),
                types.InlineKeyboardButton("🖥️ عرضي كامل كما هو", callback_data="zoom_no"),
                types.InlineKeyboardButton("❌ إلغاء", callback_data="cancel_flow"),
            )
            safe_edit_message(
                "🎬 <b>اختر شكل البث العرضي:</b>",
                chat_id,
                call.message.message_id,
                reply_markup=markup,
            )

    @tg_bot.callback_query_handler(func=lambda call: call.data in ("zoom_yes", "zoom_no"))
    def handle_zoom_decision(call):
        if not is_admin(call.from_user):
            return
        safe_answer_callback(call)
        chat_id = call.message.chat.id
        if chat_id not in USER_STATES:
            return
        data = USER_STATES[chat_id]
        data["layout"] = "landscape_to_portrait_pad" if call.data == "zoom_yes" else "landscape_raw"
        finalize_add_stream(tg_bot, chat_id, call.message.message_id, data)

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("chlayout_"))
    def handle_change_layout(call):
        if not is_admin(call.from_user):
            return
        target = call.data[len("chlayout_"):]
        streams = load_streams()
        idx = find_stream_idx(target, streams)
        if idx == -1:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث.")
            return
        safe_answer_callback(call)
        sid = streams[idx]["id"]
        markup = types.InlineKeyboardMarkup(row_width=1)
        for lay in KNOWN_LAYOUTS:
            markup.add(types.InlineKeyboardButton(get_layout_title(lay), callback_data=f"setlay_{sid}_{lay}"))
        markup.add(types.InlineKeyboardButton("🔙 رجوع للبطاقة", callback_data=f"refresh_{sid}"))
        safe_edit_message(
            f"📐 <b>اختر نمط العرض لحساب @{html.escape(streams[idx]['tiktok_user'])}:</b>",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("setlay_"))
    def handle_set_layout(call):
        if not is_admin(call.from_user):
            return
        raw = call.data[len("setlay_"):]
        matched_layout, target_str = None, None
        for lay in KNOWN_LAYOUTS:
            if raw.endswith("_" + lay):
                matched_layout = lay
                target_str = raw[:-len("_" + lay)]
                break
        if not matched_layout:
            return

        def _update_layout(streams: list):
            idx = find_stream_idx(target_str, streams)
            if idx == -1:
                return None
            streams[idx]["layout"] = matched_layout
            return idx

        idx, _ = mutate_streams(_update_layout)
        if idx is None:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث")
            return
        safe_answer_callback(call, "✅ تم تغيير المقاس!")
        updated = load_streams(include_status=True)
        cur_idx = find_stream_idx(target_str, updated)
        if cur_idx != -1:
            msg, markup = build_stream_card(cur_idx, updated[cur_idx])
            safe_edit_message(msg, call.message.chat.id, call.message.message_id, reply_markup=markup)

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("chkey_"))
    def handle_change_key(call):
        if not is_admin(call.from_user):
            return
        target = call.data[len("chkey_"):]
        streams = load_streams()
        idx = find_stream_idx(target, streams)
        if idx == -1:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث")
            return
        safe_answer_callback(call)
        s = streams[idx]
        USER_STATES[call.message.chat.id] = {"step": "AWAIT_NEW_KEY", "stream_id": s["id"]}
        prompt = (
            f"🌐 أرسل رابط RTMP الجديد لحساب @{html.escape(s['tiktok_user'])}:"
            if s.get("platform") == "Custom RTMP"
            else f"🔑 أرسل المفتاح الجديد لحساب @{html.escape(s['tiktok_user'])}:"
        )
        tg_bot.send_message(call.message.chat.id, prompt, reply_markup=cancel_inline_markup())

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("chlog_"))
    def handle_channel_log(call):
        if not is_admin(call.from_user):
            return
        target = call.data[len("chlog_"):]
        streams = load_streams()
        idx = find_stream_idx(target, streams)
        if idx == -1:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث")
            return
        safe_answer_callback(call)
        s = streams[idx]
        sid = s["id"]
        err_log = tail_file(f"/tmp/pipeline_err_{sid}.log", lines=20, max_chars=2600)
        if not err_log:
            err_log = "لا توجد أخطاء أو سجلات FFmpeg حديثة لهذه القناة."
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("🔙 رجوع للبطاقة", callback_data=f"refresh_{sid}"))
        safe_edit_message(
            f"📜 <b>سجل قناة @{html.escape(s['tiktok_user'])}:</b>\n<pre>{html.escape(err_log)}</pre>",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("refresh_"))
    def handle_refresh(call):
        if not is_admin(call.from_user):
            return
        target = call.data[len("refresh_"):]
        streams = load_streams(include_status=True)
        idx = find_stream_idx(target, streams)
        if idx == -1:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث")
            return
        safe_answer_callback(call, "🔄 تم التحديث")
        msg, markup = build_stream_card(idx, streams[idx])
        safe_edit_message(msg, call.message.chat.id, call.message.message_id, reply_markup=markup)

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("pause_") or call.data.startswith("resume_"))
    def handle_toggle(call):
        if not is_admin(call.from_user):
            return
        is_pause = call.data.startswith("pause_")
        target = call.data[len("pause_"):] if is_pause else call.data[len("resume_"):]

        def _toggle(streams: list):
            idx = find_stream_idx(target, streams)
            if idx == -1:
                return None
            streams[idx]["enabled"] = not is_pause
            return idx

        idx, _ = mutate_streams(_toggle)
        if idx is None:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث")
            return
        safe_answer_callback(call, "⏸️ تم الإيقاف المؤقت" if is_pause else "▶️ تم التشغيل")
        updated = load_streams(include_status=True)
        cur_idx = find_stream_idx(target, updated)
        if cur_idx != -1:
            msg, markup = build_stream_card(cur_idx, updated[cur_idx])
            safe_edit_message(msg, call.message.chat.id, call.message.message_id, reply_markup=markup)

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("delask_"))
    def handle_delete_ask(call):
        if not is_admin(call.from_user):
            return
        target = call.data[len("delask_"):]
        streams = load_streams()
        idx = find_stream_idx(target, streams)
        if idx == -1:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث")
            return
        safe_answer_callback(call)
        s = streams[idx]
        sid = s["id"]
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("✅ نعم، احذف", callback_data=f"del_{sid}"),
            types.InlineKeyboardButton("❌ إلغاء", callback_data=f"refresh_{sid}"),
        )
        safe_edit_message(
            f"⚠️ <b>هل أنت متأكد من حذف بث @{html.escape(s['tiktok_user'])} ({html.escape(s['platform'])})؟</b>",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
        )

    @tg_bot.callback_query_handler(func=lambda call: call.data.startswith("del_"))
    def handle_delete(call):
        if not is_admin(call.from_user):
            return
        target = call.data[len("del_"):]

        def _delete(streams: list):
            idx = find_stream_idx(target, streams)
            if idx == -1:
                return None
            return streams.pop(idx)

        deleted, _ = mutate_streams(_delete)
        if not deleted:
            safe_answer_callback(call, "⚠️ لم يتم العثور على البث")
            return
        safe_answer_callback(call, "🗑️ تم الحذف")
        safe_edit_message(
            f"🗑️ تم حذف بث حساب: @{html.escape(deleted['tiktok_user'])}",
            call.message.chat.id,
            call.message.message_id,
        )


def finalize_add_stream(tg_bot: telebot.TeleBot, chat_id: int, message_id: int, data: dict) -> None:
    def _append_stream(streams: list):
        for existing in streams:
            if (
                existing.get("tiktok_user", "").lower() == data["tiktok_user"].lower()
                and existing.get("target_url") == data["target_url"]
            ):
                return "DUPLICATE"
        entry = {
            "id": uuid.uuid4().hex[:8],
            "tiktok_user": data["tiktok_user"],
            "platform": data["platform"],
            "server_url": data.get("server_url", ""),
            "stream_key": data.get("stream_key", ""),
            "target_url": data["target_url"],
            "layout": data.get("layout", "portrait_copy"),
            "enabled": True,
        }
        streams.append(entry)
        return entry

    res, _ = mutate_streams(_append_stream)
    USER_STATES.pop(chat_id, None)
    if res == "DUPLICATE":
        safe_edit_message(
            f"⚠️ البث <code>@{html.escape(data['tiktok_user'])}</code> بنفس المفتاح مضاف بالفعل.",
            chat_id,
            message_id,
        )
        tg_bot.send_message(chat_id, "القائمة الرئيسية:", reply_markup=main_keyboard())
        return

    safe_edit_message(
        f"🎉 <b>تم حفظ البث وبدء مراقبته بنجاح!</b>\n"
        f"• <b>تيك توك:</b> <code>@{html.escape(data['tiktok_user'])}</code>\n"
        f"• <b>المنصة:</b> {html.escape(data['platform'])}\n"
        f"• <b>النمط:</b> {get_layout_title(data.get('layout', 'portrait_copy'))}",
        chat_id,
        message_id,
    )
    tg_bot.send_message(chat_id, "جاهز ومستعد للمراقبة:", reply_markup=main_keyboard())


def mark_bot_ready() -> None:
    try:
        with open(BOT_READY_PATH, "w", encoding="utf-8") as handle:
            handle.write("ok")
    except OSError:
        pass


def main() -> None:
    global bot
    ensure_data_dir()
    if not BOT_TOKEN:
        print("[BOT] ERROR: BOT_TOKEN is missing!", flush=True)
        raise SystemExit(1)

    if not ADMIN_USERNAME and not ADMIN_IDS:
        print("[BOT] WARNING: ADMIN_USERNAME and ADMIN_ID are unset; anyone can control this bot!", flush=True)

    bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
    register_handlers(bot)
    mark_bot_ready()

    if os.path.exists(STANDBY_FLAG_PATH):
        print(f"[BOT] {MY_NAME} initialized in STANDBY mode (polling paused until handoff cutover).", flush=True)
        while os.path.exists(STANDBY_FLAG_PATH):
            time.sleep(0.25)

    print(f"[BOT] Starting Telegram Bot polling on {MY_NAME}...", flush=True)
    while True:
        try:
            bot.infinity_polling(timeout=15, long_polling_timeout=10)
        except Exception as exc:
            print(f"[BOT] Polling error: {exc}", flush=True)
            time.sleep(3)


if __name__ == "__main__":
    main()
