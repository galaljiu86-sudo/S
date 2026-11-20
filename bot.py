#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Stream Management & Monitoring Telegram Bot.
- Fixed 180% Landscape-to-Portrait black canvas mode (no manual live zoom controls).
- Unique stream IDs in all inline callbacks.
"""

import json
import os
import re
import subprocess
import time
import uuid
import psutil
import telebot
from telebot import types

BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "").lstrip("@").lower().strip()
MY_NAME = os.environ.get("MY_NAME", "Runner")

DATA_DIR = os.path.abspath("data")
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
STATUS_PATH = os.path.join(DATA_DIR, "status.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
RESTREAMER_LOG = os.path.join(DATA_DIR, "restreamer.log")

os.makedirs(DATA_DIR, exist_ok=True)

if not BOT_TOKEN:
    print("[BOT] ERROR: BOT_TOKEN is missing!", flush=True)
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
LAST_VALID_STREAMS = []
USER_STATES = {}


def is_admin(user) -> bool:
    if not ADMIN_USERNAME:
        return True
    return bool(user and user.username and user.username.lower() == ADMIN_USERNAME)


def load_statuses() -> dict:
    if os.path.exists(STATUS_PATH):
        try:
            with open(STATUS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def load_streams() -> list:
    global LAST_VALID_STREAMS
    if os.path.exists(STREAMS_PATH):
        try:
            with open(STREAMS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    statuses = load_statuses()
                    for idx, s in enumerate(data):
                        if not s.get("id"):
                            s["id"] = f"{s.get('tiktok_user')}_{idx}"
                        sid = s["id"]
                        if sid in statuses:
                            s["is_live"] = statuses[sid].get("is_live", False)
                    LAST_VALID_STREAMS = data
                    return data
        except Exception:
            pass
    return list(LAST_VALID_STREAMS)


def save_streams(streams: list):
    tmp_path = STREAMS_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(streams, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, STREAMS_PATH)
    with open("/tmp/reload_restreamer", "w") as f:
        f.write(str(time.time()))


def save_chat_id(chat_id: int):
    tmp_path = CONFIG_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({"chat_id": chat_id}, f)
    os.replace(tmp_path, CONFIG_PATH)


def find_stream_idx(identifier: str, streams: list) -> int:
    if not streams:
        return -1
    raw_str = str(identifier).strip()
    for i, s in enumerate(streams):
        if str(s.get("id")) == raw_str:
            return i
    if raw_str.isdigit():
        idx = int(raw_str)
        if 0 <= idx < len(streams):
            return idx
    return -1


def get_layout_title(layout_code: str) -> str:
    mapping = {
        "portrait_copy": "📱 طولي أصلي (نسخ مباشر مستمر)",
        "portrait_stable": "⚡ طولي فائق الثبات (30fps & 2500k)",
        "landscape_to_portrait_pad": "🎬 طولي بشاشة سوداء وتكبير 180%",
        "landscape_raw": "🖥️ عرضي كامل كما هو",
    }
    return mapping.get(layout_code, "📱 طولي")


def build_stream_card(idx: int, s: dict):
    sid = s["id"]
    is_enabled = s.get("enabled", True)
    if not is_enabled:
        status_txt = "⏸️ متوقف مؤقتاً"
    elif s.get("is_live"):
        status_txt = "🔴 لايف شغال حالياً"
    else:
        status_txt = "⚪ في الانتظار (أوفلاين)"

    layout_str = get_layout_title(s.get("layout", "portrait_copy"))
    key_preview = s["stream_key"][:6] + "..." if s.get("stream_key") else "مخصص"
    msg = (
        f"<b>#{idx+1} - @{s['tiktok_user']}</b>\n"
        f"• <b>المنصة:</b> {s['platform']}\n"
        f"• <b>نمط العرض:</b> {layout_str}\n"
        f"• <b>المفتاح:</b> <code>{key_preview}</code>\n"
        f"• <b>الحالة:</b> {status_txt}\n"
    )
    markup = types.InlineKeyboardMarkup(row_width=2)
    toggle_btn = (
        types.InlineKeyboardButton("⏸️ إيقاف مؤقت", callback_data=f"pause_{sid}")
        if is_enabled
        else types.InlineKeyboardButton("▶️ تشغيل البث", callback_data=f"resume_{sid}")
    )
    layout_btn = types.InlineKeyboardButton("📐 تغيير المقاس", callback_data=f"chlayout_{sid}")
    key_btn = types.InlineKeyboardButton("🔑 تغيير المفتاح", callback_data=f"chkey_{sid}")
    del_btn = types.InlineKeyboardButton("🗑️ حذف", callback_data=f"del_{sid}")
    markup.add(toggle_btn, layout_btn, key_btn, del_btn)
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


@bot.message_handler(commands=["start"])
def handle_start(message):
    if not is_admin(message.from_user):
        bot.send_message(message.chat.id, "⛔ <b>عذراً، هذا البوت مخصص لمدير السيرفر فقط.</b>")
        return
    save_chat_id(message.chat.id)
    bot.send_message(
        message.chat.id,
        f"👋 مرحباً بك يا @{message.from_user.username} في لوحة التحكم!\n"
        f"🖥️ السيرفر النشط الآن: <code>{MY_NAME}</code>",
        reply_markup=main_keyboard(),
    )


@bot.message_handler(func=lambda msg: True)
def handle_text(message):
    if not is_admin(message.from_user):
        bot.send_message(message.chat.id, "⛔ <b>عذراً، هذا البوت مخصص لمدير السيرفر فقط.</b>")
        return
    chat_id = message.chat.id
    text = (message.text or "").strip()
    save_chat_id(chat_id)

    if text == "111":
        USER_STATES.pop(chat_id, None)
        bot.send_message(chat_id, "🔙 <b>تم الإلغاء والعودة للشاشة الرئيسية.</b>", reply_markup=main_keyboard())
        return

    state = USER_STATES.get(chat_id, {}).get("step")

    if text == "📊 إحصائيات البثوث":
        streams = load_streams()
        total = len(streams)
        active_enabled = sum(1 for s in streams if s.get("enabled", True))
        live_now = sum(1 for s in streams if s.get("is_live") and s.get("enabled", True))
        paused = total - active_enabled
        offline = active_enabled - live_now
        res = (
            f"<b>📊 تقرير حالة البثوث المباشرة:</b>\n\n"
            f"• <b>إجمالي البثوث:</b> <code>{total}</code>\n"
            f"• <b>تعمل لايف الآن:</b> 🔴 <code>{live_now}</code>\n"
            f"• <b>في الانتظار (أوفلاين):</b> ⚪ <code>{offline}</code>\n"
            f"• <b>متوقفة مؤقتاً:</b> ⏸️ <code>{paused}</code>\n"
        )
        bot.send_message(chat_id, res, reply_markup=main_keyboard())
        return

    elif text == "📈 استهلاك وسرعة السيرفر":
        net_start = psutil.net_io_counters()
        cpu = psutil.cpu_percent(interval=1.0)
        net_end = psutil.net_io_counters()
        ram = psutil.virtual_memory()
        up_kb = (net_end.bytes_sent - net_start.bytes_sent) / 1024.0
        down_kb = (net_end.bytes_recv - net_start.bytes_recv) / 1024.0
        msg = (
            f"📈 <b>بيانات السيرفر ({MY_NAME}):</b>\n\n"
            f"🖥️ <b>المعالج:</b> <code>{cpu}%</code> | 🧠 <b>الرام:</b> <code>{ram.percent}%</code>\n"
            f"⬆️ <b>الرفع:</b> <code>{(up_kb*8/1000):.2f} Mbps</code> | ⬇️ <b>السحب:</b> <code>{(down_kb*8/1000):.2f} Mbps</code>"
        )
        bot.send_message(chat_id, msg, reply_markup=main_keyboard())
        return

    elif text == "📜 سجل التشغيل (Logs)":
        logs_txt = "ملف السجل غير موجود."
        if os.path.exists(RESTREAMER_LOG):
            res = subprocess.run(["tail", "-n", "30", RESTREAMER_LOG], capture_output=True, text=True)
            logs_txt = res.stdout.strip() or "السجل فارغ."
        escaped = telebot.formatting.escape_html(logs_txt[-3500:])
        bot.send_message(chat_id, f"📜 <b>آخر 30 سطر:</b>\n\n<pre>{escaped}</pre>", reply_markup=main_keyboard())
        return

    elif text == "📋 قائمة وإدارة البثوث":
        streams = load_streams()
        if not streams:
            bot.send_message(chat_id, "ℹ️ لا توجد بثوث مضافة.", reply_markup=main_keyboard())
            return
        for idx, s in enumerate(streams):
            msg, markup = build_stream_card(idx, s)
            bot.send_message(chat_id, msg, reply_markup=markup)
        return

    elif text == "🔄 إعادة تشغيل البثوث":
        with open("/tmp/restart_all_streams", "w") as f:
            f.write(str(time.time()))
        bot.send_message(chat_id, "🔄 تم إرسال أمر إعادة تشغيل آمن لكل البثوث.", reply_markup=main_keyboard())
        return

    elif text == "➕ إضافة بث جديد":
        USER_STATES[chat_id] = {"step": "AWAIT_TIKTOK_USER"}
        bot.send_message(chat_id, "📌 <b>أرسل اسم حساب تيك توك أو رابطه:</b>", reply_markup=types.ReplyKeyboardRemove())
        return

    if state == "AWAIT_NEW_KEY":
        new_key = text.strip()
        target_sid = USER_STATES[chat_id].get("stream_id")
        streams = load_streams()
        idx = find_stream_idx(target_sid, streams)
        if idx != -1:
            s = streams[idx]
            if s.get("platform") == "Custom RTMP":
                s["target_url"] = new_key
                s["server_url"] = new_key
                s["stream_key"] = ""
            else:
                s["stream_key"] = new_key
                server_url = s.get("server_url", "")
                s["target_url"] = f"{server_url.rstrip('/')}/{new_key.lstrip('/')}" if server_url else new_key
            save_streams(streams)
            USER_STATES.pop(chat_id, None)
            bot.send_message(chat_id, f"✅ تم تحديث مفتاح @{s['tiktok_user']} بنجاح.", reply_markup=main_keyboard())
        else:
            USER_STATES.pop(chat_id, None)
            bot.send_message(chat_id, "⚠️ لم يتم العثور على البث.", reply_markup=main_keyboard())
        return

    if state == "AWAIT_TIKTOK_USER":
        match = re.search(r"tiktok\.com/@([^/?#]+)", text)
        username = match.group(1) if match else text.lstrip("@").strip()
        USER_STATES[chat_id] = {"step": "CHOOSE_PLATFORM", "tiktok_user": username}
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔵 Facebook Live", callback_data="plat_fb"),
            types.InlineKeyboardButton("🟣 Instagram Live", callback_data="plat_ig"),
            types.InlineKeyboardButton("🔴 YouTube Live", callback_data="plat_yt"),
            types.InlineKeyboardButton("🌐 Custom RTMP", callback_data="plat_custom"),
        )
        bot.send_message(chat_id, f"تم تحديد الحساب: <b>@{username}</b>\n\nاختر المنصة:", reply_markup=markup)
        return

    elif state == "AWAIT_STREAM_KEY":
        data = USER_STATES[chat_id]
        stream_key = text.strip()
        data["stream_key"] = stream_key
        data["target_url"] = f"{data['server_url'].rstrip('/')}/{stream_key.lstrip('/')}"
        data["step"] = "CHOOSE_ORIENTATION"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📱 بالطول (افتراضي)", callback_data="orient_portrait"),
            types.InlineKeyboardButton("🖥️ بالعرض", callback_data="orient_landscape"),
        )
        bot.send_message(chat_id, "📐 <b>هل البث الأساسي على تيك توك بالطول ولا بالعرض؟</b>", reply_markup=markup)
        return

    elif state == "AWAIT_CUSTOM_URL":
        target_url = text.strip()
        data = USER_STATES[chat_id]
        data["server_url"] = target_url
        data["stream_key"] = ""
        data["target_url"] = target_url
        data["step"] = "CHOOSE_ORIENTATION"
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📱 بالطول (افتراضي)", callback_data="orient_portrait"),
            types.InlineKeyboardButton("🖥️ بالعرض", callback_data="orient_landscape"),
        )
        bot.send_message(chat_id, "📐 <b>هل البث الأساسي على تيك توك بالطول ولا بالعرض؟</b>", reply_markup=markup)
        return


@bot.callback_query_handler(func=lambda call: call.data.startswith("plat_"))
def handle_plat(call):
    if not is_admin(call.from_user):
        return
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        return
    code = call.data.split("_")[1]
    if code == "fb":
        USER_STATES[chat_id].update({"step": "AWAIT_STREAM_KEY", "platform": "Facebook", "server_url": "rtmps://live-api-s.facebook.com:443/rtmp/"})
        bot.edit_message_text("🔵 <b>Facebook Live</b>\n🔑 أرسل مفتاح البث:", chat_id, call.message.message_id)
    elif code == "ig":
        USER_STATES[chat_id].update({"step": "AWAIT_STREAM_KEY", "platform": "Instagram", "server_url": "rtmps://live-upload.instagram.com:443/rtmp/"})
        bot.edit_message_text("🟣 <b>Instagram Live</b>\n🔑 أرسل مفتاح البث:", chat_id, call.message.message_id)
    elif code == "yt":
        USER_STATES[chat_id].update({"step": "AWAIT_STREAM_KEY", "platform": "YouTube", "server_url": "rtmp://a.rtmp.youtube.com/live2/"})
        bot.edit_message_text("🔴 <b>YouTube Live</b>\n🔑 أرسل مفتاح البث:", chat_id, call.message.message_id)
    elif code == "custom":
        USER_STATES[chat_id].update({"step": "AWAIT_CUSTOM_URL", "platform": "Custom RTMP"})
        bot.edit_message_text("🌐 أرسل رابط RTMP كاملاً:", chat_id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data in ["orient_portrait", "orient_landscape"])
def handle_orient(call):
    if not is_admin(call.from_user):
        return
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        return
    data = USER_STATES[chat_id]
    if call.data == "orient_portrait":
        data["layout"] = "portrait_copy"
        finalize_add_stream(chat_id, call.message.message_id, data)
    else:
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("🎬 طولي بشاشة سوداء وتكبير 180%", callback_data="zoom_yes"),
            types.InlineKeyboardButton("🖥️ عرضي كامل كما هو", callback_data="zoom_no"),
        )
        bot.edit_message_text("🎬 <b>اختر شكل البث العرضي:</b>", chat_id, call.message.message_id, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data in ["zoom_yes", "zoom_no"])
def handle_zoom_decision(call):
    if not is_admin(call.from_user):
        return
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        return
    data = USER_STATES[chat_id]
    data["layout"] = "landscape_to_portrait_pad" if call.data == "zoom_yes" else "landscape_raw"
    finalize_add_stream(chat_id, call.message.message_id, data)


def finalize_add_stream(chat_id: int, message_id: int, data: dict):
    streams = load_streams()
    streams.append({
        "id": uuid.uuid4().hex[:8],
        "tiktok_user": data["tiktok_user"],
        "platform": data["platform"],
        "server_url": data.get("server_url", ""),
        "stream_key": data.get("stream_key", ""),
        "target_url": data["target_url"],
        "layout": data.get("layout", "portrait_copy"),
        "enabled": True,
        "is_live": False,
    })
    save_streams(streams)
    USER_STATES.pop(chat_id, None)
    bot.edit_message_text(
        f"🎉 <b>تم حفظ البث وبدء مراقبته بنجاح!</b>\n"
        f"• <b>تيك توك:</b> <code>@{data['tiktok_user']}</code>\n"
        f"• <b>المنصة:</b> {data['platform']}\n"
        f"• <b>النمط:</b> {get_layout_title(data.get('layout', 'portrait_copy'))}",
        chat_id,
        message_id,
    )
    bot.send_message(chat_id, "جاهز ومستعد للمراقبة:", reply_markup=main_keyboard())


@bot.callback_query_handler(func=lambda call: call.data.startswith("chlayout_"))
def handle_change_layout(call):
    if not is_admin(call.from_user):
        return
    target = call.data[len("chlayout_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث.")
        return
    bot.answer_callback_query(call.id)
    sid = streams[idx]["id"]
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📱 طولي أصلي (نسخ مباشر مستمر)", callback_data=f"setlay_{sid}_portrait_copy"),
        types.InlineKeyboardButton("⚡ طولي فائق الثبات (30fps & 2500k)", callback_data=f"setlay_{sid}_portrait_stable"),
        types.InlineKeyboardButton("🎬 طولي بشاشة سوداء وتكبير 180%", callback_data=f"setlay_{sid}_landscape_to_portrait_pad"),
        types.InlineKeyboardButton("🖥️ عرضي كامل كما هو", callback_data=f"setlay_{sid}_landscape_raw"),
    )
    bot.edit_message_text(f"📐 <b>اختر نمط العرض لحساب @{streams[idx]['tiktok_user']}:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("setlay_"))
def handle_set_layout(call):
    if not is_admin(call.from_user):
        return
    raw = call.data[len("setlay_"):]
    known_layouts = ["portrait_copy", "portrait_stable", "landscape_to_portrait_pad", "landscape_raw"]
    matched_layout, target_str = None, None
    for lay in known_layouts:
        if raw.endswith("_" + lay):
            matched_layout = lay
            target_str = raw[:-len("_" + lay)]
            break
    if not matched_layout:
        return
    streams = load_streams()
    idx = find_stream_idx(target_str, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    streams[idx]["layout"] = matched_layout
    save_streams(streams)
    bot.answer_callback_query(call.id, "✅ تم تغيير المقاس!")
    bot.edit_message_text(
        f"✅ تم تحديث نمط العرض لـ @{streams[idx]['tiktok_user']} إلى:\n<b>{get_layout_title(matched_layout)}</b>",
        call.message.chat.id,
        call.message.message_id,
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("chkey_"))
def handle_change_key(call):
    if not is_admin(call.from_user):
        return
    target = call.data[len("chkey_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        return
    bot.answer_callback_query(call.id)
    s = streams[idx]
    USER_STATES[call.message.chat.id] = {"step": "AWAIT_NEW_KEY", "stream_id": s["id"]}
    bot.send_message(call.message.chat.id, f"🔑 أرسل المفتاح الجديد لحساب @{s['tiktok_user']}:", reply_markup=types.ReplyKeyboardRemove())


@bot.callback_query_handler(func=lambda call: call.data.startswith("pause_"))
def handle_pause(call):
    if not is_admin(call.from_user):
        return
    streams = load_streams()
    idx = find_stream_idx(call.data[len("pause_"):], streams)
    if idx != -1:
        streams[idx]["enabled"] = False
        save_streams(streams)
        bot.answer_callback_query(call.id, "⏸️ تم الإيقاف المؤقت")
        bot.edit_message_text(f"⏸️ تم إيقاف بث @{streams[idx]['tiktok_user']} مؤقتاً.", call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("resume_"))
def handle_resume(call):
    if not is_admin(call.from_user):
        return
    streams = load_streams()
    idx = find_stream_idx(call.data[len("resume_"):], streams)
    if idx != -1:
        streams[idx]["enabled"] = True
        save_streams(streams)
        bot.answer_callback_query(call.id, "▶️ تم التشغيل")
        bot.edit_message_text(f"▶️ تم تشغيل بث @{streams[idx]['tiktok_user']}.", call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("del_"))
def handle_delete(call):
    if not is_admin(call.from_user):
        return
    streams = load_streams()
    idx = find_stream_idx(call.data[len("del_"):], streams)
    if idx != -1:
        deleted = streams.pop(idx)
        save_streams(streams)
        bot.answer_callback_query(call.id, "🗑️ تم الحذف")
        bot.edit_message_text(f"🗑️ تم حذف بث حساب: @{deleted['tiktok_user']}", call.message.chat.id, call.message.message_id)


if __name__ == "__main__":
    print(f"[BOT] Starting Telegram Bot on {MY_NAME}...", flush=True)
    while True:
        try:
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception:
            time.sleep(3)
