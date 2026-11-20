#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Stream Management & Monitoring Telegram Bot (Hardened Edition).
- Uses unique stream IDs in all inline callbacks (immune to list deletions).
- Admin verification on both text messages and inline callbacks.
- Decoupled status.json + atomic streams.json writes.
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
                        elif s.get("tiktok_user") in statuses:
                            s["is_live"] = statuses[s.get("tiktok_user")].get("is_live", False)
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
    clean_id = raw_str.lower().replace("@", "")
    for i, s in enumerate(streams):
        u = s.get("tiktok_user", "").lower()
        if u and (clean_id == u or clean_id.startswith(u) or u.startswith(clean_id)):
            return i
    return -1


def get_layout_title(layout_code: str, zoom_pct: int = 80) -> str:
    mapping = {
        "portrait_copy": "📱 طولي أصلي (نسخ مباشر كود شاكر)",
        "portrait_stable": "⚡ طولي فائق الثبات (30fps & 2500k)",
        "landscape_to_portrait_pad": f"🎬 طولي بشاشة سوداء وتكبير ({zoom_pct}%)",
        "landscape_raw": "🖥️ عرضي كامل كما هو",
    }
    return mapping.get(layout_code, "📱 طولي")


def build_zoom_markup(sid: str, current_pct: int) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    minus_5 = max(50, current_pct - 5)
    plus_5 = min(200, current_pct + 5)
    markup.row(
        types.InlineKeyboardButton(f"➖ 5% ({minus_5}%)", callback_data=f"setz_{sid}_{minus_5}"),
        types.InlineKeyboardButton(f"➕ 5% ({plus_5}%)", callback_data=f"setz_{sid}_{plus_5}"),
    )
    minus_10 = max(50, current_pct - 10)
    plus_10 = min(200, current_pct + 10)
    markup.row(
        types.InlineKeyboardButton(f"⏪ 10% ({minus_10}%)", callback_data=f"setz_{sid}_{minus_10}"),
        types.InlineKeyboardButton(f"⏩ 10% ({plus_10}%)", callback_data=f"setz_{sid}_{plus_10}"),
    )
    markup.row(
        types.InlineKeyboardButton("70%", callback_data=f"setz_{sid}_70"),
        types.InlineKeyboardButton("80%", callback_data=f"setz_{sid}_80"),
        types.InlineKeyboardButton("90%", callback_data=f"setz_{sid}_90"),
        types.InlineKeyboardButton("100%", callback_data=f"setz_{sid}_100"),
    )
    markup.row(
        types.InlineKeyboardButton("120%", callback_data=f"setz_{sid}_120"),
        types.InlineKeyboardButton("150%", callback_data=f"setz_{sid}_150"),
        types.InlineKeyboardButton("200% (ملء الشاشة)", callback_data=f"setz_{sid}_200"),
    )
    markup.row(types.InlineKeyboardButton("🔙 حفظ ورجوع", callback_data=f"zoomback_{sid}"))
    return markup


def build_stream_card(idx: int, s: dict):
    sid = s["id"]
    is_enabled = s.get("enabled", True)
    if not is_enabled:
        status_txt = "⏸️ متوقف مؤقتاً"
    elif s.get("is_live"):
        status_txt = "🔴 لايف شغال حالياً"
    else:
        status_txt = "⚪ في الانتظار (أوفلاين)"

    zoom_val = s.get("zoom_pct", 80)
    layout_str = get_layout_title(s.get("layout", "portrait_copy"), zoom_val)
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
    markup.add(toggle_btn, layout_btn)
    if s.get("layout") == "landscape_to_portrait_pad":
        markup.add(types.InlineKeyboardButton(f"🔍 تحكم في الحجم ({zoom_val}%)", callback_data=f"zoomctrl_{sid}"))
    markup.add(key_btn, del_btn)
    return msg, markup


def main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("➕ إضافة بث جديد"),
        types.KeyboardButton("📋 قائمة وإدارة البثوث"),
        types.KeyboardButton("📊 إحصائيات البثوث"),
        types.KeyboardButton("📈 استهلاك وسرعة السيرفر"),
        types.KeyboardButton("💡 سعة وتحمل السيرفر"),
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
        f"🖥️ السيرفر النشط الآن: <code>{MY_NAME}</code>\n\n"
        f"اختر من الأزرار بالأسفل لإدارة البثوث ومتابعة السرعة اللحظية:",
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
            f"• <b>إجمالي البثوث المسجلة:</b> <code>{total}</code>\n"
            f"• <b>بثوث تعمل لايف الآن:</b> 🔴 <code>{live_now}</code>\n"
            f"• <b>بثوث في وضع الانتظار (أوفلاين):</b> ⚪ <code>{offline}</code>\n"
            f"• <b>بثوث متوقفة مؤقتاً (بأمرك):</b> ⏸️ <code>{paused}</code>\n"
            f"• <b>السيرفر الخادم المسؤول:</b> <code>{MY_NAME}</code>\n"
        )
        bot.send_message(chat_id, res, reply_markup=main_keyboard())
        return

    elif text == "📈 استهلاك وسرعة السيرفر":
        net_start = psutil.net_io_counters()
        cpu = psutil.cpu_percent(interval=1.0)
        net_end = psutil.net_io_counters()
        ram = psutil.virtual_memory()
        up_speed_kb = (net_end.bytes_sent - net_start.bytes_sent) / 1024.0
        down_speed_kb = (net_end.bytes_recv - net_start.bytes_recv) / 1024.0
        up_speed_mbps = (up_speed_kb * 8.0) / 1000.0
        down_speed_mbps = (down_speed_kb * 8.0) / 1000.0
        total_sent_mb = net_end.bytes_sent / (1024 * 1024)
        total_recv_mb = net_end.bytes_recv / (1024 * 1024)
        uptime_sec = int(time.time() - psutil.boot_time())
        hours = uptime_sec // 3600
        mins = (uptime_sec % 3600) // 60
        streams = load_streams()
        live_count = sum(1 for s in streams if s.get("is_live") and s.get("enabled", True))
        msg = (
            f"📈 <b>بيانات واستهلاك السيرفر الحية ({MY_NAME}):</b>\n\n"
            f"🖥️ <b>المعالج (CPU):</b> <code>{cpu}%</code>\n"
            f"🧠 <b>الرامات (RAM):</b> <code>{ram.percent}%</code> ({ram.used // (1024*1024)}MB / {ram.total // (1024*1024)}MB)\n"
            f"🔴 <b>البثوث الحية الآن:</b> <code>{live_count}</code>\n"
            f"⏱️ <b>مدة عمل السيرفر:</b> <code>{hours}h {mins}m</code>\n\n"
            f"🌐 <b>سرعة نقل البيانات الحية:</b>\n"
            f"• ⬆️ <b>سرعة الرفع (Upload):</b> <code>{up_speed_kb:.1f} KB/s</code> (<code>{up_speed_mbps:.2f} Mbps</code>)\n"
            f"• ⬇️ <b>سرعة السحب (Download):</b> <code>{down_speed_kb:.1f} KB/s</code> (<code>{down_speed_mbps:.2f} Mbps</code>)\n\n"
            f"📦 <b>إجمالي البيانات المنقولة:</b>\n"
            f"• 📤 <b>إجمالي الرفع:</b> <code>{total_sent_mb:.1f} MB</code>\n"
            f"• 📥 <b>إجمالي السحب:</b> <code>{total_recv_mb:.1f} MB</code>"
        )
        bot.send_message(chat_id, msg, reply_markup=main_keyboard())
        return

    elif text == "💡 سعة وتحمل السيرفر":
        msg = (
            f"💡 <b>تقرير سعة وتحمل السيرفر ({MY_NAME}):</b>\n\n"
            f"• <b>مواصفات السيرفر:</b> 2 vCPUs | 7 GB RAM\n"
            f"• <b>بثوث YouTube (كود شاكر المعتمد):</b> نسخ مباشر بدون استهلاك معالج (0% CPU). السيرفر يتحمل حتى <b>10 إلى 15 بث متزامن</b> بأمان تام.\n"
            f"• <b>بثوث Meta (Facebook & Instagram):</b> تشفير 2500k CBR فائق الثبات بـ <code>superfast</code> وأولوية <code>nice -n 10</code>. يتحمل السيرفر <b>2 إلى 3 بثوث متزامنة</b> مع بقاء بثوث يوتيوب في أمان كامل 100% دون أن تتأثر نهائياً.\n"
            f"• <b>استهلاك الرامات:</b> كل بث يستهلك ~150-200 ميجا؛ رامات 7 جيجا تضمن عدم امتلاء الذاكرة إطلاقاً.\n"
        )
        bot.send_message(chat_id, msg, reply_markup=main_keyboard())
        return

    elif text == "📜 سجل التشغيل (Logs)":
        if os.path.exists(RESTREAMER_LOG):
            try:
                res = subprocess.run(["tail", "-n", "30", RESTREAMER_LOG], capture_output=True, text=True)
                logs_txt = res.stdout.strip() or "السجل فارغ حالياً."
            except Exception as e:
                logs_txt = f"تعذر قراءة السجل: {e}"
        else:
            logs_txt = "ملف السجل غير موجود بعد."
        escaped_logs = telebot.formatting.escape_html(logs_txt[-3500:])
        bot.send_message(chat_id, f"📜 <b>آخر 30 سطر من سجل المحرك:</b>\n\n<pre>{escaped_logs}</pre>", reply_markup=main_keyboard())
        return

    elif text == "📋 قائمة وإدارة البثوث":
        streams = load_streams()
        if not streams:
            bot.send_message(chat_id, "ℹ️ لا توجد أي بثوث مضافة. اضغط على <b>➕ إضافة بث جديد</b>.", reply_markup=main_keyboard())
            return
        for idx, s in enumerate(streams):
            msg, markup = build_stream_card(idx, s)
            bot.send_message(chat_id, msg, reply_markup=markup)
        return

    elif text == "🔄 إعادة تشغيل البثوث":
        with open("/tmp/restart_all_streams", "w") as f:
            f.write(str(time.time()))
        bot.send_message(chat_id, "🔄 تم إرسال أمر إعادة تشغيل آمن وسلس لكل البثوث للمحرك.", reply_markup=main_keyboard())
        return

    elif text == "➕ إضافة بث جديد":
        USER_STATES[chat_id] = {"step": "AWAIT_TIKTOK_USER"}
        bot.send_message(
            chat_id,
            "📌 <b>أرسل اسم حساب تيك توك:</b>\n(مثال: <code>c.ahmed.h</code> أو رابط الحساب)",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        return

    if state == "AWAIT_NEW_KEY":
        new_key = text.strip()
        target_sid = USER_STATES[chat_id].get("stream_id")
        streams = load_streams()
        idx = find_stream_idx(target_sid, streams)
        if idx != -1:
            s = streams[idx]
            dup_user = None
            for other_idx, other_s in enumerate(streams):
                if other_idx != idx and other_s.get("enabled", True):
                    if (other_s.get("stream_key") and other_s.get("stream_key") == new_key) or (
                        other_s.get("target_url") and new_key in other_s.get("target_url")
                    ):
                        dup_user = other_s.get("tiktok_user")
                        break
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
            key_preview = s["stream_key"][:6] + "..." if s.get("stream_key") else "مخصص"
            warn_msg = (
                f"\n\n⚠️ <b>تنبيه هام جداً:</b> هذا المفتاح مستخدم حالياً لحساب آخر (<b>@{dup_user}</b>)!"
                if dup_user
                else ""
            )
            bot.send_message(
                chat_id,
                f"✅ <b>تم تحديث مفتاح البث بنجاح!</b>\n\n"
                f"• <b>الحساب:</b> @{s['tiktok_user']}\n"
                f"• <b>المنصة:</b> {s['platform']}\n"
                f"• <b>المفتاح الجديد:</b> <code>{key_preview}</code>\n\n"
                f"🔄 تم تطبيق التغيير فوراً على المحرك دون مقاطعة باقي البثوث.{warn_msg}",
                reply_markup=main_keyboard(),
            )
        else:
            USER_STATES.pop(chat_id, None)
            bot.send_message(chat_id, "⚠️ حدث خطأ أو لم يتم العثور على البث.", reply_markup=main_keyboard())
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
        bot.send_message(chat_id, f"تم تحديد الحساب: <b>@{username}</b>\n\nاختر المنصة المستهدفة للبث:", reply_markup=markup)
        return

    elif state == "AWAIT_STREAM_KEY":
        data = USER_STATES[chat_id]
        stream_key = text.strip()
        server_url = data["server_url"]
        target_url = f"{server_url.rstrip('/')}/{stream_key.lstrip('/')}"
        streams = load_streams()
        dup_user = None
        for other_s in streams:
            if other_s.get("enabled", True):
                if (other_s.get("stream_key") and other_s.get("stream_key") == stream_key) or (
                    other_s.get("target_url") and stream_key in other_s.get("target_url")
                ):
                    dup_user = other_s.get("tiktok_user")
                    break
        if dup_user and dup_user != data.get("tiktok_user"):
            bot.send_message(
                chat_id,
                f"⚠️ <b>تحذير هام:</b> هذا المفتاح مستخدم حالياً لحساب آخر (<b>@{dup_user}</b>)!\n"
                f"ممنوع استخدام نفس المفتاح لبثين في نفس الوقت لأن المنصة ستطردهما معاً.",
            )
        data["stream_key"] = stream_key
        data["target_url"] = target_url
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
    code = call.data.split("_")[1]
    if chat_id not in USER_STATES:
        bot.send_message(chat_id, "انتهت الجلسة، ابدأ من جديد بالضغط على إضافة بث.")
        return
    if code == "fb":
        USER_STATES[chat_id].update({"step": "AWAIT_STREAM_KEY", "platform": "Facebook", "server_url": "rtmps://live-api-s.facebook.com:443/rtmp/"})
        bot.edit_message_text("🔵 <b>تم اختيار Facebook Live</b>\n\n🔑 أرسل الآن <b>مفتاح البث (Stream Key)</b>:", chat_id, call.message.message_id)
    elif code == "ig":
        USER_STATES[chat_id].update({"step": "AWAIT_STREAM_KEY", "platform": "Instagram", "server_url": "rtmps://live-upload.instagram.com:443/rtmp/"})
        bot.edit_message_text("🟣 <b>تم اختيار Instagram Live</b>\n\n🔑 أرسل الآن <b>مفتاح البث (Stream Key)</b>:", chat_id, call.message.message_id)
    elif code == "yt":
        USER_STATES[chat_id].update({"step": "AWAIT_STREAM_KEY", "platform": "YouTube", "server_url": "rtmp://a.rtmp.youtube.com/live2/"})
        bot.edit_message_text("🔴 <b>تم اختيار YouTube Live</b>\n\n🔑 أرسل الآن <b>مفتاح البث (Stream Key)</b>:", chat_id, call.message.message_id)
    elif code == "custom":
        USER_STATES[chat_id].update({"step": "AWAIT_CUSTOM_URL", "platform": "Custom RTMP"})
        bot.edit_message_text("🌐 أرسل الآن رابط RTMP كاملاً مع المفتاح:", chat_id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data in ["orient_portrait", "orient_landscape"])
def handle_orient(call):
    if not is_admin(call.from_user):
        return
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        bot.send_message(chat_id, "انتهت الجلسة، ابدأ من جديد بالضغط على إضافة بث.")
        return
    data = USER_STATES[chat_id]
    if call.data == "orient_portrait":
        if data.get("platform") == "YouTube":
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("📱 طولي أصلي (نسخ مباشر كود شاكر - 0% معالج)", callback_data="port_copy"),
                types.InlineKeyboardButton("⚡ طولي فائق الثبات (30fps & 2500k - للشاشات الثابتة)", callback_data="port_stable"),
            )
            bot.edit_message_text("📐 <b>اختر نمط البث الطولي ليوتيوب:</b>", chat_id, call.message.message_id, reply_markup=markup)
            return
        data["layout"] = "portrait_copy"
        finalize_add_stream(chat_id, call.message.message_id, data)
    else:
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("🎬 نعم، طولي بشاشة سوداء وتكبير 80%", callback_data="zoom_yes"),
            types.InlineKeyboardButton("🖥️ لا، انقله بالعرض كامل كما هو", callback_data="zoom_no"),
        )
        bot.edit_message_text(
            "🎬 <b>تحب نخليه بالطول على شاشة سوداء ونكبّره بنسبة 80%؟</b>",
            chat_id,
            call.message.message_id,
            reply_markup=markup,
        )


@bot.callback_query_handler(func=lambda call: call.data in ["port_copy", "port_stable"])
def handle_portrait_type(call):
    if not is_admin(call.from_user):
        return
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        bot.send_message(chat_id, "انتهت الجلسة، ابدأ من جديد بالضغط على إضافة بث.")
        return
    data = USER_STATES[chat_id]
    data["layout"] = "portrait_stable" if call.data == "port_stable" else "portrait_copy"
    finalize_add_stream(chat_id, call.message.message_id, data)


@bot.callback_query_handler(func=lambda call: call.data in ["zoom_yes", "zoom_no"])
def handle_zoom_decision(call):
    if not is_admin(call.from_user):
        return
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        bot.send_message(chat_id, "انتهت الجلسة، ابدأ من جديد بالضغط على إضافة بث.")
        return
    data = USER_STATES[chat_id]
    data["layout"] = "landscape_to_portrait_pad" if call.data == "zoom_yes" else "landscape_raw"
    finalize_add_stream(chat_id, call.message.message_id, data)


def finalize_add_stream(chat_id: int, message_id: int, data: dict):
    streams = load_streams()
    short_uid = uuid.uuid4().hex[:8]
    streams.append({
        "id": short_uid,
        "tiktok_user": data["tiktok_user"],
        "platform": data["platform"],
        "server_url": data.get("server_url", ""),
        "stream_key": data.get("stream_key", ""),
        "target_url": data["target_url"],
        "layout": data.get("layout", "portrait_copy"),
        "zoom_pct": data.get("zoom_pct", 80),
        "enabled": True,
        "is_live": False,
    })
    save_streams(streams)
    USER_STATES.pop(chat_id, None)
    zoom_val = data.get("zoom_pct", 80)
    layout_str = get_layout_title(data.get("layout", "portrait_copy"), zoom_val)
    bot.edit_message_text(
        f"🎉 <b>تم حفظ البث وبدء مراقبته بنجاح!</b>\n\n"
        f"• <b>تيك توك:</b> <code>@{data['tiktok_user']}</code>\n"
        f"• <b>المنصة:</b> {data['platform']}\n"
        f"• <b>نمط العرض:</b> {layout_str}\n\n"
        f"🚀 سيصلك إشعار فوري لحظة فتح البث.",
        chat_id,
        message_id,
    )
    bot.send_message(chat_id, "جاهز ومستعد للمراقبة! يمكنك إدارة البثوث عبر الأزرار:", reply_markup=main_keyboard())


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
    zoom_val = streams[idx].get("zoom_pct", 80)
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📱 طولي أصلي (نسخ مباشر كود شاكر)", callback_data=f"setlay_{sid}_portrait_copy"),
        types.InlineKeyboardButton("⚡ طولي فائق الثبات (30fps & 2500k)", callback_data=f"setlay_{sid}_portrait_stable"),
        types.InlineKeyboardButton(f"🎬 طولي بشاشة سوداء وتكبير ({zoom_val}%)", callback_data=f"setlay_{sid}_landscape_to_portrait_pad"),
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
        bot.answer_callback_query(call.id, "نمط غير معروف")
        return
    streams = load_streams()
    idx = find_stream_idx(target_str, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    streams[idx]["layout"] = matched_layout
    if "zoom_pct" not in streams[idx]:
        streams[idx]["zoom_pct"] = 80
    save_streams(streams)
    sid = streams[idx]["id"]
    zoom_val = streams[idx].get("zoom_pct", 80)
    bot.answer_callback_query(call.id, "✅ تم تغيير المقاس بنجاح!")
    markup = types.InlineKeyboardMarkup()
    if matched_layout == "landscape_to_portrait_pad":
        markup.add(types.InlineKeyboardButton(f"🔍 ضبط نسبة التكبير الآن ({zoom_val}%)", callback_data=f"zoomctrl_{sid}"))
    bot.edit_message_text(
        f"✅ تم تحديث نمط العرض لـ @{streams[idx]['tiktok_user']} إلى:\n<b>{get_layout_title(matched_layout, zoom_val)}</b>",
        call.message.chat.id,
        call.message.message_id,
        reply_markup=markup if matched_layout == "landscape_to_portrait_pad" else None,
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("zoomctrl_"))
def handle_zoom_control(call):
    if not is_admin(call.from_user):
        return
    target = call.data[len("zoomctrl_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث.")
        return
    bot.answer_callback_query(call.id)
    s = streams[idx]
    current_zoom = s.get("zoom_pct", 80)
    markup = build_zoom_markup(s["id"], current_zoom)
    txt = (
        f"🔍 <b>لوحة التحكم المباشر في حجم العرض (Live Zoom)</b>\n\n"
        f"• <b>الحساب:</b> @{s['tiktok_user']}\n"
        f"• <b>النسبة الحالية:</b> <b>{current_zoom}%</b>\n\n"
        f"💡 <i>يمكنك الضغط على +5% أو -5% لتكبير أو تصغير الفيديو فوراً أثناء البث!</i>"
    )
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("setz_"))
def handle_set_zoom(call):
    if not is_admin(call.from_user):
        return
    raw = call.data[len("setz_"):]
    if "_" not in raw:
        bot.answer_callback_query(call.id, "خطأ في البيانات")
        return
    target_str, pct_str = raw.rsplit("_", 1)
    try:
        new_pct = max(50, min(200, int(pct_str)))
    except ValueError:
        bot.answer_callback_query(call.id, "قيمة غير صالحة")
        return
    streams = load_streams()
    idx = find_stream_idx(target_str, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    s = streams[idx]
    if s.get("zoom_pct", 80) == new_pct:
        bot.answer_callback_query(call.id, f"النسبة هي {new_pct}% بالفعل")
        return
    s["zoom_pct"] = new_pct
    save_streams(streams)
    bot.answer_callback_query(call.id, f"🎯 تم ضبط الحجم إلى {new_pct}%")
    markup = build_zoom_markup(s["id"], new_pct)
    txt = (
        f"🔍 <b>لوحة التحكم المباشر في حجم العرض (Live Zoom)</b>\n\n"
        f"• <b>الحساب:</b> @{s['tiktok_user']}\n"
        f"• <b>النسبة الحالية:</b> <b>{new_pct}%</b>\n\n"
        f"⚡ <i>تم إرسال أمر التكبير للمحرك؛ سيعاد ضبط المقاس تلقائياً دون تأثر باقي البثوث!</i>"
    )
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("zoomback_"))
def handle_zoom_back(call):
    if not is_admin(call.from_user):
        return
    target = call.data[len("zoomback_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    bot.answer_callback_query(call.id)
    msg, markup = build_stream_card(idx, streams[idx])
    bot.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("chkey_"))
def handle_change_key(call):
    if not is_admin(call.from_user):
        return
    chat_id = call.message.chat.id
    target = call.data[len("chkey_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    bot.answer_callback_query(call.id)
    s = streams[idx]
    USER_STATES[chat_id] = {"step": "AWAIT_NEW_KEY", "stream_id": s["id"]}
    prompt_txt = (
        f"🔑 <b>أرسل الآن الرابط الجديد بالكامل لـ @{s['tiktok_user']}:</b>"
        if s.get("platform") == "Custom RTMP"
        else f"🔑 <b>أرسل الآن مفتاح البث الجديد لحساب @{s['tiktok_user']} على {s['platform']}:</b>"
    )
    bot.send_message(chat_id, prompt_txt, reply_markup=types.ReplyKeyboardRemove())


@bot.callback_query_handler(func=lambda call: call.data.startswith("pause_"))
def handle_pause(call):
    if not is_admin(call.from_user):
        return
    target = call.data[len("pause_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    streams[idx]["enabled"] = False
    save_streams(streams)
    bot.answer_callback_query(call.id, "⏸️ تم إيقاف البث مؤقتاً")
    bot.edit_message_text(f"⏸️ تم إيقاف بث @{streams[idx]['tiktok_user']} مؤقتاً.", call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("resume_"))
def handle_resume(call):
    if not is_admin(call.from_user):
        return
    target = call.data[len("resume_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    streams[idx]["enabled"] = True
    save_streams(streams)
    bot.answer_callback_query(call.id, "▶️ تم استئناف تشغيل البث")
    bot.edit_message_text(f"▶️ تم تفعيل وتشغيل بث @{streams[idx]['tiktok_user']}.", call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("del_"))
def handle_delete(call):
    if not is_admin(call.from_user):
        return
    target = call.data[len("del_"):]
    streams = load_streams()
    idx = find_stream_idx(target, streams)
    if idx == -1:
        bot.answer_callback_query(call.id, "⚠️ لم يتم العثور على البث")
        return
    deleted = streams.pop(idx)
    save_streams(streams)
    bot.answer_callback_query(call.id, "🗑️ تم الحذف بنجاح!")
    bot.edit_message_text(f"🗑️ تم حذف بث حساب: @{deleted['tiktok_user']}", call.message.chat.id, call.message.message_id)


if __name__ == "__main__":
    print(f"[BOT] Starting Telegram Bot on {MY_NAME}...", flush=True)
    while True:
        try:
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception:
            time.sleep(3)
