#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Stream Management & Monitoring Telegram Bot
Auth: Via Admin Telegram Username (No password required)
Features:
- Granular stream control (Pause/Resume/Delete/Change Layout/Change Stream Key).
- Supports Portrait Copy, Landscape to Portrait (80% zoom & black canvas), and Landscape Raw.
- Real-time live upload & download speed + Total data transfer.
- Server capacity & stats.
- Live logs viewer.
- Anti-duplicate messaging.
"""

import json
import os
import re
import subprocess
import time
import psutil
import telebot
from telebot import types

BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "").lstrip("@").lower().strip()
MY_NAME = os.environ.get("MY_NAME", "Runner")

DATA_DIR = os.path.abspath("data")
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
RESTREAMER_LOG = os.path.join(DATA_DIR, "restreamer.log")

os.makedirs(DATA_DIR, exist_ok=True)

if not BOT_TOKEN:
    print("[BOT] ERROR: BOT_TOKEN is missing!", flush=True)
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# --- إدارة الصلاحيات والبيانات ---

def is_admin(user) -> bool:
    if not ADMIN_USERNAME:
        return True
    return bool(user.username and user.username.lower() == ADMIN_USERNAME)

def load_streams() -> list:
    if os.path.exists(STREAMS_PATH):
        try:
            with open(STREAMS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_streams(streams: list):
    with open(STREAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(streams, f, indent=2, ensure_ascii=False)
    with open("/tmp/reload_restreamer", "w") as f:
        f.write(str(time.time()))

def save_chat_id(chat_id: int):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump({"chat_id": chat_id}, f)

def get_layout_title(layout_code: str) -> str:
    if layout_code == "landscape_to_portrait_pad":
        return "🎬 طولي بشاشة سوداء وتكبير 80%"
    elif layout_code == "landscape_raw":
        return "🖥️ عرضي كامل كما هو"
    else:
        return "📱 طولي أصلي (نسخ مباشر)"

USER_STATES = {}

def main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("➕ إضافة بث جديد"),
        types.KeyboardButton("📋 قائمة وإدارة البثوث"),
        types.KeyboardButton("📊 إحصائيات البثوث"),
        types.KeyboardButton("📈 استهلاك وسرعة السيرفر"),
        types.KeyboardButton("💡 سعة وتحمل السيرفر"),
        types.KeyboardButton("📜 سجل التشغيل (Logs)"),
        types.KeyboardButton("🔄 إعادة تشغيل البثوث")
    )
    return markup

# --- معالجة الأوامر ---

@bot.message_handler(commands=['start'])
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
        reply_markup=main_keyboard()
    )

@bot.message_handler(func=lambda msg: True)
def handle_text(message):
    if not is_admin(message.from_user):
        bot.send_message(message.chat.id, "⛔ <b>عذراً، هذا البوت مخصص لمدير السيرفر فقط.</b>")
        return

    chat_id = message.chat.id
    text = message.text.strip()
    save_chat_id(chat_id)
    state = USER_STATES.get(chat_id, {}).get("step")

    # 1. إحصائيات البثوث
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

    # 2. استهلاك وسرعة السيرفر الفعلية اللحظية + الإجمالي
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
        minutes = (uptime_sec % 3600) // 60

        res = (
            f"<b>📈 استهلاك وموارد السيرفر (<code>{MY_NAME}</code>):</b>\n\n"
            f"⚙️ <b>المعالج (CPU):</b> {cpu}%\n"
            f"🧠 <b>الرام (RAM):</b> {ram.percent}% (مستخدم {ram.used // (1024*1024)}MB من أصل {ram.total // (1024*1024)}MB)\n\n"
            f"🚀 <b>السرعة اللحظية الفعلية (Live Speed):</b>\n"
            f"   ⬆️ <b>سرعة الرفع:</b> <code>{up_speed_mbps:.2f} Mbps</code> ({up_speed_kb:.1f} KB/s)\n"
            f"   ⬇️ <b>سرعة التحميل:</b> <code>{down_speed_mbps:.2f} Mbps</code> ({down_speed_kb:.1f} KB/s)\n\n"
            f"🌐 <b>إجمالي استهلاك البيانات (Total Transfer):</b>\n"
            f"   ⬆️ رفع: {total_sent_mb:.1f} MB\n"
            f"   ⬇️ تحميل: {total_recv_mb:.1f} MB\n\n"
            f"⏱️ <b>مدة عمل السيرفر:</b> {hours} ساعة و {minutes} دقيقة\n"
        )
        bot.send_message(chat_id, res, reply_markup=main_keyboard())
        return

    # 3. سعة وتحمل السيرفر
    elif text == "💡 سعة وتحمل السيرفر":
        ram = psutil.virtual_memory()
        free_ram_mb = ram.available // (1024 * 1024)
        streams = load_streams()
        current_streams = len(streams)
        estimated_more = free_ram_mb // 200

        res = (
            f"<b>💡 تقرير قدرة وتحمل السيرفر (<code>{MY_NAME}</code>):</b>\n\n"
            f"🖥️ <b>المواصفات:</b> 2 vCPUs | 7 GB RAM | 1Gbps Network\n"
            f"📊 <b>البثوث الحالية:</b> {current_streams} بثوث\n"
            f"🧠 <b>الرام المتبقية الحرة:</b> {free_ram_mb} MB\n\n"
            f"🚀 <b>السعة التقديرية:</b>\n"
            f"• البثوث الطولية (Copy): تستهلك ~180MB رام فقط و ~1% معالج.\n"
            f"• البثوث المحولة (تعديل الأبعاد والتكبير): تستهلك ~8% معالج فقط بفضل تقنية ultrafast.\n\n"
            f"✅ <b>يمكنك إضافة حتى {estimated_more} بثوث إضافية</b> بأمان تام!"
        )
        bot.send_message(chat_id, res, reply_markup=main_keyboard())
        return

    # 4. سجل التشغيل (Logs)
    elif text == "📜 سجل التشغيل (Logs)":
        if os.path.exists(RESTREAMER_LOG):
            try:
                with open(RESTREAMER_LOG, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                last_lines = "".join(lines[-25:]) if lines else "السجل فارغ حالياً."
                bot.send_message(
                    chat_id,
                    f"<b>📜 آخر أحداث التشغيل (Restreamer Log):</b>\n<pre>{last_lines[-3800:]}</pre>",
                    reply_markup=main_keyboard()
                )
            except Exception as e:
                bot.send_message(chat_id, f"تعذر قراءة السجل: {e}")
        else:
            bot.send_message(chat_id, "ℹ️ ملف السجل غير موجود حالياً.", reply_markup=main_keyboard())
        return

    # 5. قائمة وإدارة البثوث (تشغيل / إيقاف / تغيير المقاس / تغيير المفتاح / حذف)
    elif text == "📋 قائمة وإدارة البثوث":
        streams = load_streams()
        if not streams:
            bot.send_message(chat_id, "ℹ️ لا توجد أي بثوث مضافة. اضغط على <b>➕ إضافة بث جديد</b>.", reply_markup=main_keyboard())
            return

        for idx, s in enumerate(streams):
            is_enabled = s.get("enabled", True)
            if not is_enabled:
                status_txt = "⏸️ متوقف مؤقتاً"
            elif s.get("is_live"):
                status_txt = "🔴 لايف شغال حالياً"
            else:
                status_txt = "⚪ في الانتظار (أوفلاين)"

            layout_str = get_layout_title(s.get("layout", "portrait_copy"))
            key_preview = s['stream_key'][:6] + "..." if s.get('stream_key') else "مخصص"
            msg = (
                f"<b>#{idx+1} - @{s['tiktok_user']}</b>\n"
                f"• <b>المنصة:</b> {s['platform']}\n"
                f"• <b>نمط العرض:</b> {layout_str}\n"
                f"• <b>المفتاح:</b> <code>{key_preview}</code>\n"
                f"• <b>الحالة:</b> {status_txt}\n"
            )
            markup = types.InlineKeyboardMarkup(row_width=2)
            toggle_btn = types.InlineKeyboardButton("⏸️ إيقاف مؤقت", callback_data=f"pause_{idx}") if is_enabled else types.InlineKeyboardButton("▶️ تشغيل البث", callback_data=f"resume_{idx}")
            layout_btn = types.InlineKeyboardButton("📐 تغيير المقاس", callback_data=f"chlayout_{idx}")
            key_btn = types.InlineKeyboardButton("🔑 تغيير المفتاح", callback_data=f"chkey_{idx}")
            del_btn = types.InlineKeyboardButton("🗑️ حذف", callback_data=f"del_{idx}")
            markup.add(toggle_btn, layout_btn)
            markup.add(key_btn, del_btn)

            bot.send_message(chat_id, msg, reply_markup=markup)
        return

    # 6. إعادة تشغيل البثوث
    elif text == "🔄 إعادة تشغيل البثوث":
        subprocess.run(["pkill", "-f", "ffmpeg"], check=False)
        bot.send_message(chat_id, "🔄 تم إرسال أمر إعادة تشغيل البثوث فوراً.", reply_markup=main_keyboard())
        return

    # 7. إضافة بث جديد
    elif text == "➕ إضافة بث جديد":
        USER_STATES[chat_id] = {"step": "AWAIT_TIKTOK_USER"}
        bot.send_message(
            chat_id,
            "📌 <b>أرسل اسم حساب تيك توك:</b>\n(مثال: <code>c.ahmed.h</code> أو رابط الحساب)",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return

    # تغيير مفتاح البث (Stream Key) لبث موجود
    if state == "AWAIT_NEW_KEY":
        new_key = text.strip()
        idx = USER_STATES[chat_id].get("stream_idx")
        streams = load_streams()
        if idx is not None and 0 <= idx < len(streams):
            s = streams[idx]
            if s.get("platform") == "Custom RTMP":
                s["target_url"] = new_key
                s["server_url"] = new_key
                s["stream_key"] = ""
            else:
                s["stream_key"] = new_key
                server_url = s.get("server_url", "")
                if server_url:
                    s["target_url"] = f"{server_url.rstrip('/')}/{new_key.lstrip('/')}"
                else:
                    s["target_url"] = new_key
            save_streams(streams)
            USER_STATES.pop(chat_id, None)

            key_preview = s['stream_key'][:6] + "..." if s.get('stream_key') else "مخصص"
            bot.send_message(
                chat_id,
                f"✅ <b>تم تحديث مفتاح البث بنجاح!</b>\n\n"
                f"• <b>الحساب:</b> @{s['tiktok_user']}\n"
                f"• <b>المنصة:</b> {s['platform']}\n"
                f"• <b>المفتاح الجديد:</b> <code>{key_preview}</code>\n\n"
                f"🔄 تم تطبيق التغيير فوراً على المحرك.",
                reply_markup=main_keyboard()
            )
        else:
            USER_STATES.pop(chat_id, None)
            bot.send_message(chat_id, "⚠️ حدث خطأ أو لم يتم العثور على البث.", reply_markup=main_keyboard())
        return

    # خطوات الإضافة (Wizard)
    if state == "AWAIT_TIKTOK_USER":
        match = re.search(r"tiktok\.com/@([^/?#]+)", text)
        username = match.group(1) if match else text.lstrip("@").strip()

        USER_STATES[chat_id] = {
            "step": "CHOOSE_PLATFORM",
            "tiktok_user": username
        }

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔵 Facebook Live", callback_data="plat_fb"),
            types.InlineKeyboardButton("🟣 Instagram Live", callback_data="plat_ig"),
            types.InlineKeyboardButton("🔴 YouTube Live", callback_data="plat_yt"),
            types.InlineKeyboardButton("🌐 Custom RTMP", callback_data="plat_custom")
        )
        bot.send_message(
            chat_id,
            f"تم تحديد الحساب: <b>@{username}</b>\n\nاختر المنصة المستهدفة للبث:",
            reply_markup=markup
        )
        return

    elif state == "AWAIT_STREAM_KEY":
        data = USER_STATES[chat_id]
        stream_key = text.strip()
        server_url = data["server_url"]
        target_url = f"{server_url.rstrip('/')}/{stream_key.lstrip('/')}"

        data["stream_key"] = stream_key
        data["target_url"] = target_url
        data["step"] = "CHOOSE_ORIENTATION"

        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📱 بالطول (افتراضي)", callback_data="orient_portrait"),
            types.InlineKeyboardButton("🖥️ بالعرض", callback_data="orient_landscape")
        )
        bot.send_message(
            chat_id,
            "📐 <b>هل البث الأساسي على تيك توك بالطول ولا بالعرض؟</b>",
            reply_markup=markup
        )
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
            types.InlineKeyboardButton("🖥️ بالعرض", callback_data="orient_landscape")
        )
        bot.send_message(
            chat_id,
            "📐 <b>هل البث الأساسي على تيك توك بالطول ولا بالعرض؟</b>",
            reply_markup=markup
        )
        return

# --- Callbacks ---

@bot.callback_query_handler(func=lambda call: call.data.startswith("plat_"))
def handle_plat(call):
    chat_id = call.message.chat.id
    code = call.data.split("_")[1]

    if chat_id not in USER_STATES:
        bot.send_message(chat_id, "انتهت الجلسة، ابدأ من جديد بالضغط على إضافة بث.")
        return

    if code == "fb":
        USER_STATES[chat_id].update({
            "step": "AWAIT_STREAM_KEY",
            "platform": "Facebook",
            "server_url": "rtmps://live-api-s.facebook.com:443/rtmp/"
        })
        bot.edit_message_text(
            "🔵 <b>تم اختيار Facebook Live</b>\n"
            "الرابط الافتراضي: <code>rtmps://live-api-s.facebook.com:443/rtmp/</code>\n\n"
            "🔑 أرسل الآن <b>مفتاح البث (Stream Key)</b> الخاص بفيسبوك:",
            chat_id, call.message.message_id
        )
    elif code == "ig":
        USER_STATES[chat_id].update({
            "step": "AWAIT_STREAM_KEY",
            "platform": "Instagram",
            "server_url": "rtmps://edgetee-upload-hbe1-2.xx.fbcdn.net:443/rtmp/"
        })
        bot.edit_message_text(
            "🟣 <b>تم اختيار Instagram Live</b>\n"
            "الرابط الافتراضي: <code>rtmps://edgetee-upload-hbe1-2.xx.fbcdn.net:443/rtmp/</code>\n\n"
            "🔑 أرسل الآن <b>مفتاح البث (Stream Key)</b> الخاص بإنستغرام:",
            chat_id, call.message.message_id
        )
    elif code == "yt":
        USER_STATES[chat_id].update({
            "step": "AWAIT_STREAM_KEY",
            "platform": "YouTube",
            "server_url": "rtmp://a.rtmp.youtube.com/live2/"
        })
        bot.edit_message_text(
            "🔴 <b>تم اختيار YouTube Live</b>\n"
            "الرابط الافتراضي: <code>rtmp://a.rtmp.youtube.com/live2/</code>\n\n"
            "🔑 أرسل الآن <b>مفتاح البث (Stream Key)</b> الخاص بيوتيوب:",
            chat_id, call.message.message_id
        )
    elif code == "custom":
        USER_STATES[chat_id].update({"step": "AWAIT_CUSTOM_URL", "platform": "Custom RTMP"})
        bot.edit_message_text(
            "🌐 أرسل الآن رابط RTMP كاملاً مع المفتاح:",
            chat_id, call.message.message_id
        )

# معالجة اختيار اتجاه البث (طول / عرض)
@bot.callback_query_handler(func=lambda call: call.data in ["orient_portrait", "orient_landscape"])
def handle_orientation(call):
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        bot.send_message(chat_id, "انتهت الجلسة، ابدأ من جديد بالضغط على إضافة بث.")
        return

    data = USER_STATES[chat_id]

    if call.data == "orient_portrait":
        data["layout"] = "portrait_copy"
        finalize_add_stream(chat_id, call.message.message_id, data)
    else:
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("🎬 نعم، طولي بشاشة سوداء وتكبير 80%", callback_data="zoom_yes"),
            types.InlineKeyboardButton("🖥️ لا، انقله بالعرض كامل كما هو", callback_data="zoom_no")
        )
        bot.edit_message_text(
            "🎬 <b>تحب نخليه بالطول على شاشة سوداء ونكبّره بنسبة 80% (زي التيك توك)؟</b>\n\n"
            "<i>(الفيديو هيكون في المنتصف أو فوق النص بسنة بسيطة وتحته وفوقه مساحة سوداء مريحة للعين)</i>",
            chat_id, call.message.message_id,
            reply_markup=markup
        )

# معالجة قرار التكبير والشاشة السوداء
@bot.callback_query_handler(func=lambda call: call.data in ["zoom_yes", "zoom_no"])
def handle_zoom_decision(call):
    chat_id = call.message.chat.id
    if chat_id not in USER_STATES:
        bot.send_message(chat_id, "انتهت الجلسة، ابدأ من جديد بالضغط على إضافة بث.")
        return

    data = USER_STATES[chat_id]
    if call.data == "zoom_yes":
        data["layout"] = "landscape_to_portrait_pad"
    else:
        data["layout"] = "landscape_raw"

    finalize_add_stream(chat_id, call.message.message_id, data)

def finalize_add_stream(chat_id: int, message_id: int, data: dict):
    streams = load_streams()
    streams.append({
        "tiktok_user": data["tiktok_user"],
        "platform": data["platform"],
        "server_url": data.get("server_url", ""),
        "stream_key": data.get("stream_key", ""),
        "target_url": data["target_url"],
        "layout": data.get("layout", "portrait_copy"),
        "enabled": True,
        "is_live": False
    })
    save_streams(streams)
    USER_STATES.pop(chat_id, None)

    layout_str = get_layout_title(data.get("layout", "portrait_copy"))
    bot.edit_message_text(
        f"🎉 <b>تم حفظ البث وبدء مراقبته بنجاح!</b>\n\n"
        f"• <b>تيك توك:</b> <code>@{data['tiktok_user']}</code>\n"
        f"• <b>المنصة:</b> {data['platform']}\n"
        f"• <b>نمط العرض:</b> {layout_str}\n\n"
        f"🚀 سيصلك إشعار فوري لحظة فتح البث.",
        chat_id, message_id
    )
    bot.send_message(chat_id, "جاهز ومستعد للمراقبة! يمكنك إدارة البثوث عبر الأزرار:", reply_markup=main_keyboard())

# تغيير المقاس للبثوث الموجودة مسبقاً
@bot.callback_query_handler(func=lambda call: call.data.startswith("chlayout_"))
def handle_change_layout(call):
    idx = int(call.data.split("_")[1])
    streams = load_streams()
    if 0 <= idx < len(streams):
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📱 طولي أصلي", callback_data=f"setlay_{idx}_portrait_copy"),
            types.InlineKeyboardButton("🎬 طولي بشاشة سوداء وتكبير 80%", callback_data=f"setlay_{idx}_landscape_to_portrait_pad"),
            types.InlineKeyboardButton("🖥️ عرضي كامل كما هو", callback_data=f"setlay_{idx}_landscape_raw")
        )
        bot.edit_message_text(
            f"📐 <b>اختر نمط العرض لحساب @{streams[idx]['tiktok_user']}:</b>",
            call.message.chat.id, call.message.message_id,
            reply_markup=markup
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith("setlay_"))
def handle_set_layout(call):
    parts = call.data.split("_")
    idx = int(parts[1])
    new_layout = "_".join(parts[2:])
    streams = load_streams()
    if 0 <= idx < len(streams):
        streams[idx]["layout"] = new_layout
        save_streams(streams)
        bot.answer_callback_query(call.id, "✅ تم تغيير المقاس بنجاح!")
        bot.edit_message_text(
            f"✅ تم تحديث نمط العرض لـ @{streams[idx]['tiktok_user']} إلى:\n<b>{get_layout_title(new_layout)}</b>",
            call.message.chat.id, call.message.message_id
        )

# تغيير مفتاح البث (Stream Key)
@bot.callback_query_handler(func=lambda call: call.data.startswith("chkey_"))
def handle_change_key(call):
    chat_id = call.message.chat.id
    idx = int(call.data.split("_")[1])
    streams = load_streams()
    if 0 <= idx < len(streams):
        s = streams[idx]
        USER_STATES[chat_id] = {"step": "AWAIT_NEW_KEY", "stream_idx": idx}
        if s.get("platform") == "Custom RTMP":
            bot.send_message(
                chat_id,
                f"🔑 <b>أرسل الآن الرابط الجديد بالكامل لـ @{s['tiktok_user']}:</b>\n(مثال: <code>rtmp://...</code>)",
                reply_markup=types.ReplyKeyboardRemove()
            )
        else:
            bot.send_message(
                chat_id,
                f"🔑 <b>أرسل الآن مفتاح البث (Stream Key) الجديد لحساب @{s['tiktok_user']} على {s['platform']}:</b>",
                reply_markup=types.ReplyKeyboardRemove()
            )

@bot.callback_query_handler(func=lambda call: call.data.startswith("pause_"))
def handle_pause(call):
    idx = int(call.data.split("_")[1])
    streams = load_streams()
    if 0 <= idx < len(streams):
        streams[idx]["enabled"] = False
        save_streams(streams)
        bot.answer_callback_query(call.id, "⏸️ تم إيقاف البث مؤقتاً")
        bot.edit_message_text(f"⏸️ تم إيقاف بث @{streams[idx]['tiktok_user']} مؤقتاً.", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("resume_"))
def handle_resume(call):
    idx = int(call.data.split("_")[1])
    streams = load_streams()
    if 0 <= idx < len(streams):
        streams[idx]["enabled"] = True
        save_streams(streams)
        bot.answer_callback_query(call.id, "▶️ تم استئناف تشغيل البث")
        bot.edit_message_text(f"▶️ تم تفعيل وتشغيل بث @{streams[idx]['tiktok_user']}.", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_"))
def handle_delete(call):
    idx = int(call.data.split("_")[1])
    streams = load_streams()
    if 0 <= idx < len(streams):
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
