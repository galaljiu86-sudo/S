#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Stream Management & Monitoring Telegram Bot
Auth: Via Admin Telegram Username (No password required)
Features: Granular stream control (Pause/Resume/Delete), Server capacity & stats, Live logs viewer.
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
    # إشعار الريستريمر بالتحديث
    with open("/tmp/reload_restreamer", "w") as f:
        f.write(str(time.time()))

def save_chat_id(chat_id: int):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump({"chat_id": chat_id}, f)

USER_STATES = {}

def main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add(
        types.KeyboardButton("➕ إضافة بث جديد"),
        types.KeyboardButton("📋 قائمة وإدارة البثوث"),
        types.KeyboardButton("📊 إحصائيات البثوث"),
        types.KeyboardButton("📜 سجل التشغيل (Logs)"),
        types.KeyboardButton("📈 استهلاك السيرفر والموارد"),
        types.KeyboardButton("💡 سعة وتحمل السيرفر"),
        types.KeyboardButton("🔄 إعادة تشغيل البثوث")
    )
    return markup

# --- الأوامر الرئيسية ---

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
        f"اختر من الأزرار بالأسفل لإدارة البثوث والسيرفر:",
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

    # 2. سجل التشغيل والأخطاء (Logs)
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

    # 3. استهلاك السيرفر والموارد
    elif text == "📈 استهلاك السيرفر والموارد":
        cpu = psutil.cpu_percent(interval=0.5)
        ram = psutil.virtual_memory()
        net = psutil.net_io_counters()
        uptime_sec = int(time.time() - psutil.boot_time())
        hours = uptime_sec // 3600
        minutes = (uptime_sec % 3600) // 60

        res = (
            f"<b>📈 استهلاك موارد السيرفر (<code>{MY_NAME}</code>):</b>\n\n"
            f"⚙️ <b>المعالج (CPU):</b> {cpu}%\n"
            f"🧠 <b>الرام (RAM):</b> {ram.percent}% (مستخدم {ram.used // (1024*1024)}MB من أصل {ram.total // (1024*1024)}MB)\n"
            f"🌐 <b>استهلاك الشبكة:</b>\n"
            f"   ⬆️ رفع: {net.bytes_sent / (1024*1024):.1f} MB\n"
            f"   ⬇️ تحميل: {net.bytes_recv / (1024*1024):.1f} MB\n"
            f"⏱️ <b>مدة العمل:</b> {hours} ساعة و {minutes} دقيقة\n"
        )
        bot.send_message(chat_id, res, reply_markup=main_keyboard())
        return

    # 4. سعة وتحمل السيرفر
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
            f"بما أن الفيديو يتم نسخه مباشرة بدون إعادة ترميز (Copy Codec)، يستهلك كل بث 180MB رام فقط و ~1% معالج.\n\n"
            f"✅ <b>يمكنك إضافة حتى {estimated_more} بثوث إضافية</b> فوق البثوث الحالية بأمان تام (السيرفر يتحمل إجمالاً من 15 إلى 25 بث متزامن)!"
        )
        bot.send_message(chat_id, res, reply_markup=main_keyboard())
        return

    # 5. قائمة وإدارة البثوث (تشغيل / إيقاف / حذف)
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
                status_txt = "⚪ في الانتظار (أوفلاين أو جاري الفحص)"

            key_preview = s['stream_key'][:6] + "..." if s.get('stream_key') else "مخصص"
            msg = (
                f"<b>#{idx+1} - @{s['tiktok_user']}</b>\n"
                f"• <b>المنصة:</b> {s['platform']}\n"
                f"• <b>المفتاح:</b> <code>{key_preview}</code>\n"
                f"• <b>الحالة:</b> {status_txt}\n"
            )
            markup = types.InlineKeyboardMarkup(row_width=2)
            toggle_btn = types.InlineKeyboardButton("⏸️ إيقاف مؤقت", callback_data=f"pause_{idx}") if is_enabled else types.InlineKeyboardButton("▶️ تشغيل البث", callback_data=f"resume_{idx}")
            del_btn = types.InlineKeyboardButton("🗑️ حذف", callback_data=f"del_{idx}")
            markup.add(toggle_btn, del_btn)

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
            "📌 <b>أرسل اسم حساب تيك توك:</b>\n(مثال: <code>d.shakertawfiqalaroury</code> أو رابط الحساب)",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return

    # معالجة خطوات الإضافة (Wizard)
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

        streams = load_streams()
        streams.append({
            "tiktok_user": data["tiktok_user"],
            "platform": data["platform"],
            "server_url": server_url,
            "stream_key": stream_key,
            "target_url": target_url,
            "enabled": True,
            "is_live": False
        })
        save_streams(streams)
        USER_STATES.pop(chat_id, None)

        bot.send_message(
            chat_id,
            f"🎉 <b>تم حفظ البث وبدء مراقبته بنجاح!</b>\n\n"
            f"• <b>تيك توك:</b> <code>@{data['tiktok_user']}</code>\n"
            f"• <b>المنصة:</b> {data['platform']}\n"
            f"• سيصلك إشعار فوري لحظة التقاط إشارة البث المباشر.",
            reply_markup=main_keyboard()
        )
        return

    elif state == "AWAIT_CUSTOM_URL":
        target_url = text.strip()
        data = USER_STATES[chat_id]
        streams = load_streams()
        streams.append({
            "tiktok_user": data["tiktok_user"],
            "platform": "Custom RTMP",
            "server_url": target_url,
            "stream_key": "",
            "target_url": target_url,
            "enabled": True,
            "is_live": False
        })
        save_streams(streams)
        USER_STATES.pop(chat_id, None)

        bot.send_message(
            chat_id,
            f"🎉 تم حفظ الرابط المخصص بنجاح:\n<code>{target_url}</code>",
            reply_markup=main_keyboard()
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
            "🔑 أرسل الآن <b>مفتاح البث الدائم (Permanent Stream Key)</b>:",
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
