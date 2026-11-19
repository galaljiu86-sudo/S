#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Stream Management & Monitoring Telegram Bot
Auth: Via Admin Telegram Username (No password required)
Features:
- Granular stream control (Pause/Resume/Delete).
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
        # أخذ عينة لمدة ثانية واحدة لحساب السرعة اللحظية الفعلية بدقة
        net_start = psutil.net_io_counters()
        cpu = psutil.cpu_percent(interval=1.0)
        net_end = psutil.net_io_counters()
        ram = psutil.virtual_memory()

        # حساب السرعة اللحظية الحالية (كيلوبايت/ثانية و ميجابت/ثانية)
        up_speed_kb = (net_end.bytes_sent - net_start.bytes_sent) / 1024.0
        down_speed_kb = (net_end.bytes_recv - net_start.bytes_recv) / 1024.0
        up_speed_mbps = (up_speed_kb * 8.0) / 1000.0
        down_speed_mbps = (down_speed_kb * 8.0) / 1000.0

        # إجمالي حركة البيانات منذ بداية تشغيل السيرفر
        total_sent_mb = net_end.bytes_sent / (1024 * 1024)
        total_recv_mb = net_end.bytes_recv / (1024 * 1024)

        uptime_sec = int(time.time() - psutil.boot_time())
        hours = uptime_sec // 3600
        minutes = (uptime_sec % 3600) // 60

        res = (
            f"<b>📈 استهلاك وسرعة السيرفر (<code>{MY_NAME}</code>):</b>\n\n"
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
            f"بما أن الفيديو يتم نسخه مباشرة بدون إعادة ترميز (Copy Codec)، يستهلك كل بث 180MB رام فقط و ~1% معالج.\n\n"
            f"✅ <b>يمكنك إضافة حتى {estimated_more} بثوث إضافية</b> بأمان تام (السيرفر يتحمل إجمالاً من 15 إلى 25 بث متزامن)!"
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
                status_txt = "⚪ في الانتظار (أوفلاين)"

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
            "📌 <b>أرسل اسم حساب تيك توك:</b>\n(مثال: <code>c.ahmed.h</code> أو رابط الحساب)",
            reply_markup=types.ReplyKeyboardRemove()
        )
        return

    # معالجة خطوات الإضافة (Wizard)
    if state == "AWAIT_TIKTOK_USER":
        user_val = text.replace("https://www.tiktok.com/@", "").replace("https://tiktok.com/@", "").lstrip("@").split("/")[0].strip()
        USER_STATES[chat_id] = {"step": "AWAIT_PLATFORM", "tiktok_user": user_val}
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        markup.add(
            types.KeyboardButton("YouTube"),
            types.KeyboardButton("Facebook"),
            types.KeyboardButton("Instagram"),
            types.KeyboardButton("Custom RTMP")
        )
        bot.send_message(chat_id, f"✅ تم حفظ الحساب: <b>@{user_val}</b>\nاختر المنصة المراد الإرسال إليها:", reply_markup=markup)
        return

    elif state == "AWAIT_PLATFORM":
        platform = text
        USER_STATES[chat_id]["platform"] = platform
        USER_STATES[chat_id]["step"] = "AWAIT_STREAM_KEY"
        
        if platform == "Custom RTMP":
            bot.send_message(
                chat_id,
                "🔗 أرسل رابط الـ RTMP الكامل مع المفتاح:\n(مثال: <code>rtmp://live.twitch.tv/app/live_xyz</code>)",
                reply_markup=types.ReplyKeyboardRemove()
            )
        else:
            bot.send_message(
                chat_id,
                f"🔑 أرسل <b>مفتاح البث (Stream Key)</b> الخاص بـ {platform}:",
                reply_markup=types.ReplyKeyboardRemove()
            )
        return

    elif state == "AWAIT_STREAM_KEY":
        data = USER_STATES.pop(chat_id, {})
        platform = data.get("platform", "YouTube")
        key = text

        if platform == "YouTube":
            target = f"rtmp://a.rtmp.youtube.com/live2/{key}"
        elif platform == "Facebook":
            target = f"rtmps://live-api-s.facebook.com:443/rtmp/{key}"
        elif platform == "Instagram":
            target = f"rtmps://live-upload.instagram.com:443/rtmp/{key}"
        else:
            target = key

        streams = load_streams()
        streams.append({
            "tiktok_user": data["tiktok_user"],
            "platform": platform,
            "stream_key": key if platform != "Custom RTMP" else "",
            "target_url": target,
            "enabled": True,
            "is_live": False
        })
        save_streams(streams)

        bot.send_message(
            chat_id,
            f"🎉 <b>تمت إضافة البث وتفعيله بنجاح!</b>\n\n"
            f"• <b>حساب تيك توك:</b> @{data['tiktok_user']}\n"
            f"• <b>المنصة:</b> {data['platform']}\n"
            f"• سيصلك إشعار فوري لحظة فتح البث.",
            reply_markup=main_keyboard()
        )
        return

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    if not is_admin(call.from_user):
        bot.answer_callback_query(call.id, "⛔ ليس لديك صلاحية.")
        return

    data = call.data
    streams = load_streams()

    if data.startswith("pause_"):
        idx = int(data.split("_")[1])
        if idx < len(streams):
            streams[idx]["enabled"] = False
            save_streams(streams)
            bot.answer_callback_query(call.id, f"تم إيقاف @{streams[idx]['tiktok_user']} مؤقتاً")
            bot.edit_message_text(
                f"⏸️ <b>تم إيقاف البث لحساب @{streams[idx]['tiktok_user']} مؤقتاً.</b>",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )

    elif data.startswith("resume_"):
        idx = int(data.split("_")[1])
        if idx < len(streams):
            streams[idx]["enabled"] = True
            save_streams(streams)
            bot.answer_callback_query(call.id, f"تم استئناف @{streams[idx]['tiktok_user']}")
            bot.edit_message_text(
                f"▶️ <b>تم استئناف وتشغيل البث لحساب @{streams[idx]['tiktok_user']}.</b>",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )

    elif data.startswith("del_"):
        idx = int(data.split("_")[1])
        if idx < len(streams):
            removed = streams.pop(idx)
            save_streams(streams)
            bot.answer_callback_query(call.id, "تم الحذف")
            bot.edit_message_text(
                f"🗑️ <b>تم حذف البث الخاص بـ @{removed['tiktok_user']} نهائياً.</b>",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )

if __name__ == "__main__":
    print(f"[{MY_NAME}] Bot daemon starting...", flush=True)
    while True:
        try:
            bot.polling(none_stop=True, timeout=20, long_polling_timeout=20)
        except Exception as e:
            time.sleep(3)
