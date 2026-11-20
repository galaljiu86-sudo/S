#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Stream Management & Monitoring Telegram Bot (Hardened Enterprise Edition).
Auth: Via Admin Telegram Username (No password required).
Features:
- Granular Stream Control: Pause, Resume, Delete, Layout, Stream Key, Live Zoom.
- Decoupled State & Atomic Writes: status.json for live state + atomic streams.json to eliminate race conditions.
- Real-time live upload & download speed + Total data transfer.
- Server capacity & stats (superfast, nice -n 10, Dr. Shaker copy 0% CPU).
- Live logs viewer.
- Anti-duplicate stream key detection.
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
STATUS_PATH = os.path.join(DATA_DIR, "status.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")
RESTREAMER_LOG = os.path.join(DATA_DIR, "restreamer.log")

os.makedirs(DATA_DIR, exist_ok=True)

if not BOT_TOKEN:
    print("[BOT] ERROR: BOT_TOKEN is missing!", flush=True)
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

LAST_VALID_STREAMS = []

# --- إدارة الصلاحيات والبيانات الذرية ---

def is_admin(user) -> bool:
    if not ADMIN_USERNAME:
        return True
    return bool(user.username and user.username.lower() == ADMIN_USERNAME)

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
    """
    كتابة ذرية تحمي ملف streams.json من الفساد عند التزامن:
    كتابة ملف مؤقت .tmp ثم استبداله بـ os.replace.
    """
    tmp_path = STREAMS_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(streams, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, STREAMS_PATH)

    with open("/tmp/reload_restreamer", "w") as f:
        f.write(str(time.time()))

def save_chat_id(chat_id: int):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump({"chat_id": chat_id}, f)

def find_stream_idx(identifier: str, streams: list) -> int:
    for i, s in enumerate(streams):
        if s.get("id") == identifier:
            return i
    if identifier.isdigit():
        idx = int(identifier)
        if 0 <= idx < len(streams):
            return idx
    return -1

def get_layout_title(layout_code: str, zoom_pct: int = 80) -> str:
    mapping = {
        "portrait_copy": "📱 طولي أصلي (نسخ مباشر كود شاكر)",
        "portrait_stable": "⚡ طولي فائق الثبات (30fps & 2500k)",
        "landscape_to_portrait_pad": f"🎬 طولي بشاشة سوداء وتكبير ({zoom_pct}%)",
        "landscape_raw": "🖥️ عرضي كامل كما هو"
    }
    return mapping.get(layout_code, "📱 طولي")

def build_zoom_markup(sid: str, current_pct: int) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    minus_5 = max(50, current_pct - 5)
    plus_5 = min(200, current_pct + 5)
    row1 = [
        types.InlineKeyboardButton(f"➖ 5% ({minus_5}%)", callback_data=f"setz_{sid}_{minus_5}"),
        types.InlineKeyboardButton(f"➕ 5% ({plus_5}%)", callback_data=f"setz_{sid}_{plus_5}")
    ]
    markup.row(*row1)

    minus_10 = max(50, current_pct - 10)
    plus_10 = min(200, current_pct + 10)
    row2 = [
        types.InlineKeyboardButton(f"⏪ 10% ({minus_10}%)", callback_data=f"setz_{sid}_{minus_10}"),
        types.InlineKeyboardButton(f"⏩ 10% ({plus_10}%)", callback_data=f"setz_{sid}_{plus_10}")
    ]
    markup.row(*row2)

    row3 = [
        types.InlineKeyboardButton("70%", callback_data=f"setz_{sid}_70"),
        types.InlineKeyboardButton("80%", callback_data=f"setz_{sid}_80"),
        types.InlineKeyboardButton("90%", callback_data=f"setz_{sid}_90"),
        types.InlineKeyboardButton("100%", callback_data=f"setz_{sid}_100")
    ]
    markup.row(*row3)

    row4 = [
        types.InlineKeyboardButton("120%", callback_data=f"setz_{sid}_120"),
        types.InlineKeyboardButton("150%", callback_data=f"setz_{sid}_150"),
        types.InlineKeyboardButton("200% (ملء الشاشة)", callback_data=f"setz_{sid}_200")
    ]
    markup.row(*row4)

    row5 = [
        types.InlineKeyboardButton("🔙 حفظ ورجوع", callback_data=f"zoomback_{sid}")
    ]
    markup.row(*row5)
    return markup

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

    # إلغاء أي خطوة والعودة للشاشة الرئيسية فوراً عند كتابة 111
    if text == "111":
        USER_STATES.pop(chat_id, None)
        bot.send_message(chat_id, "🔙 <b>تم الإلغاء والعودة للشاشة الرئيسية.</b>", reply_markup=main_keyboard())
        return

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

    # 3. سعة وتحمل السيرفر
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

    # 4. سجل التشغيل (Logs)
    elif text == "📜 سجل التشغيل (Logs)":
        if os.path.exists(RESTREAMER_LOG):
            try:
                res = subprocess.run(["tail", "-n", "30", RESTREAMER_LOG], capture_output=True, text=True)
                logs_txt = res.stdout.strip()
                if not logs_txt:
                    logs_txt = "السجل فارغ حالياً."
            except Exception as e:
                logs_txt = f"تعذر قراءة السجل: {e}"
        else:
            logs_txt = "ملف السجل غير موجود بعد."

        escaped_logs = telebot.formatting.escape_html(logs_txt[-3500:])
        bot.send_message(chat_id, f"📜 <b>آخر 30 سطر من سجل المحرك:</b>\n\n<pre>{escaped_logs}</pre>", reply_markup=main_keyboard())
        return

    # 5. قائمة وإدارة البثوث
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

            sid = s.get("id") or str(idx)
            zoom_val = s.get("zoom_pct", 80)
            layout_str = get_layout_title(s.get("layout", "portrait_copy"), zoom_val)
            key_preview = s['stream_key'][:6] + "..." if s.get('stream_key') else "مخصص"
            msg = (
                f"<b>#{idx+1} - @{s['tiktok_user']}</b>\n"
                f"• <b>المنصة:</b> {s['platform']}\n"
                f"• <b>نمط العرض:</b> {layout_str}\n"
                f"• <b>المفتاح:</b> <code>{key_preview}</code>\n"
                f"• <b>الحالة:</b> {status_txt}\n"
            )
            markup = types.InlineKeyboardMarkup(row_width=2)
            toggle_btn = types.InlineKeyboardButton("⏸️ إيقاف مؤقت", callback_data=f"pause_{sid}") if is_enabled else types.InlineKeyboardButton("▶️ تشغيل البث", callback_data=f"resume_{sid}")
            layout_btn = types.InlineKeyboardButton("📐 تغيير المقاس", callback_data=f"chlayout_{sid}")
            key_btn = types.InlineKeyboardButton("🔑 تغيير المفتاح", callback_data=f"chkey_{sid}")
            del_btn = types.InlineKeyboardButton("🗑️ حذف", callback_data=f"del_{sid}")
            markup.add(toggle_btn, layout_btn)
            if s.get("layout") == "landscape_to_portrait_pad":
                zoom_btn = types.InlineKeyboardButton(f"🔍 تحكم في الحجم ({zoom_val}%)", callback_data=f"zoomctrl_{sid}")
                markup.add(zoom_btn)
            markup.add(key_btn, del_btn)

            bot.send_message(chat_id, msg, reply_markup=markup)
        return

    # 6. إعادة تشغيل البثوث (إعادة تشغيل نظيفة دون صدمة العمليات)
    elif text == "🔄 إعادة تشغيل البثوث":
        with open("/tmp/restart_all_streams", "w") as f:
            f.write(str(time.time()))
        bot.send_message(chat_id, "🔄 تم إرسال أمر إعادة تشغيل آمن وسلس لكل البثوث للمحرك.", reply_markup=main_keyboard())
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
        target_sid = USER_STATES[chat_id].get("stream_id")
        streams = load_streams()
        idx = find_stream_idx(target_sid, streams)
        if idx != -1:
            s = streams[idx]

            # فحص إذا كان المفتاح مستخدماً في بث آخر نشط
            dup_user = None
            for other_idx, other_s in enumerate(streams):
                if other_idx != idx and other_s.get("enabled", True):
                    if (other_s.get("stream_key") and other_s.get("stream_key") == new_key) or \
                       (other_s.get("target_url") and new_key in other_s.get("target_url")):
                        dup_user = other_s.get("tiktok_user")
                        break

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
            warn_msg = ""
            if dup_user:
                warn_msg = f"\n\n⚠️ <b>تنبيه هام جداً:</b> هذا المفتاح مستخدم حالياً لحساب آخر (<b>@{dup_user}</b>)! استخدام نفس المفتاح لبثين في نفس الوقت سيجعلهما يطردان بعضهما ويتوقفان."

            bot.send_message(
                chat_id,
                f"✅ <b>تم تحديث مفتاح البث بنجاح!</b>\n\n"
                f"• <b>الحساب:</b> @{s['tiktok_user']}\n"
                f"• <b>المنصة:</b> {s['platform']}\n"
                f"• <b>المفتاح الجديد:</b> <code>{key_preview}</code>\n\n"
                f"🔄 تم تطبيق التغيير فوراً على المحرك دون مقاطعة باقي البثوث.{warn_msg}",
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

        streams = load_streams()
        dup_user = None
        for other_s in streams:
            if other_s.get("enabled", True):
                if (other_s.get("stream_key") and other_s.get("stream_key") == stream_key) or \
                   (other_s.get("target_url") and stream_key in other_s.get("target_url")):
                    dup_user = other_s.get("tiktok_user")
                    break

        if dup_user and dup_user != data.get("tiktok_user"):
            bot.send_message(
                chat_id,
                f"⚠️ <b>تحذير هام:</b> هذا المفتاح مستخدم حالياً لحساب آخر (<b>@{dup_user}</b>)!\n"
                f"ممنوع استخدام نفس المفتاح لبثين في نفس الوقت لأن المنصة ستطردهما معاً."
            )

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

@bot.callback_query_handler(func=lambda call: call.data in ["orient_portrait", "orient_landscape"])
def handle_orient(call):
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
            "<i>(الفيديو هيكون في المنتصف أو فوق النص بسنة بسيطة وتحته وفوقه مساحة سوداء مريحة للعين، وتقدر تغير التكبير في أي وقت بأزرار +5% و -5%)</i>",
            chat_id, call.message.message_id,
            reply_markup=markup
        )

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
    clean_id = f"{data['tiktok_user']}_{int(time.time()*10)%100000}"
    streams.append({
        "id": clean_id,
        "tiktok_user": data["tiktok_user"],
        "platform": data["platform"],
        "server_url": data.get("server_url", ""),
        "stream_key": data.get("stream_key", ""),
        "target_url": data["target_url"],
        "layout": data.get("layout", "portrait_copy"),
        "zoom_pct": data.get("zoom_pct", 80),
        "enabled": True,
        "is_live": False
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
        chat_id, message_id
    )
    bot.send_message(chat_id, "جاهز ومستعد للمراقبة! يمكنك إدارة البثوث عبر الأزرار:", reply_markup=main_keyboard())

# تغيير المقاس للبثوث الموجودة مسبقاً
@bot.callback_query_handler(func=lambda call: call.data.startswith("chlayout_"))
def handle_change_layout(call):
    sid = call.data.split("_")[1]
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        actual_id = streams[idx].get("id") or str(idx)
        zoom_val = streams[idx].get("zoom_pct", 80)
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(
            types.InlineKeyboardButton("📱 طولي أصلي (نسخ مباشر كود شاكر)", callback_data=f"setlay_{actual_id}_portrait_copy"),
            types.InlineKeyboardButton("⚡ طولي فائق الثبات (30fps & 2500k - للشاشات الثابتة)", callback_data=f"setlay_{actual_id}_portrait_stable"),
            types.InlineKeyboardButton(f"🎬 طولي بشاشة سوداء وتكبير ({zoom_val}%)", callback_data=f"setlay_{actual_id}_landscape_to_portrait_pad"),
            types.InlineKeyboardButton("🖥️ عرضي كامل كما هو", callback_data=f"setlay_{actual_id}_landscape_raw")
        )
        bot.edit_message_text(
            f"📐 <b>اختر نمط العرض لحساب @{streams[idx]['tiktok_user']}:</b>",
            call.message.chat.id, call.message.message_id,
            reply_markup=markup
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith("setlay_"))
def handle_set_layout(call):
    parts = call.data.split("_")
    sid = parts[1]
    new_layout = "_".join(parts[2:])
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        actual_id = streams[idx].get("id") or str(idx)
        streams[idx]["layout"] = new_layout
        if "zoom_pct" not in streams[idx]:
            streams[idx]["zoom_pct"] = 80
        save_streams(streams)
        zoom_val = streams[idx].get("zoom_pct", 80)
        bot.answer_callback_query(call.id, "✅ تم تغيير المقاس بنجاح!")
        markup = types.InlineKeyboardMarkup()
        if new_layout == "landscape_to_portrait_pad":
            markup.add(types.InlineKeyboardButton(f"🔍 ضبط نسبة التكبير الآن ({zoom_val}%)", callback_data=f"zoomctrl_{actual_id}"))
        bot.edit_message_text(
            f"✅ تم تحديث نمط العرض لـ @{streams[idx]['tiktok_user']} إلى:\n<b>{get_layout_title(new_layout, zoom_val)}</b>",
            call.message.chat.id, call.message.message_id,
            reply_markup=markup if new_layout == "landscape_to_portrait_pad" else None
        )

# التحكم في نسبة التكبير Live Zoom
@bot.callback_query_handler(func=lambda call: call.data.startswith("zoomctrl_"))
def handle_zoom_control(call):
    sid = call.data.split("_")[1]
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        actual_id = streams[idx].get("id") or str(idx)
        s = streams[idx]
        current_zoom = s.get("zoom_pct", 80)
        markup = build_zoom_markup(actual_id, current_zoom)
        txt = (
            f"🔍 <b>لوحة التحكم المباشر في حجم العرض (Live Zoom)</b>\n\n"
            f"• <b>الحساب:</b> @{s['tiktok_user']}\n"
            f"• <b>النسبة الحالية:</b> <b>{current_zoom}%</b>\n\n"
            f"💡 <i>يمكنك الضغط على +5% أو -5% لتكبير أو تصغير الفيديو فوراً أثناء البث وبدون أي انقطاع لباقي البثوث!</i>\n"
            f"• <b>50% إلى 100%:</b> تصغير وتوسيط على شاشة سوداء بالطول.\n"
            f"• <b>100% إلى 200%:</b> تكبير تدريجي لملء أطراف الشاشة."
        )
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("setz_"))
def handle_set_zoom(call):
    parts = call.data.split("_")
    sid = parts[1]
    new_pct = int(parts[2])
    new_pct = max(50, min(200, new_pct))

    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        actual_id = streams[idx].get("id") or str(idx)
        s = streams[idx]
        old_pct = s.get("zoom_pct", 80)
        if old_pct == new_pct:
            bot.answer_callback_query(call.id, f"النسبة هي {new_pct}% بالفعل")
            return

        s["zoom_pct"] = new_pct
        save_streams(streams)

        bot.answer_callback_query(call.id, f"🎯 تم ضبط الحجم إلى {new_pct}%")
        markup = build_zoom_markup(actual_id, new_pct)
        txt = (
            f"🔍 <b>لوحة التحكم المباشر في حجم العرض (Live Zoom)</b>\n\n"
            f"• <b>الحساب:</b> @{s['tiktok_user']}\n"
            f"• <b>النسبة الحالية:</b> <b>{new_pct}%</b>\n\n"
            f"⚡ <i>تم إرسال أمر التكبير للمحرك؛ البث سيعاد ضبط مقاسه تلقائياً في ثانية واحدة دون أن يتأثر أي بث آخر!</i>\n"
            f"• <b>50% إلى 100%:</b> تصغير وتوسيط على شاشة سوداء بالطول.\n"
            f"• <b>100% إلى 200%:</b> تكبير تدريجي لملء أطراف الشاشة."
        )
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("zoomback_"))
def handle_zoom_back(call):
    sid = call.data.split("_")[1]
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        actual_id = streams[idx].get("id") or str(idx)
        s = streams[idx]
        is_enabled = s.get("enabled", True)
        if not is_enabled:
            status_txt = "⏸️ متوقف مؤقتاً"
        elif s.get("is_live"):
            status_txt = "🔴 لايف شغال حالياً"
        else:
            status_txt = "⚪ في الانتظار (أوفلاين)"

        zoom_val = s.get("zoom_pct", 80)
        layout_str = get_layout_title(s.get("layout", "portrait_copy"), zoom_val)
        key_preview = s['stream_key'][:6] + "..." if s.get('stream_key') else "مخصص"
        msg = (
            f"<b>#{idx+1} - @{s['tiktok_user']}</b>\n"
            f"• <b>المنصة:</b> {s['platform']}\n"
            f"• <b>نمط العرض:</b> {layout_str}\n"
            f"• <b>المفتاح:</b> <code>{key_preview}</code>\n"
            f"• <b>الحالة:</b> {status_txt}\n"
        )
        markup = types.InlineKeyboardMarkup(row_width=2)
        toggle_btn = types.InlineKeyboardButton("⏸️ إيقاف مؤقت", callback_data=f"pause_{actual_id}") if is_enabled else types.InlineKeyboardButton("▶️ تشغيل البث", callback_data=f"resume_{actual_id}")
        layout_btn = types.InlineKeyboardButton("📐 تغيير المقاس", callback_data=f"chlayout_{actual_id}")
        key_btn = types.InlineKeyboardButton("🔑 تغيير المفتاح", callback_data=f"chkey_{actual_id}")
        del_btn = types.InlineKeyboardButton("🗑️ حذف", callback_data=f"del_{actual_id}")
        markup.add(toggle_btn, layout_btn)
        if s.get("layout") == "landscape_to_portrait_pad":
            zoom_btn = types.InlineKeyboardButton(f"🔍 تحكم في الحجم ({zoom_val}%)", callback_data=f"zoomctrl_{actual_id}")
            markup.add(zoom_btn)
        markup.add(key_btn, del_btn)

        bot.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup)

# تغيير مفتاح البث (Stream Key)
@bot.callback_query_handler(func=lambda call: call.data.startswith("chkey_"))
def handle_change_key(call):
    chat_id = call.message.chat.id
    sid = call.data.split("_")[1]
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        s = streams[idx]
        actual_id = s.get("id") or str(idx)
        USER_STATES[chat_id] = {"step": "AWAIT_NEW_KEY", "stream_id": actual_id}
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
    sid = call.data.split("_")[1]
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        streams[idx]["enabled"] = False
        save_streams(streams)
        bot.answer_callback_query(call.id, "⏸️ تم إيقاف البث مؤقتاً")
        bot.edit_message_text(f"⏸️ تم إيقاف بث @{streams[idx]['tiktok_user']} مؤقتاً.", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("resume_"))
def handle_resume(call):
    sid = call.data.split("_")[1]
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
        streams[idx]["enabled"] = True
        save_streams(streams)
        bot.answer_callback_query(call.id, "▶️ تم استئناف تشغيل البث")
        bot.edit_message_text(f"▶️ تم تفعيل وتشغيل بث @{streams[idx]['tiktok_user']}.", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_"))
def handle_delete(call):
    sid = call.data.split("_")[1]
    streams = load_streams()
    idx = find_stream_idx(sid, streams)
    if idx != -1:
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
