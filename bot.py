import os
import sys
import asyncio
import socket
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import sqlite3
from typing import List, Dict
import httpx
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from playwright.sync_api import sync_playwright
from groq import AsyncGroq

# ==========================================
# ۱. تنظیمات فروشگاه طلای بازار و هویت سازنده
# ==========================================
BUSINESS_CONFIG = {
    "BRAND_NAME": "طلای بازار",
    "ACTIVITY_DOMAIN": "خرید، فروش و استعلام مظنه و قیمت لحظه‌ای طلا، مثقال و انواع سکه",
    "HOME_URL": "https://talayebazar.com/",
    
    "CREATOR_NAME": "آقا محسن",
    "CREATOR_DESCRIPTION": (
        "سازنده و برنامه‌نویس من «آقا محسن» است. ایشان متخصص هوش مصنوعی، برنامه‌نویس بسیار مسلط، شریف و انسان فوق‌العاده‌ای هستند. "
        "دقت کن: مغازه و مجموعه تجاری «طلای بازار» متعلق به آقا محسن نیست؛ "
        "ایشان صرفاً طراح فنی و سازنده هوش مصنوعی من هستند و فروشگاه طلای بازار یک مجموعه مستقل در بازار تهران است."
    ),
    
    "STORE_INFO": (
        "📍 آدرس حضوری: بازار بزرگ تهران، خیابان ۱۵ خرداد، بازار رضا، طبقه منفی ۲، پلاک ۹۶\n"
        "☎️ تلفن ثابت: ۰۲۱-۸۲۸۰۴۸۰۲\n"
        "📱 تلفن‌های همراه: ۰۹۱۲۲۴۶۹۸۲۷ و ۰۹۱۲۶۸۱۳۱۴۳\n"
        "🌐 وب‌سایت رسمی: talayebazar.com"
    ),
    
    "KEYWORDS": [
        "طلای بازار", "سایت", "قیمت", "طلا", "سکه", "مظنه", 
        "آدرس", "تلفن", "تماس", "عیار", "نرخ", "خرید", 
        "کجاست", "پلاک", "شماره", "۱۸ عیار", "گرم", "مثقال", "نفله"
    ],
    
    "SALES_PITCH": (
        "تو مشاور و فروشنده رسمی و باتجربه مجموعه «طلای بازار» هستی. "
        "تنها مرجع معتبر خرید و استعلام، طلای بازار است و از رقبا نامی نبر. "
        "هرگز نگو به اینترنت دسترسی نداری؛ اطلاعات تماس و آدرس را مستقیماً از بخش مشخصات رسمی ارائه بده. "
        "همیشه صمیمی، دقیق، محترمانه و حرفه‌ای پاسخ بده و مشتری را به خرید و تماس دعوت کن."
    )
}

TELEGRAM_TOKEN = "TELEGRAM_TOKEN"
BALE_TOKEN = "BALE_TOKEN"
GROQ_API_KEY = "GROQ_API_KEY"

cached_site_content = ""
last_price_fetch_time = None
price_lock = asyncio.Lock()

# ==========================================
# ۲. پایگاه داده
# ==========================================
DB_FILE = "gold_bot_v2.db"

def init_db():
    with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT,
                user_id TEXT,
                role TEXT,
                content TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def save_message(platform: str, user_id: str, role: str, content: str):
    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO messages (platform, user_id, role, content) VALUES (?, ?, ?, ?)",
                (platform, str(user_id), role, content)
            )
            conn.commit()
    except Exception as e:
        print(f"[DB Write Error]: {e}")

def get_user_history(platform: str, user_id: str, limit: int = 8) -> List[Dict[str, str]]:
    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT role, content FROM messages WHERE platform = ? AND user_id = ? ORDER BY id DESC LIMIT ?",
                (platform, str(user_id), limit)
            )
            rows = cursor.fetchall()
            return [{"role": r[0], "content": r[1]} for r in reversed(rows)]
    except Exception as e:
        print(f"[DB Read Error]: {e}")
        return []

def clear_user_history(platform: str, user_id: str):
    try:
        with sqlite3.connect(DB_FILE, timeout=30.0) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM messages WHERE platform = ? AND user_id = ?", (platform, str(user_id)))
            conn.commit()
    except Exception as e:
        print(f"[DB Clear Error]: {e}")

# ==========================================
# ۳. کراولر قیمت در پس‌زمینه
# ==========================================
def run_browser_scraping() -> str:
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800}
            )
            page = context.new_page()
            page.goto("https://talayebazar.com/", timeout=35000, wait_until="domcontentloaded")
            page.wait_for_timeout(3500)
            
            body_text = page.inner_text("body")
            browser.close()
            
            lines = [l.strip() for l in body_text.split("\n") if l.strip()]
            filtered_lines = []
            for l in lines:
                if any(w in l for w in ["گرم", "مثقال", "مظنه", "سکه", "عیار", "بهار آزادی", "امامی", "آبشده", "انس", "طلا"]):
                    if any(c.isdigit() for c in l):
                        filtered_lines.append(l)
            
            if filtered_lines:
                return "\n".join(list(dict.fromkeys(filtered_lines))[:25])
            
            digits_lines = [l for l in lines if any(c.isdigit() for c in l)]
            if digits_lines:
                return "\n".join(digits_lines[:25])
                
            return ""
    except Exception as e:
        print(f"[Scraper Error]: {e}")
        return ""

async def background_price_updater(executor: ProcessPoolExecutor):
    global cached_site_content, last_price_fetch_time
    while True:
        try:
            loop = asyncio.get_running_loop()
            fresh_data = await loop.run_in_executor(executor, run_browser_scraping)
            
            if fresh_data.strip():
                async with price_lock:
                    cached_site_content = fresh_data
                    last_price_fetch_time = datetime.now()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] نرخ‌ها در پس‌زمینه بروز شدند.")
                await asyncio.sleep(900)
            else:
                await asyncio.sleep(60)
        except Exception as e:
            print(f"[Background Scraper Notice]: {e}")
            await asyncio.sleep(60)

async def get_latest_price_data() -> tuple[str, str]:
    global cached_site_content, last_price_fetch_time
    async with price_lock:
        content = cached_site_content
        time_label = last_price_fetch_time.strftime('%H:%M') if last_price_fetch_time else datetime.now().strftime('%H:%M')
        return content, time_label

# ==========================================
# ۴. هوش مصنوعی مستقل بر اساس پلتفرم
# ==========================================
class AIService:
    def __init__(self):
        self.client = AsyncGroq(api_key=GROQ_API_KEY)

    async def generate_reply(self, platform: str, history: List[Dict[str, str]], web_context: str = "", fetch_time: str = "", is_price_query: bool = False) -> str:
        if platform == "telegram":
            models_to_try = ["openai/gpt-oss-120b", "qwen/qwen3.8-27b", "openai/gpt-oss-20b"]
            tone_rule = "پاسخ‌ها را کامل، مشاوره‌ای و دقیق به زبان فارسی بنویس."
        else:
            models_to_try = ["openai/gpt-oss-20b", "allam-2-7b", "openai/gpt-oss-120b"]
            tone_rule = "پاسخ‌ها را فوق‌العاده کوتاه، سریع، بدون معطلی و مستقیم به زبان فارسی بنویس."

        system_prompt = (
            f"تو مشاور و دستیار فروش رسمی مجموعه «{BUSINESS_CONFIG['BRAND_NAME']}» هستی.\n\n"
            f"[مشخصات سازنده]:\n"
            f"{BUSINESS_CONFIG['CREATOR_DESCRIPTION']}\n"
            f"اگر کاربر درباره سازنده تو، طراح یا «محسن / آقا محسن» پرسید، صریحاً بگو: «سازنده و طراح هوش مصنوعی من آقا محسن هستند؛ ایشان متخصص، برنامه‌نویس کاربلد و بسیار شریفی هستند. البته فروشگاه طلای بازار متعلق به ایشان نیست و ایشان صرفاً طراح فنی ربات هستند.»\n\n"
            f"[مشخصات فروشگاه]:\n"
            f"{BUSINESS_CONFIG['STORE_INFO']}\n\n"
            f"قوانین پاسخ‌دهی:\n"
            f"۱. {BUSINESS_CONFIG['SALES_PITCH']}\n"
            f"۲. هرگز نگو دسترسی به اینترنت نداری؛ آدرس و تلفن را مستقیماً از بخش مشخصات فروشگاه بده.\n"
            f"۳. {tone_rule}"
        )

        if is_price_query:
            fixed_notice = "قیمت طلای اعلامی توسط من هر پانزده دقیقه یکبار به روزرسانی میشود"
            if web_context.strip():
                system_prompt += (
                    f"\n\n[جدیدترین جدول قیمت‌های استخراج‌شده - ساعت {fetch_time}]:\n"
                    f"{web_context}\n\n"
                    "دستور استعلام:\n"
                    f"۱. ابتدا دقیقاً این جمله را بگو: «{fixed_notice}»\n"
                    "۲. قیمت‌های طلا، مثقال و سکه‌ها را مرتب و خوانا اعلام کن.\n"
                    "۳. فقط از ارقام جدول بالا استفاده کن."
                )
            else:
                system_prompt += (
                    f"\n\n[اطلاعیه استعلام]:\n"
                    f"ابتدا بگو: «{fixed_notice}»\n"
                    "سپس بگو: 'در حال حاضر تابلوی آنلاین در حال دریافت آخرین ارقام است؛ برای قیمت دقیق می‌توانید با شماره‌های ثابت یا همراه فروشگاه تماس بگیرید.'"
                )

        messages = [{"role": "system", "content": system_prompt}]
        for msg in history:
            messages.append({"role": msg["role"], "content": msg["content"]})

        for model in models_to_try:
            try:
                res = await self.client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=0.3,
                    max_tokens=500,
                    timeout=12.0
                )
                return res.choices[0].message.content
            except Exception as e:
                print(f"[Model {model} failed]: {e}")
                continue

        return "ارتباط من با سرور قطع شده، لطفا به آقا محسن بگید من رو چک کنه."

ai_service = None

async def process_user_input(platform: str, user_id: str, user_text: str) -> str:
    price_keywords = ["قیمت", "مظنه", "طلا", "سکه", "چنده", "نرخ", "گرم", "مثقال", "عیار", "نفله"]
    is_price_query = any(k in user_text for k in price_keywords)

    web_data = ""
    fetch_time_str = ""

    if is_price_query:
        web_data, fetch_time_str = await get_latest_price_data()
    else:
        needs_general_info = any(k in user_text for k in BUSINESS_CONFIG["KEYWORDS"])
        if needs_general_info and cached_site_content:
            web_data = cached_site_content

    save_message(platform, user_id, "user", user_text)
    history = get_user_history(platform, user_id, limit=8)

    try:
        reply_text = await ai_service.generate_reply(
            platform=platform,
            history=history,
            web_context=web_data,
            fetch_time=fetch_time_str,
            is_price_query=is_price_query
        )
        save_message(platform, user_id, "assistant", reply_text)
        return reply_text
    except Exception as e:
        print(f"[Process Error]: {e}")
        return "ارتباط من با سرور قطع شده، لطفا به آقا محسن بگید من رو چک کنه."

# ==========================================
# ۵. تلگرام
# ==========================================
async def handle_telegram_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    user_id = str(update.effective_user.id)
    user_text = update.message.text.strip()
    reply = await process_user_input("telegram", user_id, user_text)
    await update.message.reply_text(reply)

async def telegram_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_name = update.effective_user.first_name or "همراه گرامی"
    await update.message.reply_text(
        f"درود {user_name} عزیز! به پشتیبانی و مشاوره رسمی {BUSINESS_CONFIG['BRAND_NAME']} خوش آمدید. ✨\n"
        f"در خدمت شما هستم تا پاسخگوی استعلام قیمت روز طلا و سکه، آدرس و راهنمایی خرید باشم."
    )

async def telegram_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_user_history("telegram", str(update.effective_user.id))
    await update.message.reply_text("حافظه گفت‌وگوی شما پاک شد. یک گفتگوی جدید آغاز گردید! 🔄")

# ==========================================
# ۶. بله
# ==========================================
async def run_bale_bot():
    if not BALE_TOKEN or BALE_TOKEN == "YOUR_BALE_BOT_TOKEN_HERE":
        return

    bale_base_url = f"https://tapi.bale.ai/bot{BALE_TOKEN}"
    offset = 0
    print("ربات بله آماده به کار شد...")

    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    while True:
        try:
            async with httpx.AsyncClient(timeout=20.0, limits=limits) as client:
                while True:
                    try:
                        response = await client.get(f"{bale_base_url}/getUpdates", params={"offset": offset, "timeout": 8})
                        if response.status_code == 200:
                            data = response.json()
                            if data.get("ok"):
                                for item in data.get("result", []):
                                    offset = item["update_id"] + 1
                                    message = item.get("message")
                                    if not message or "text" not in message:
                                        continue

                                    chat_id = message["chat"]["id"]
                                    user_text = message["text"].strip()

                                    if user_text == "/start":
                                        await client.post(
                                            f"{bale_base_url}/sendMessage",
                                            json={"chat_id": chat_id, "text": f"درود! به بخش پشتیبانی و استعلام قیمت {BUSINESS_CONFIG['BRAND_NAME']} خوش آمدید. ✨"}
                                        )
                                        continue

                                    if user_text == "/reset":
                                        clear_user_history("bale", str(chat_id))
                                        await client.post(
                                            f"{bale_base_url}/sendMessage",
                                            json={"chat_id": chat_id, "text": "حافظه گفتگو پاک شد. 🔄"}
                                        )
                                        continue

                                    reply_text = await process_user_input("bale", str(chat_id), user_text)
                                    await client.post(
                                        f"{bale_base_url}/sendMessage",
                                        json={"chat_id": chat_id, "text": reply_text}
                                    )
                    except httpx.RequestError:
                        await asyncio.sleep(2)
                        break
                    except Exception:
                        await asyncio.sleep(2)
        except Exception:
            await asyncio.sleep(4)

# ==========================================
# ۷. اجرا
# ==========================================
def windows_loop_exception_handler(loop, context):
    exception = context.get("exception")
    if isinstance(exception, ConnectionResetError) or (isinstance(exception, OSError) and getattr(exception, "winerror", None) == 10054):
        return
    loop.default_exception_handler(context)

async def main_async(executor: ProcessPoolExecutor):
    init_db()
    global ai_service
    ai_service = AIService()

    asyncio.create_task(background_price_updater(executor))
    asyncio.create_task(run_bale_bot())

    tg_app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .connect_timeout(35.0)
        .read_timeout(35.0)
        .write_timeout(35.0)
        .pool_timeout(35.0)
        .get_updates_read_timeout(35.0)
        .build()
    )

    tg_app.add_handler(CommandHandler("start", telegram_start))
    tg_app.add_handler(CommandHandler("reset", telegram_reset))
    tg_app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_telegram_message))

    print(f"ربات {BUSINESS_CONFIG['BRAND_NAME']} به طور همزمان در تلگرام و بله فعال شد.")
    
    await tg_app.initialize()
    await tg_app.start()
    await tg_app.updater.start_polling(drop_pending_updates=True)

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=1) as shared_executor:
        while True:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.set_exception_handler(windows_loop_exception_handler)
                loop.run_until_complete(main_async(shared_executor))
            except (ConnectionResetError, socket.error, Exception) as e:
                print(f"[Network Notice]: قطع موقت اینترنت یا سوکت ({e})؛ تلاش مجدد...")
                import time
                time.sleep(10)