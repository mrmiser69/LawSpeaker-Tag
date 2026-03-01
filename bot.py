# ===============================
# IMPORTS
# ===============================
import os
import time
import asyncio
import contextlib
import re
from html import escape
from typing import Optional
import random
import sqlite3

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatPermissions,
)
from telegram.error import RetryAfter, Forbidden, BadRequest, ChatMigrated
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ChatMemberHandler,
    PreCheckoutQueryHandler,
)

from psycopg_pool import ConnectionPool  # ✅ ONLY THIS (Supabase safe)

# ===============================
# CONFIG / CONSTANTS
# ===============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
START_IMAGE = "https://i.postimg.cc/Z50C1r4y/Untitled-design-(23).png"

DB_HOST = os.getenv("SUPABASE_HOST")
DB_NAME = os.getenv("SUPABASE_DB")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_PORT = int(os.getenv("SUPABASE_PORT", "6543"))

# Admin-list cache (performance: avoid per-user get_chat_member too often)
ADMIN_LIST_CACHE: dict[int, set[int]] = {}
ADMIN_LIST_CACHE_TS: dict[int, int] = {}
ADMIN_LIST_TTL = 60  # seconds (1 min)

# ===============================
# TAG MENTION BOT (LOCKED RULES)
# ===============================
FALLBACK_DB_PATH = os.getenv("FALLBACK_DB_PATH", "/data/tagbot_cache.db")
MENTION_COOLDOWN_SECONDS = 30
ACTIVE_SECONDS = 300  # /call = last 5 minutes active
EMOJIS_PER_MESSAGE = 7

# as big as you want (add more anytime)
EMOJI_POOL = [
  "🤣","😁","😊","😥","😎","❤️","👈","☠️","👽","👾","🤖","🎃","💀","👻",
  "😚","🤨","😹","👍","🥰","💞","🔥","✨","🌟","💥","🎉","🎊","💯","✅","⚡",
  "🌈","🍀","🍎","🍉","🍓","🍒","🍑","🍍","🥭","🍇","🍌","🥥","🍪","🍩",
  "🐶","🐱","🐭","🐹","🐰","🦊","🐻","🐼","🐸","🦁","🐯","🐵","🐧","🦄",
  "🎯","🎮","🎵","🎧","📌","📣","📢","🚀","🧨","🛡️","🧠","🫶","🙏","🤝"
]

# ===============================
# GLOBAL CACHES / STATE
# ===============================
STATS_CACHE = {"users": 0, "groups": 0, "admin_groups": 0, "last_update": 0}
STATS_TTL = 300  # 5 minutes

BOT_ADMIN_CACHE: set[int] = set()
USER_ADMIN_CACHE: dict[int, set[int]] = {}
REMINDER_MESSAGES: dict[int, list[int]] = {}
PENDING_BROADCAST = {}
PENDING_TARGET = {}
PENDING_BUTTON_WAIT = {}
BOT_START_TIME = int(time.time())

LOG_RATE_CACHE = {}
LOG_RATE_SECONDS = 60

ADMIN_VERIFY_CACHE = {}
ADMIN_VERIFY_SECONDS = 60

# Tag mention runtime caches (DB down ok)
LAST_MENTION_TS: dict[int, int] = {}   # chat_id -> ts
STOPPED_CHATS: set[int] = set()        # chat_id stopped by /stop

# ===============================
# DB POOL + DB EXEC
# ===============================
pool = None
DB_READY = False

async def db_execute(query, params=None, fetch=False):
    loop = asyncio.get_running_loop()

    def _run():
        if pool is None:
            raise RuntimeError("DB pool not initialized")
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    # cur.description can be None (e.g., some commands)
                    cols = [d.name for d in (cur.description or [])]
                    rows = cur.fetchall()
                    conn.commit()
                    return [dict(zip(cols, r)) for r in rows] if cols else rows
                conn.commit()

    return await loop.run_in_executor(None, _run)

# ✅ prevent "Task exception was never retrieved" when DB is down
async def safe_db_execute(query, params=None, fetch=False):
    # If DB is not ready, don't even try (avoid log spam + overhead)
    if pool is None or not DB_READY:
        return None
    try:
        return await db_execute(query, params=params, fetch=fetch)
    except Exception as e:
        # keep bot running even if DB fails
        rate_limited_log("db_error", f"❌ DB ERROR: {e}")
        return None

# ===============================
# FALLBACK SQLITE (PERSIST MEMBERS + STOP STATE)
# ===============================
_sql_conn = None
_sqlite_lock = asyncio.Lock()

def _sqlite_connect():
    global _sql_conn
    if _sql_conn is None:
        # check_same_thread=False because we run in executor threads
        _sql_conn = sqlite3.connect(FALLBACK_DB_PATH, check_same_thread=False)
    return _sql_conn

async def sqlite_execute(query: str, params=(), fetch: bool=False):
    loop = asyncio.get_running_loop()
    async with _sqlite_lock:
        def _run():
            conn = _sqlite_connect()
            cur = conn.cursor()
            cur.execute(query, params)
            if fetch:
                rows = cur.fetchall()
                conn.commit()
                return rows
            conn.commit()
            return None
        return await loop.run_in_executor(None, _run)

async def init_fallback_sqlite():
    # Always available even when Postgres down
    await sqlite_execute("""
      CREATE TABLE IF NOT EXISTS members (
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        last_seen INTEGER NOT NULL,
        PRIMARY KEY (chat_id, user_id)
      )
    """)
    await sqlite_execute("""
      CREATE TABLE IF NOT EXISTS stops (
        chat_id INTEGER PRIMARY KEY,
        stopped_by INTEGER,
        stopped_by_name TEXT,
        stopped_at INTEGER
      )
    """)

# ===============================
# DB INIT / DB HELPERS
# ===============================
async def init_db():
    if pool is None:
        return
    
    await safe_db_execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY
        )
    """)
    await safe_db_execute("""
        CREATE TABLE IF NOT EXISTS groups (
            group_id BIGINT PRIMARY KEY,
            is_admin_cached BOOLEAN DEFAULT FALSE,
            last_checked_at BIGINT,
            fail_count INT DEFAULT 0,
            last_fail_at BIGINT
        )
    """)
    # safety for existing DB (if table already created)
    await safe_db_execute("ALTER TABLE groups ADD COLUMN IF NOT EXISTS fail_count INT DEFAULT 0")
    await safe_db_execute("ALTER TABLE groups ADD COLUMN IF NOT EXISTS last_fail_at BIGINT")   

async def is_group_admin_cached_db(chat_id: int) -> bool:
    rows = await safe_db_execute(
        "SELECT is_admin_cached FROM groups WHERE group_id=%s",
        (chat_id,),
        fetch=True
    )
    return bool(rows and rows[0].get("is_admin_cached"))

# ===============================
# GENERIC HELPERS
# ===============================
def rate_limited_log(key: str, message: str):
    now = int(time.time())
    last = LOG_RATE_CACHE.get(key, 0)
    if now - last >= LOG_RATE_SECONDS:
        LOG_RATE_CACHE[key] = now
        print(message)

def clear_reminders(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    job_queue = context.job_queue
    if job_queue is None:
        return
    for job in list(job_queue.jobs()):
        data = job.data or {}
        if data.get("chat_id") != chat_id:
            continue
        name = job.name or ""
        if name.startswith("auto_leave_") or data.get("type") == "admin_reminder":
            job.schedule_removal()

# pagination helper (OFFSET version) - from your pasted code
async def iter_db_ids(query, batch_size=500):
    offset = 0
    while True:
        rows = await safe_db_execute(
            f"{query} LIMIT %s OFFSET %s",
            (batch_size, offset),
            fetch=True
        )
        if rows is None:
            break
        if not rows:
            break
        yield rows
        offset += batch_size

async def update_progress(msg, done, total):
    if total <= 0:
        percent = 100
    else:
        percent = int((done / total) * 100)
    bar_blocks = min(10, percent // 10)
    bar = "█" * bar_blocks + "░" * (10 - bar_blocks)
    try:
        await msg.edit_text(
            "📢 <b>Broadcasting...</b>\n\n"
            f"⏳ Progress: {bar} {percent}%",
            parse_mode="HTML"
        )
    except:
        pass

async def get_admin_set(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """
    Fast path: fetch admin list once per TTL and reuse.
    This avoids calling get_chat_member(chat_id, user_id) too often.
    """
    now = int(time.time())
    ts = ADMIN_LIST_CACHE_TS.get(chat_id, 0)
    if now - ts < ADMIN_LIST_TTL and chat_id in ADMIN_LIST_CACHE:
        return ADMIN_LIST_CACHE[chat_id], True
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        s = {a.user.id for a in admins if a and getattr(a, "user", None)}
        ADMIN_LIST_CACHE[chat_id] = s
        ADMIN_LIST_CACHE_TS[chat_id] = now
        return s, True
    except Exception:
        return ADMIN_LIST_CACHE.get(chat_id, set()), False

# ===============================
# TAG MENTION HELPERS
# ===============================
async def is_group_admin_or_creator(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        m = await context.bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False

async def can_bot_work_for_mentions(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Locked rule: Bot must be admin to work.
    (We DON'T require delete permission for tag-mention; only admin/creator.)
    """
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        return me.status in ("administrator", "creator")
    except Exception:
        return False

async def upsert_member_activity(chat_id: int, user_id: int):
    now = int(time.time())
    # Always persist to SQLite (survive DB down + restart)
    await sqlite_execute(
        "INSERT INTO members(chat_id,user_id,last_seen) VALUES(?,?,?) "
        "ON CONFLICT(chat_id,user_id) DO UPDATE SET last_seen=excluded.last_seen",
        (chat_id, user_id, now)
    )
    # Optional: also persist to Postgres if available (not required for fallback)
    if DB_READY:
        await safe_db_execute(
          """
          CREATE TABLE IF NOT EXISTS members (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            last_seen BIGINT NOT NULL,
            PRIMARY KEY(chat_id, user_id)
          )
          """
        )
        await safe_db_execute(
          "INSERT INTO members(chat_id,user_id,last_seen) VALUES(%s,%s,%s) "
          "ON CONFLICT(chat_id,user_id) DO UPDATE SET last_seen=EXCLUDED.last_seen",
          (chat_id, user_id, now)
        )

def build_emoji_mentions(user_ids: list[int]) -> str:
    # 7 per message, emoji text hides mention target
    mentions = []
    # randomize emoji each run
    pool = EMOJI_POOL[:]
    random.shuffle(pool)
    for i, uid in enumerate(user_ids):
        emoji = pool[i % len(pool)]
        mentions.append(f"<a href='tg://user?id={uid}'>{emoji}</a>")
    return "".join(mentions)

async def get_all_members(chat_id: int) -> list[int]:
    # Prefer Postgres if available; fallback SQLite always works
    if DB_READY:
        rows = await safe_db_execute(
          "SELECT user_id FROM members WHERE chat_id=%s ORDER BY last_seen DESC",
          (chat_id,), fetch=True
        )
        if rows:
            return [int(r["user_id"]) for r in rows]
    rows2 = await sqlite_execute(
      "SELECT user_id FROM members WHERE chat_id=? ORDER BY last_seen DESC",
      (chat_id,), fetch=True
    ) or []
    return [int(r[0]) for r in rows2]

async def get_active_members(chat_id: int) -> list[int]:
    now = int(time.time())
    cutoff = now - ACTIVE_SECONDS
    if DB_READY:
        rows = await safe_db_execute(
          "SELECT user_id FROM members WHERE chat_id=%s AND last_seen >= %s ORDER BY last_seen DESC",
          (chat_id, cutoff), fetch=True
        )
        if rows:
            return [int(r["user_id"]) for r in rows]
    rows2 = await sqlite_execute(
      "SELECT user_id FROM members WHERE chat_id=? AND last_seen>=? ORDER BY last_seen DESC",
      (chat_id, cutoff), fetch=True
    ) or []
    return [int(r[0]) for r in rows2]

# ===============================
# ADMIN / PERMISSION HELPERS
# ===============================
async def is_bot_admin(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if chat_id in BOT_ADMIN_CACHE:
        return True
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator") and getattr(me, "can_delete_messages", False):
            BOT_ADMIN_CACHE.add(chat_id)
            return True
        return False
    except:
        return False

# (NOTE) is_bot_admin/ensure_bot_admin_live ကို existing broadcast/admin-cache သုံးနေတုန်းပါ
# Tag mention commands အတွက်တော့ can_bot_work_for_mentions() ကိုသီးသန့်သုံးမယ်

async def ensure_bot_admin_live(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    now = int(time.time())
    last = ADMIN_VERIFY_CACHE.get(chat_id, 0)
    # Throttle: within ADMIN_VERIFY_SECONDS, trust cache (True/False)
    if now - last < ADMIN_VERIFY_SECONDS:
        return chat_id in BOT_ADMIN_CACHE

    ADMIN_VERIFY_CACHE[chat_id] = now
    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
    except ChatMigrated as e:
        new_id = e.new_chat_id

        ADMIN_VERIFY_CACHE.pop(chat_id, None)
        # IMPORTANT: don't throttle the immediate retry; force fresh API check
        ADMIN_VERIFY_CACHE.pop(new_id, None)

        # -------- RAM migrate --------
        if chat_id in BOT_ADMIN_CACHE:
            BOT_ADMIN_CACHE.discard(chat_id)
            BOT_ADMIN_CACHE.add(new_id)
        USER_ADMIN_CACHE[new_id] = USER_ADMIN_CACHE.pop(chat_id, set())
        REMINDER_MESSAGES[new_id] = REMINDER_MESSAGES.pop(chat_id, [])
        
        # -------- ADMIN LIST cache migrate/clear --------
        if chat_id in ADMIN_LIST_CACHE:
            ADMIN_LIST_CACHE[new_id] = ADMIN_LIST_CACHE.pop(chat_id)
        if chat_id in ADMIN_LIST_CACHE_TS:
            ADMIN_LIST_CACHE_TS[new_id] = ADMIN_LIST_CACHE_TS.pop(chat_id)

        # -------- DB migrate --------
        # ✅ IMPORTANT: UPSERT new row + remove old row (avoid stale rows)
        context.application.create_task(
            safe_db_execute(
                """
                INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                VALUES (%s, TRUE, %s)
                ON CONFLICT (group_id)
                DO UPDATE SET
                  is_admin_cached = TRUE,
                  last_checked_at = EXCLUDED.last_checked_at
                """,
                (new_id, now)
            )
        )
        
        context.application.create_task(
            safe_db_execute("DELETE FROM groups WHERE group_id=%s", (chat_id,))
        )
        
        # retry with new chat_id
        return await ensure_bot_admin_live(new_id, context)
    except Exception:
        ADMIN_VERIFY_CACHE.pop(chat_id, None)                        
        # cannot access -> treat as removed / no admin
        BOT_ADMIN_CACHE.discard(chat_id)
        USER_ADMIN_CACHE.pop(chat_id, None)
        REMINDER_MESSAGES.pop(chat_id, None)
        return False

    is_admin = me.status in ("administrator", "creator")
    can_delete = getattr(me, "can_delete_messages", False)
    if is_admin and can_delete:
        BOT_ADMIN_CACHE.add(chat_id)
        # ✅ keep DB in-sync (support-only) so broadcast/stats stay correct
        context.application.create_task(
            safe_db_execute(
                """
                INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                VALUES (%s, TRUE, %s)
                ON CONFLICT (group_id)
                DO UPDATE SET
                  is_admin_cached = TRUE,
                  last_checked_at = EXCLUDED.last_checked_at
                """,
                (chat_id, now)
            )
        )
        return True

    BOT_ADMIN_CACHE.discard(chat_id)
    USER_ADMIN_CACHE.pop(chat_id, None)
    REMINDER_MESSAGES.pop(chat_id, None)

    context.application.create_task(
        safe_db_execute(
            """
            INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
            VALUES (%s, FALSE, %s)
            ON CONFLICT (group_id)
            DO UPDATE SET
              is_admin_cached = FALSE,
              last_checked_at = EXCLUDED.last_checked_at
            """,
            (chat_id, now)
        )
    )
    return False

async def is_user_admin(chat_id: int, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    admins = USER_ADMIN_CACHE.setdefault(chat_id, set())
    if user_id in admins:
        return True
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        if member.status in ("administrator", "creator"):
            admins.add(user_id)
            return True
        return False
    except:
        return False

# ===============================
# /start + DONATE + PAYMENTS
# ===============================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.message
    if not chat or not user or not msg:
        return

    bot = context.bot
    bot_username = bot.username or ""

    # PRIVATE
    if chat.type == "private":
        context.application.create_task(
            safe_db_execute(
                "INSERT INTO users VALUES (%s) ON CONFLICT DO NOTHING",
                (user.id,)
            )
        )

        user_name = escape(user.first_name or "User")
        bot_name = escape(bot.first_name or "Bot")
        user_mention = f"<a href='tg://user?id={user.id}'>{user_name}</a>"
        bot_mention = (
            f"<a href='https://t.me/{bot_username}'>{bot_name}</a>"
            if bot_username else bot_name
        )

        text = (
            f"<b>────「 {bot_mention} 」</b>\n\n"
            f"<b>ဟယ်လို {user_mention} ! 👋</b>\n\n"
            "<b>ငါသည် Group များအတွက်</b>\n"
            "<b>Tag Mention Bot တစ်ခုဖြစ်တယ်။</b>\n"
            "<b>ငါ၏လုပ်နိုင်စွမ်းကို ကောင်းကောင်းအသုံးချပါ။</b>\n\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
            "<b>📌 ငါ၏လုပ်နိုင်စွမ်း</b>\n\n"
            "<blockquote>"
            "✅ Tag Mention Group Member (Setting မလို)\n"
            "✅ Member များ အသုံးပြုခွင့်မရှိ။\n"
            "✅ Owner နှင့် Admin များသားအသုံးပြုခွင့်ရှိ။"
            "</blockquote>\n\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
            "<b>📥 ငါ့ကိုအသုံးပြုရန်</b>\n\n"
            "<blockquote>"
            "➕ ငါ့ကို Group ထဲထည့်ပါ\n"
            "⭐️ ငါ့ကို Admin ပေးပါ"
            "</blockquote>"
        )

        buttons = []
        if bot_username:
            buttons.append([
                InlineKeyboardButton(
                    "➕ 𝗔𝗗𝗗 𝗠𝗘 𝗧𝗢 𝗬𝗢𝗨𝗥 𝗚𝗥𝗢𝗨𝗣",
                    url=f"https://t.me/{bot_username}?startgroup=true"
                )
            ])
        buttons.append([InlineKeyboardButton("🤍 DONATE US 🤍", callback_data="donate_menu")])
        buttons.append([
            InlineKeyboardButton("👨‍💻 𝐃𝐞𝐯𝐞𝐥𝐨𝐩𝐞𝐫", url="tg://user?id=5942810488"),
            InlineKeyboardButton("📢 𝐂𝐡𝐚𝐧𝐧𝐞𝐥", url="https://t.me/MMTelegramBotss"),
        ])

        await msg.reply_photo(
            photo=START_IMAGE,
            caption=text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    # GROUP
    if chat.type in ("group", "supergroup"):
        try:
            me = await bot.get_chat_member(chat.id, bot.id)
        except:
            return

        if me.status in ("member", "restricted"):
            if not getattr(me, "can_send_messages", True):
                return

        if me.status in ("administrator", "creator") and getattr(me, "can_delete_messages", False):
            try:
                await bot.send_message(
                    chat.id,
                    "✅ Bot ကို Admin အဖြစ်ခန့်ထားပြီးသားပါ။\n\n"
                    "🏷️ <b>LawSpeaker Tag Mention</b>\n\n"
                    "🤖 Bot က လက်ရှိ Group မှာ ကောင်းကောင်းအလုပ်လုပ်နေပါပြီး။",
                    parse_mode="HTML"
                )
            except RetryAfter:
                return
            except Exception:
                return
            return

        try:
            await bot.send_message(
                chat.id,
                "⚠️ <b>Bot သည် Admin မဟုတ်သေးပါ</b>\n\n"
                "🤖 <b>Bot ကို အလုပ်လုပ်စေရန်</b>\n"
                "⭐️ <b>Admin Permission ပေးပါ</b>\n\n"
                "Required: Admin Permission",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "⭐ 𝗚𝗜𝗩𝗘 𝗔𝗗𝗠𝗜𝗡 𝗣𝗘𝗥𝗠𝗜𝗦𝗦𝗜𝗢𝗡",
                        url=f"https://t.me/{bot_username}?startgroup=true"
                    )
                ]])
            )
        except RetryAfter:
            return
        except Exception:
            return
        return

async def donate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer()

    data = (query.data or "").strip()
    if query.message.chat.type != "private":
        return

    bot = context.bot
    bot_username = bot.username or ""
    user = update.effective_user

    if data == "donate_menu":
        donate_text = (
            "<b>🤍 Support Us !</b>\n\n"
            "မင်းအတွက် အလုပ်ကောင်းကောင်းလုပ်နေတဲ့ Bot ကို Support ပေးနိုင်ပါတယ်။\n\n"
            "<b>👇 အောက်ကနေ ရွေးပါ</b>"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⭐️ 𝗦𝗨𝗣𝗣𝗢𝗥𝗧 𝗕𝗢𝗧 (5 Stars)", callback_data="donate_stars_5")],
            [InlineKeyboardButton("🪙 𝗦𝗨𝗣𝗣𝗢𝗥𝗧 𝗗𝗘𝗩𝗘𝗟𝗢𝗣𝗘𝗥 (TON)", callback_data="donate_ton")],
            [InlineKeyboardButton("⬅️ Back", callback_data="donate_back_start")],
        ])
        await query.message.edit_caption(caption=donate_text, parse_mode="HTML", reply_markup=kb)
        return

    if data == "donate_back_start":
        user_name = escape(user.first_name or "User")
        bot_name = escape(bot.first_name or "Bot")
        user_mention = f"<a href='tg://user?id={user.id}'>{user_name}</a>"
        bot_mention = (
            f"<a href='https://t.me/{bot_username}'>{bot_name}</a>"
            if bot_username else bot_name
        )
        start_text = (
            f"<b>────「 {bot_mention} 」</b>\n\n"
            f"<b>ဟယ်လို {user_mention} ! 👋</b>\n\n"
            "<b>ငါသည် Group များအတွက်</b>\n"
            "<b>Tag Mention Bot တစ်ခုဖြစ်တယ်။</b>\n"
            "<b>ငါ၏လုပ်နိုင်စွမ်းကို ကောင်းကောင်းအသုံးချပါ။</b>\n\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
            "<b>📌 ငါ၏လုပ်နိုင်စွမ်း</b>\n\n"
            "✅ Tag Mention (LawSpeaker) / Group mention helper\n\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖\n\n"
            "<b>📥 ငါ့ကိုအသုံးပြုရန်</b>\n\n"
            "➕ ငါ့ကို Group ထဲထည့်ပါ\n"
            "⭐️ ငါ့ကို Admin ပေးပါ"
        )
        buttons = []
        if bot_username:
            buttons.append([
                InlineKeyboardButton(
                    "➕ 𝗔𝗗𝗗 𝗠𝗘 𝗧𝗢 𝗬𝗢𝗨𝗥 𝗚𝗥𝗢𝗨𝗣",
                    url=f"https://t.me/{bot_username}?startgroup=true"
                )
            ])
        buttons.append([InlineKeyboardButton("🤍 DONATE US 🤍", callback_data="donate_menu")])
        buttons.append([
            InlineKeyboardButton("👨‍💻 𝐃𝐞𝐯𝐞𝐥𝐨𝐩𝐞𝐫", url="tg://user?id=5942810488"),
            InlineKeyboardButton("📢 𝐂𝐡𝐚𝐧𝐧𝐞𝐥", url="https://t.me/MMTelegramBotss"),
        ])
        await query.message.edit_caption(
            caption=start_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    if data == "donate_ton":
        TON_ADDRESS = os.getenv("TON_ADDRESS", "PUT_YOUR_TON_ADDRESS_HERE")
        ton_text = (
            "<b>🪙 Support Developer (TON)</b>\n\n"
            f"<b>TON Address:</b>\n<code>{escape(TON_ADDRESS)}</code>\n\n"
            "✅ Address ကို copy လုပ်ပြီး TON coin ပေးပို့နိုင်ပါတယ်ဗျ။\n"
            "💙 Thank You For Supporting !"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="donate_menu")]])
        await query.message.edit_caption(caption=ton_text, parse_mode="HTML", reply_markup=kb)
        return

    if data == "donate_stars_5":
        from telegram import LabeledPrice
        try:
            await context.bot.send_invoice(
                chat_id=query.message.chat.id,
                title="Support Bot",
                description=(
                    "⭐️ Telegram Stars ၅ လုံးနဲ့ Bot ကို Support ပေးနိုင်ပါတယ်။\n\n"
                    "မင်းရဲ့ အားပေးမှုက ဒီ Bot ကို ပိုကောင်းအောင် ဆက်လုပ်နိုင်ဖို့ အားအင်ဖြစ်စေပါတယ် 💙"
                ),
                payload=f"donate_bot_5_{user.id}",
                currency="XTR",
                prices=[LabeledPrice("Support", 5)],
                provider_token="",
            )
        except Exception as e:
            await query.answer(f"❌ Donate မလုပ်နိုင်ပါ: {e}", show_alert=True)
        return

async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    if not query:
        return
    if not (query.payload or "").startswith("donate_bot_5_"):
        await query.answer(ok=False, error_message="Invalid payment payload.")
        return
    await query.answer(ok=True)

async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await msg.reply_text("✅ ကျေးဇူးတင်ပါတယ်! Stars Donate လုပ်ပြီးပါပြီ ⭐️")

# ===============================
# /stats (OWNER COMMANDS)
# ===============================
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    if (not chat or chat.type != "private" or not user or user.id != OWNER_ID or not msg):
        return

    now = time.time()
    if now - STATS_CACHE["last_update"] > STATS_TTL:
        users = await safe_db_execute(
            "SELECT COUNT(*) AS c FROM users",
            fetch=True
        )
        groups = await safe_db_execute(
            "SELECT COUNT(*) AS c FROM groups",
            fetch=True
        )
        admin_groups = await safe_db_execute(
            "SELECT COUNT(*) AS c FROM groups WHERE is_admin_cached = TRUE",
            fetch=True
        )

        if users is None or groups is None or admin_groups is None:
            await msg.reply_text("⚠️ Stats မတွက်နိုင်ပါ (DB unavailable)")
            return

        STATS_CACHE["users"] = int(users[0]["c"]) if users else 0
        STATS_CACHE["groups"] = int(groups[0]["c"]) if groups else 0
        STATS_CACHE["admin_groups"] = int(admin_groups[0]["c"]) if admin_groups else 0
        STATS_CACHE["last_update"] = now


    no_admin = max(0, STATS_CACHE["groups"] - STATS_CACHE["admin_groups"])
    uptime = int(time.time()) - BOT_START_TIME
    h, m = divmod(uptime // 60, 60)

    await msg.reply_text(
        "📊 <b>Bot Statistics</b>\n\n"
        f"👤 Users: <b>{STATS_CACHE['users']}</b>\n"
        f"👥 Groups: <b>{STATS_CACHE['groups']}</b>\n\n"
        f"🔐 Admin Groups: <b>{STATS_CACHE['admin_groups']}</b>\n"
        f"⚠️ No Admin Groups: <b>{no_admin}</b>\n\n"
        f"⏱️ Uptime: <b>{h}h {m}m</b>",
        parse_mode="HTML"
    )

# ===============================
# BROADCAST FAIL TRACKING (NON-ADMIN CLEANUP)
# ===============================
async def record_broadcast_result(chat_id: int, success: bool):
    rows = await safe_db_execute(
        "SELECT is_admin_cached, fail_count FROM groups WHERE group_id=%s",
        (chat_id,),
        fetch=True
    )
    now = int(time.time())

    # ✅ If group row doesn't exist, create it (align admin flag with RAM cache)
    if not rows:
        if success:
            is_admin = (chat_id in BOT_ADMIN_CACHE)
            await safe_db_execute(
                """
                INSERT INTO groups (group_id, is_admin_cached, last_checked_at, fail_count, last_fail_at)
                VALUES (%s, %s, %s, 0, NULL)
                ON CONFLICT (group_id)
                DO UPDATE SET last_checked_at = EXCLUDED.last_checked_at
                """,
                (chat_id, is_admin, now)
            )
            return
        # fail: start fail_count at 1
        await safe_db_execute(
            """
            INSERT INTO groups (group_id, is_admin_cached, last_checked_at, fail_count, last_fail_at)
            VALUES (%s, FALSE, %s, 1, %s)
            ON CONFLICT (group_id)
            DO UPDATE SET
              last_checked_at = EXCLUDED.last_checked_at,
              fail_count = COALESCE(groups.fail_count, 0) + 1,
              last_fail_at = EXCLUDED.last_fail_at
            """,
            (chat_id, now, now)
        )
        return

    is_admin = bool(rows[0].get("is_admin_cached"))
    fails = int(rows[0].get("fail_count") or 0)

    if success:
        await safe_db_execute(
            "UPDATE groups SET fail_count=0, last_fail_at=NULL WHERE group_id=%s",
            (chat_id,)
        )
        return

    # only count fails for non-admin groups
    if not is_admin:
        fails += 1
        if fails >= 10:
            await safe_db_execute("DELETE FROM groups WHERE group_id=%s", (chat_id,))
            return
        await safe_db_execute(
            "UPDATE groups SET fail_count=%s, last_fail_at=%s WHERE group_id=%s",
            (fails, now, chat_id)
        )

# ===============================
# BROADCAST SYSTEM
# ===============================
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return
    msg = update.effective_message
    if not msg:
        return

    raw = msg.text or msg.caption or ""
    if not raw.startswith("/broadcast"):
        return

    # mode decide
    mode = "content"  # your current content-send mode (send_photo/send_video/etc)
    if raw.startswith("/broadcast_fwd"):
        mode = "forward"
        raw = raw.replace("/broadcast_fwd", "", 1).strip()
    elif raw.startswith("/broadcast_copy"):
        mode = "copy"
        raw = raw.replace("/broadcast_copy", "", 1).strip()
    else:
        raw = raw.replace("/broadcast", "", 1).strip()

    # for forward/copy: use replied message as source (important)
    src = msg.reply_to_message if msg.reply_to_message else msg
    content = {
        "mode": mode,
        "text": raw,  # optional extra text/caption override
        "photo": src.photo[-1].file_id if getattr(src, "photo", None) else None,
        "video": src.video.file_id if getattr(src, "video", None) else None,
        "audio": src.audio.file_id if getattr(src, "audio", None) else None,
        "document": src.document.file_id if getattr(src, "document", None) else None,
        # for forward/copy
        "from_chat_id": src.chat.id,
        "message_id": src.message_id,
    }

    # ✅ allow text-only OR media
    has_any_media = any([content["photo"], content["video"], content["audio"], content["document"]])
    has_text = bool(content["text"])

    # forward/copy must have a replied message (otherwise it just forwards the command)
    if mode in ("forward", "copy") and not msg.reply_to_message:
        await msg.reply_text("❌ /broadcast_fwd or /broadcast_copy ကို forward/copy လုပ်ချင်တဲ့ message ကို Reply ပြီး သုံးပါ။")
        return

    if not (has_text or has_any_media or mode in ("forward", "copy")):
        await msg.reply_text("❌ Broadcast လုပ်ရန် content မတွေ့ပါ")
        return

    PENDING_BROADCAST[OWNER_ID] = content

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ CONFIRM", callback_data="broadcast_confirm"),
        InlineKeyboardButton("❌ CANCEL", callback_data="broadcast_cancel")
    ]])
    await msg.reply_text(
        "📢 <b>Broadcast Confirm လုပ်ပါ</b>",
        parse_mode="HTML",
        reply_markup=keyboard
    )

async def broadcast_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        await query.answer()
        return
    await query.answer()
    if OWNER_ID not in PENDING_BROADCAST:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Users only", callback_data="bc_target_users")],
        [InlineKeyboardButton("👥 Groups only", callback_data="bc_target_groups")],
        [InlineKeyboardButton("👥👤 Users + Groups", callback_data="bc_target_all")],
        [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
    ])

    await query.edit_message_text(
        "📢 <b>Broadcast Target ကိုရွေးပါ</b>",
        parse_mode="HTML",
        reply_markup=keyboard
    )

async def broadcast_target_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()

    if OWNER_ID not in PENDING_BROADCAST:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    target_type = query.data
    PENDING_TARGET[OWNER_ID] = target_type

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Post Now", callback_data="bc_post_now")],
        [InlineKeyboardButton("➕ Auto Add Button", callback_data="bc_btn_auto")],
        [InlineKeyboardButton("🔗 Manual Button URL", callback_data="bc_btn_manual")],
        [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
    ])
    await query.edit_message_text(
        "📢 <b>Post Option ကိုရွေးပါ</b>",
        parse_mode="HTML",
        reply_markup=keyboard
    )

async def broadcast_auto_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()

    if OWNER_ID not in PENDING_BROADCAST or OWNER_ID not in PENDING_TARGET:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    bot_username = context.bot.username or ""
    if not bot_username:
        await query.edit_message_text("❌ Bot username မရှိလို့ Auto button link မလုပ်နိုင်ပါ")
        return

    url = f"https://t.me/{bot_username}?startgroup=true"

    data = PENDING_BROADCAST.pop(OWNER_ID, None)
    target_type = PENDING_TARGET.pop(OWNER_ID, None)
    PENDING_BUTTON_WAIT.pop(OWNER_ID, None)

    if not data or not target_type:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    progress_msg = await query.edit_message_text(
        "📢 <b>Broadcasting...</b>\n\n⏳ Progress: 0%",
        parse_mode="HTML"
    )
    await run_broadcast(context, data, target_type, progress_msg, button_url=url)

async def broadcast_post_now_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()

    data = PENDING_BROADCAST.pop(OWNER_ID, None)
    target_type = PENDING_TARGET.pop(OWNER_ID, None)
    PENDING_BUTTON_WAIT.pop(OWNER_ID, None)

    if not data or not target_type:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    progress_msg = await query.edit_message_text(
        "📢 <b>Broadcasting...</b>\n\n⏳ Progress: 0%",
        parse_mode="HTML"
    )
    await run_broadcast(context, data, target_type, progress_msg, button_url=None)

async def broadcast_manual_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()

    if OWNER_ID not in PENDING_BROADCAST or OWNER_ID not in PENDING_TARGET:
        await query.edit_message_text("❌ Broadcast data မရှိပါ")
        return

    PENDING_BUTTON_WAIT[OWNER_ID] = True
    await query.edit_message_text(
        "🔗 Button URL ကို ပို့ပါ\n\nExample:\nhttps://t.me/YourBot",
        parse_mode="HTML"
    )

async def broadcast_button_url_receiver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    msg = update.effective_message
    if not user or user.id != OWNER_ID or not msg:
        return
    if OWNER_ID not in PENDING_BUTTON_WAIT:
        return

    url = (msg.text or "").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        await msg.reply_text("❌ Invalid URL (http/https) ပဲထည့်ပါ")
        return

    PENDING_BUTTON_WAIT.pop(OWNER_ID, None)
    data = PENDING_BROADCAST.pop(OWNER_ID, None)
    target_type = PENDING_TARGET.pop(OWNER_ID, None)

    if not data or not target_type:
        await msg.reply_text("❌ Broadcast data မရှိပါ")
        return

    progress_msg = await msg.reply_text(
        "📢 <b>Broadcasting...</b>\n\n⏳ Progress: 0%",
        parse_mode="HTML"
    )
    await run_broadcast(context, data, target_type, progress_msg, button_url=url)

async def run_broadcast(context: ContextTypes.DEFAULT_TYPE,data: dict,target_type: str,progress_msg,button_url: Optional[str] = None):
    # ✅ DB down guard (avoid "Completed 0/0" confusion)
    if pool is None or not DB_READY:
        try:
            await progress_msg.edit_text(
                "❌ <b>Broadcast မလုပ်နိုင်ပါ</b>\n\n"
                "⚠️ <b>DB unavailable</b> (Bot running without DB)",
                parse_mode="HTML"
            )
        except Exception:
            pass
        return

    sent = 0
    attempted = 0
    start_time = time.time()
    total = 0

    if target_type in ("bc_target_users", "bc_target_all"):
        rows = await safe_db_execute("SELECT COUNT(*) AS c FROM users", fetch=True)
        total += int(rows[0]["c"]) if rows else 0
    if target_type in ("bc_target_groups", "bc_target_all"):
        rows = await safe_db_execute("SELECT COUNT(*) AS c FROM groups", fetch=True)
        total += int(rows[0]["c"]) if rows else 0

    async def send_with_optional_button(cid: int, is_group: bool):
        nonlocal sent, attempted
        # reuse your existing send_content for media/forward/copy,
        # but for "content text only" we add reply_markup easily by sending message ourselves
        # easiest: just pass button_url inside data and handle in send_content (minimal edits)
        tmp = dict(data)
        tmp["button_url"] = button_url
        res = await safe_send(send_content, context, cid, tmp)
        attempted += 1
        if res:
            sent += 1
            if is_group:
                context.application.create_task(record_broadcast_result(cid, True))
        else:
            if is_group:
                context.application.create_task(record_broadcast_result(cid, False))

        if attempted % 50 == 0 or attempted == total:
            await update_progress(progress_msg, attempted, total)

    if target_type in ("bc_target_users", "bc_target_all"):
        async for rows in iter_db_ids("SELECT user_id FROM users ORDER BY user_id"):
            for r in rows:
                await send_with_optional_button(int(r["user_id"]), is_group=False)

    if target_type in ("bc_target_groups", "bc_target_all"):
        async for rows in iter_db_ids("SELECT group_id FROM groups ORDER BY group_id"):
            for r in rows:
                await send_with_optional_button(int(r["group_id"]), is_group=True)

    elapsed = int(time.time() - start_time)
    try:
        await progress_msg.edit_text(
            "✅ <b>Broadcast Completed</b>\n\n"
            f"📨 Sent: <b>{sent}</b>\n"
            f"📦 Attempted: <b>{attempted}</b>\n"
            f"⏱️ Time: <b>{elapsed // 60}m {elapsed % 60}s</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass

async def safe_send(func, *args, **kwargs):
    for _ in range(5):
        try:
            return await func(*args, **kwargs)
        except ChatMigrated as e:
            try:
                ctx = args[0]
                old_chat_id = args[1]
                new_chat_id = e.new_chat_id

                # -------- RAM migrate/clear (important for consistency) --------
                # admin verify throttle: allow fresh checks on new id
                ADMIN_VERIFY_CACHE.pop(old_chat_id, None)
                ADMIN_VERIFY_CACHE.pop(new_chat_id, None)

                # admin list cache
                if old_chat_id in ADMIN_LIST_CACHE:
                    ADMIN_LIST_CACHE[new_chat_id] = ADMIN_LIST_CACHE.pop(old_chat_id)
                if old_chat_id in ADMIN_LIST_CACHE_TS:
                    ADMIN_LIST_CACHE_TS[new_chat_id] = ADMIN_LIST_CACHE_TS.pop(old_chat_id)

                try:
                    me = await ctx.bot.get_chat_member(new_chat_id, ctx.bot.id)   
                    is_admin = me.status in ("administrator", "creator") and getattr(me, "can_delete_messages", False)
                except Exception:
                    is_admin = False
                
                ctx.application.create_task(
                    safe_db_execute(
                        """
                        INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (group_id)
                        DO UPDATE SET
                          is_admin_cached = EXCLUDED.is_admin_cached,
                          last_checked_at = EXCLUDED.last_checked_at
                        """,
                        (new_chat_id, is_admin, int(time.time()))
                    )
                )
                ctx.application.create_task(
                    safe_db_execute("DELETE FROM groups WHERE group_id=%s", (old_chat_id,))
                )

                new_args = (args[0], new_chat_id, *args[2:])
                args = new_args
                continue
            except Exception:
                return None
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except (Forbidden, BadRequest):
            return None
    return None

async def broadcast_cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        await query.answer()
        return
    await query.answer()
    PENDING_BROADCAST.pop(OWNER_ID, None)
    PENDING_TARGET.pop(OWNER_ID, None)
    PENDING_BUTTON_WAIT.pop(OWNER_ID, None)
    await query.edit_message_text("❌ Broadcast Cancel လုပ်လိုက်ပါပြီ")

async def send_content(context, chat_id, data):
    mode = data.get("mode", "content")
    # optional button url for "ADD ME IN YOUR GROUP"
    button_url = (data.get("button_url") or "").strip()
    reply_markup = None
    if button_url:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ 𝗔𝗗𝗗 𝗠𝗘 𝗧𝗢 𝗬𝗢𝗨𝗥 𝗚𝗥𝗢𝗨𝗣", url=button_url)]
        ])
    # 1) forward/copy mode
    if mode in ("forward", "copy"):
        from_chat_id = data.get("from_chat_id")
        message_id = data.get("message_id")
        if not from_chat_id or not message_id:
            return None
        try:
            # ✅ allow raw HTML in override text (quote/link formatting) 
            override_text = (data.get("text") or "").strip()
            
            if mode == "forward":
                res = await context.bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id
                )
            
                # Optional: allow extra text with forward by sending a follow-up message
                if override_text:
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=override_text,
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                # ✅ Telegram limitation: forward_message cannot include buttons
                # So if button_url exists, send a follow-up button message
                if reply_markup:
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="🔗",
                            reply_markup=reply_markup
                        )
                    except Exception:
                        pass
                return res
            else:
                # IMPORTANT:
                # - caption only works for media messages
                # - text-only messages cannot accept caption (BadRequest)
                if override_text:
                    # safest: send override text first, then copy original message as-is
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=override_text,
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                res = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id
                )
                # ✅ copy_message နဲ့လည်း buttons ထည့်လို့မရတာများတဲ့အတွက်
                # button_url ရှိရင် follow-up button message ထပ်ပို့
                if reply_markup:
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="🔗",
                            reply_markup=reply_markup
                        )
                    except Exception:
                        pass
                return res
        except (Forbidden, BadRequest):
            return None
        except Exception:
            return None

    # 2) your existing "content" mode (send_photo/send_video/etc)
    # ✅ allow raw HTML in content mode (quote/link formatting)
    text = (data.get("text") or "").strip()
    try:
        if data.get("photo"):
            try:
                return await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=data["photo"],
                    caption=text if text else None,
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )
            except BadRequest:
                return await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=data["photo"],
                    caption=text if text else None,
                    reply_markup=reply_markup
                )
        if data.get("video"):
            try:
                return await context.bot.send_video(
                    chat_id=chat_id,
                    video=data["video"],
                    caption=text if text else None,
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )
            except BadRequest:
                return await context.bot.send_video(
                    chat_id=chat_id,
                    video=data["video"],
                    caption=text if text else None,
                    reply_markup=reply_markup
                )
        if data.get("audio"):
            try:
                return await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=data["audio"],
                    caption=text if text else None,
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )
            except BadRequest:
                return await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=data["audio"],
                    caption=text if text else None,
                    reply_markup=reply_markup
                )
        if data.get("document"):
            try:
                return await context.bot.send_document(
                    chat_id=chat_id,
                    document=data["document"],
                    caption=text if text else None,
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )
            except BadRequest:
                return await context.bot.send_document(
                    chat_id=chat_id,
                    document=data["document"],
                    caption=text if text else None,
                    reply_markup=reply_markup
                )
        if text:
            try:
                return await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )
            except BadRequest:
                return await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=reply_markup
                )
    except (Forbidden, BadRequest):
        return None
    except Exception:
        return None

# ===============================
# CHAT MEMBER EVENTS
# ===============================
async def leave_if_not_admin(context: ContextTypes.DEFAULT_TYPE):
    if not context.job or not context.job.data:
        return
    chat_id = context.job.data.get("chat_id")
    if not chat_id:
        return

    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator") and getattr(me, "can_delete_messages", False):
            BOT_ADMIN_CACHE.add(chat_id)
            return
    except:
        pass

    BOT_ADMIN_CACHE.discard(chat_id)
    USER_ADMIN_CACHE.pop(chat_id, None)
    REMINDER_MESSAGES.pop(chat_id, None)

    context.application.create_task(
        safe_db_execute(
            """
            UPDATE groups
            SET is_admin_cached = FALSE,
                last_checked_at = %s
            WHERE group_id = %s
            """,
            (int(time.time()), chat_id)
        )
    )
    return

async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.my_chat_member:
        return
    chat = update.effective_chat
    if not chat:
        return

    USER_ADMIN_CACHE.pop(chat.id, None)
    # reset admin list cache when bot role changes (helps correctness)
    ADMIN_LIST_CACHE.pop(chat.id, None)
    ADMIN_LIST_CACHE_TS.pop(chat.id, None)
    # ✅ Clear verify throttle so new permission applies immediately
    ADMIN_VERIFY_CACHE.pop(chat.id, None)

    old = update.my_chat_member.old_chat_member
    new = update.my_chat_member.new_chat_member
    if not old or not new:
        return

    bot_id = context.bot.id

    if (new.user.id == bot_id and new.status == "administrator" and old.status != "administrator"):
        is_ok = getattr(new, "can_delete_messages", False)
        if is_ok:
            BOT_ADMIN_CACHE.add(chat.id)
        else:
            BOT_ADMIN_CACHE.discard(chat.id)
        
        clear_reminders(context, chat.id)

        for mid in REMINDER_MESSAGES.pop(chat.id, []):
            with contextlib.suppress(Exception):
                await context.bot.delete_message(chat.id, mid)

        context.application.create_task(
            safe_db_execute(
                """
                INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (group_id)
                DO UPDATE SET
                    is_admin_cached = EXCLUDED.is_admin_cached,
                    last_checked_at = EXCLUDED.last_checked_at,
                    fail_count = 0,
                    last_fail_at = NULL
                """,
                (chat.id, is_ok, int(time.time()))
            )
        )

        try:
            await context.bot.send_message(
                chat.id,
                "✅ <b>Thank you!</b>\n\n"
                "🤖 Bot ကို <b>Admin</b> အဖြစ် ခန့်ထားပြီးပါပြီး။\n"
                "🏷️ LawSpeaker Tag Mention စနစ် စတင်အလုပ်လုပ်နေပါပြီး.........!",
                parse_mode="HTML"
            )
        except:
            pass
        return

    if (old.user.id == bot_id and old.status in ("administrator", "creator") and new.status in ("member", "left", "kicked")):
        BOT_ADMIN_CACHE.discard(chat.id)
        clear_reminders(context, chat.id)
        return

    if (new.user.id == bot_id and new.status == "member" and old.status in ("left", "kicked")):
        BOT_ADMIN_CACHE.discard(chat.id)
        clear_reminders(context, chat.id)
        # ✅ Save non-admin group too (stay in group; no auto-leave)
        context.application.create_task(
            safe_db_execute(
                """
                INSERT INTO groups (group_id, is_admin_cached, last_checked_at, fail_count)
                VALUES (%s, FALSE, %s, 0)
                ON CONFLICT (group_id)
                DO UPDATE SET
                  is_admin_cached = FALSE,
                  last_checked_at = EXCLUDED.last_checked_at
                """,
                (chat.id, int(time.time()))
            )
        )        
        try:
            me = await context.bot.get_me()
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⭐ 𝗚𝗜𝗩𝗘 𝗔𝗗𝗠𝗜𝗡 𝗣𝗘𝗥𝗠𝗜𝗦𝗦𝗜𝗢𝗡",
                    url=f"https://t.me/{me.username}?startgroup=true"
                )
            ]])
            m = await context.bot.send_message(
                chat.id,
                "⚠️ <b>Admin Permission Required</b>\n\n"
                "🤖 Bot ကို အလုပ်လုပ်နိုင်ရန်\n"
                "⭐️ <b>Admin အဖြစ် ခန့်ထားပေးပါ</b>",
                parse_mode="HTML",
                reply_markup=keyboard
            )
            REMINDER_MESSAGES.setdefault(chat.id, []).append(m.message_id)
            if context.job_queue:
                for i in range(1, 6):
                    context.job_queue.run_once(
                        admin_reminder,
                        when=300 * i,
                        data={"chat_id": chat.id, "count": i, "total": 5, "type": "admin_reminder"}
                    )
        except:
            pass

async def admin_reminder(context: ContextTypes.DEFAULT_TYPE):
    if not context.job or not context.job.data:
        return
    chat_id = context.job.data.get("chat_id")
    count = context.job.data.get("count")
    total = context.job.data.get("total")
    if not chat_id:
        return

    if chat_id in BOT_ADMIN_CACHE:
        clear_reminders(context, chat_id)
        return

    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
    except Exception:
        clear_reminders(context, chat_id)
        BOT_ADMIN_CACHE.discard(chat_id)
        REMINDER_MESSAGES.pop(chat_id, None)
        return

    if me.status in ("administrator", "creator") and getattr(me, "can_delete_messages", False):
        BOT_ADMIN_CACHE.add(chat_id)
        clear_reminders(context, chat_id)
        return

    try:
        bot = await context.bot.get_me()
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "⭐ 𝗚𝗜𝗩𝗘 𝗔𝗗𝗠𝗜𝗡 𝗣𝗘𝗥𝗠𝗜𝗦𝗦𝗜𝗢𝗡",
                url=f"https://t.me/{bot.username}?startgroup=true"
            )
        ]])
        m = await context.bot.send_message(
            chat_id,
            f"⏰ <b>Reminder ({count}/{total})</b>\n\n"
            "🤖 Bot ကို အလုပ်လုပ်နိုင်ရန်\n"
            "⭐️ <b>Admin Permission ပေးပါ</b>\n\n"
            "⚠️ Required: Admin Permission",
            parse_mode="HTML",
            reply_markup=keyboard
        )
        REMINDER_MESSAGES.setdefault(chat_id, []).append(m.message_id)
    except Exception:
        clear_reminders(context, chat_id)
        BOT_ADMIN_CACHE.discard(chat_id)
        REMINDER_MESSAGES.pop(chat_id, None)

# ===============================
# GROUP COMMANDS
# ===============================
async def refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    if not chat or not user or not msg:
        return
    if chat.type not in ("group", "supergroup"):
        return

    chat_id = chat.id
    user_id = user.id

    if not await is_user_admin(chat_id, user_id, context):
        return

    BOT_ADMIN_CACHE.discard(chat_id)
    USER_ADMIN_CACHE.pop(chat_id, None)
    ADMIN_LIST_CACHE.pop(chat_id, None)
    ADMIN_LIST_CACHE_TS.pop(chat_id, None)

    try:
        me = await context.bot.get_chat_member(chat_id, context.bot.id)
        if me.status in ("administrator", "creator") and me.can_delete_messages:
            BOT_ADMIN_CACHE.add(chat_id)
            context.application.create_task(
                safe_db_execute(
                    """
                    INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                    VALUES (%s, TRUE, %s)
                    ON CONFLICT (group_id)
                    DO UPDATE SET
                      is_admin_cached = TRUE,
                      last_checked_at = EXCLUDED.last_checked_at
                    """,
                    (chat_id, int(time.time()))
                )
            )
        else:
            await msg.reply_text(
                "⚠️ <b>Bot မှာ Admin permission မရှိပါ</b>\n\n"
                "🔧 Admin setting ထဲမှာ\n"
                "✅ <b>Admin Permission</b> ပေးပါ",
                parse_mode="HTML"
            )
            return
    except:
        return

    await msg.reply_text(
        "🔄 <b>Refresh completed!</b>\n\n"
        "✅ Admin cache updated\n"
        "✅ Bot permission re-checked",
        parse_mode="HTML"
    )

# ===============================
# STARTUP HELPERS
# ===============================
async def refresh_admin_cache(app):
    rows = await safe_db_execute(
        "SELECT group_id FROM groups WHERE is_admin_cached = TRUE",
        fetch=True
    ) or []

    BOT_ADMIN_CACHE.clear()
    verified = 0
    skipped = 0
    now = int(time.time())

    for row in rows:
        gid = row["group_id"]
        try:
            me = await app.bot.get_chat_member(gid, app.bot.id)
            if me.status in ("administrator", "creator") and getattr(me, "can_delete_messages", False):
                BOT_ADMIN_CACHE.add(gid)
                verified += 1
                await safe_db_execute(
                    """
                    UPDATE groups
                    SET is_admin_cached = TRUE,
                        last_checked_at = %s
                    WHERE group_id = %s
                    """,
                    (now, gid)
                )
            else:
                skipped += 1
                await safe_db_execute(
                    """
                    UPDATE groups
                    SET is_admin_cached = FALSE,
                        last_checked_at = %s
                    WHERE group_id = %s
                    """,
                    (now, gid)
                )
        except ChatMigrated as e:
            new_id = e.new_chat_id
            # ✅ avoid stale throttling after migration (optional but good)
            ADMIN_VERIFY_CACHE.pop(gid, None)
            ADMIN_VERIFY_CACHE.pop(new_id, None)

            # ✅ DB migrate old->new (upsert new row + remove old row)
            await safe_db_execute(
                """
                INSERT INTO groups (group_id, is_admin_cached, last_checked_at)
                VALUES (%s, TRUE, %s)
                ON CONFLICT (group_id)
                DO UPDATE SET
                  is_admin_cached = TRUE,
                  last_checked_at = EXCLUDED.last_checked_at
                """,
                (new_id, now)
            )
            await safe_db_execute("DELETE FROM groups WHERE group_id=%s", (gid,))
            # ✅ RAM migrate
            if gid in BOT_ADMIN_CACHE:
                BOT_ADMIN_CACHE.discard(gid)
                BOT_ADMIN_CACHE.add(new_id)
            USER_ADMIN_CACHE[new_id] = USER_ADMIN_CACHE.pop(gid, set())
            REMINDER_MESSAGES[new_id] = REMINDER_MESSAGES.pop(gid, [])
            # ✅ ALSO migrate/clear admin-list cache (consistency)
            if gid in ADMIN_LIST_CACHE:
                ADMIN_LIST_CACHE[new_id] = ADMIN_LIST_CACHE.pop(gid)
            if gid in ADMIN_LIST_CACHE_TS:
                ADMIN_LIST_CACHE_TS[new_id] = ADMIN_LIST_CACHE_TS.pop(gid)
                        
            # ✅ retry admin check using new_id (same loop iteration)
            try:
                me2 = await app.bot.get_chat_member(new_id, app.bot.id)
                if me2.status in ("administrator", "creator") and getattr(me2, "can_delete_messages", False):
                    BOT_ADMIN_CACHE.add(new_id)
                    verified += 1
                    await safe_db_execute(
                        """
                        UPDATE groups
                        SET is_admin_cached = TRUE,
                            last_checked_at = %s
                        WHERE group_id = %s
                        """,
                        (now, new_id)
                    )
                else:
                    skipped += 1
                    await safe_db_execute(
                        """
                        UPDATE groups
                        SET is_admin_cached = FALSE,
                            last_checked_at = %s
                        WHERE group_id = %s
                        """,
                        (now, new_id)
                    )
            except Exception as e2:
                print(f"⚠️ Skip migrated admin check for {new_id}: {e2}", flush=True)
        except Exception as e:
            print(f"⚠️ Skip admin check for {gid}: {e}", flush=True)

        await asyncio.sleep(0.2)

    print(f"✅ Admin cache verified: {verified}", flush=True)
    print(f"⚠️ Non-admin groups marked: {skipped}", flush=True)
    return now

async def refresh_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != OWNER_ID:
        return
    msg = update.effective_message

    rows = await safe_db_execute("SELECT group_id FROM groups", fetch=True) or []
    BOT_ADMIN_CACHE.clear()

    verified = 0
    skipped = 0
    failed = 0

    for row in rows:
        gid = row["group_id"]
        try:
            me = await context.bot.get_chat_member(gid, context.bot.id)
            if me.status in ("administrator", "creator") and getattr(me, "can_delete_messages", False):
                BOT_ADMIN_CACHE.add(gid)
                verified += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"⚠️ refresh_all skip {gid}: {e}")
            failed += 1
        await asyncio.sleep(0.1)

    await msg.reply_text(
        "🔄 <b>Refresh All Completed (SAFE)</b>\n\n"
        f"✅ Admin groups (active): {verified}\n"
        f"⚠️ Non-admin groups (kept): {skipped}\n"
        f"❗ API skipped: {failed}\n\n"
        "🛡️ <i>DB was NOT modified</i>",
        parse_mode="HTML"
    )

# ===============================
# TAG MENTION COMMANDS
# ===============================
async def stop_mentions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    if not chat or not user or not msg:
        return
    if chat.type not in ("group", "supergroup"):
        return
    if not await is_group_admin_or_creator(chat.id, user.id, context):
        await msg.reply_text("⚠️ မင်းမှာ ဒီ Command သုံးရန် permission မရှိပါ။")
        return
    if not await can_bot_work_for_mentions(chat.id, context):
        await msg.reply_text(
          "⚠️ <b>Admin Permission Required</b>\n\nBot ကို Admin ပေးမှ /stop သုံးလို့ရမယ်။",
          parse_mode="HTML"
        )
        return
    
    STOPPED_CHATS.add(chat.id)
    await sqlite_execute(
      "INSERT INTO stops(chat_id,stopped_by,stopped_by_name,stopped_at) VALUES(?,?,?,?) "
      "ON CONFLICT(chat_id) DO UPDATE SET stopped_by=excluded.stopped_by, stopped_by_name=excluded.stopped_by_name, stopped_at=excluded.stopped_at",
      (chat.id, user.id, user.full_name or "", int(time.time()))
    )
    who = escape(user.full_name or "Admin")
    await msg.reply_text(f"⛔ Mention ကို <b>{who}</b> က ရပ်လိုက်ပါတယ်။", parse_mode="HTML")

async def _mention_common_guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[bool, str]:
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    if not chat or not user or not msg:
        return False, ""
    if chat.type not in ("group", "supergroup"):
        return False, ""
    # admin-only
    if not await is_group_admin_or_creator(chat.id, user.id, context):
        await msg.reply_text("⚠️ မင်းမှာ ဒီ Command သုံးရန် permission မရှိပါ။")
        return False, ""
    # bot must be admin
    if not await can_bot_work_for_mentions(chat.id, context):
        await msg.reply_text(
          "⚠️ <b>Admin Permission Required</b>\n\n"
          "Bot ကို Admin ပေးမှ Tag Mention အလုပ်လုပ်ပါမယ်။",
          parse_mode="HTML"
        )
        return False, ""
    # cooldown 30s
    now = int(time.time())
    last = LAST_MENTION_TS.get(chat.id, 0)
    if now - last < MENTION_COOLDOWN_SECONDS:
        wait = MENTION_COOLDOWN_SECONDS - (now - last)
        await msg.reply_text(f"⏳ Cooldown... ({wait}s) နောက်မှထပ်သုံးပါ။")
        return False, ""
    LAST_MENTION_TS[chat.id] = now
    # if stopped, auto-resume by using /all /admins /call (no resume message)
    if chat.id in STOPPED_CHATS:
        STOPPED_CHATS.discard(chat.id)
        await sqlite_execute("DELETE FROM stops WHERE chat_id=?", (chat.id,))
    return True, (msg.text or "").strip()

async def mention_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, raw = await _mention_common_guard(update, context)
    if not ok:
        return
    chat = update.effective_chat
    msg = update.effective_message
    text = raw.replace("/admins", "", 1).strip()
    admins = await context.bot.get_chat_administrators(chat.id)
    ids = []
    for a in admins:
        u = getattr(a, "user", None)
        if not u or u.is_bot:
            continue
        ids.append(int(u.id))
    ids = list(dict.fromkeys(ids))
    await _send_mentions_in_chunks(context, msg, ids, text)

async def mention_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, raw = await _mention_common_guard(update, context)
    if not ok:
        return
    chat = update.effective_chat
    msg = update.effective_message
    text = raw.replace("/all", "", 1).strip()
    ids = await get_all_members(chat.id)
    # remove bot id if present
    ids = [i for i in ids if i != context.bot.id]
    await _send_mentions_in_chunks(context, msg, ids, text)

async def mention_everyone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # alias of /all
    ok, raw = await _mention_common_guard(update, context)
    if not ok:
        return
    chat = update.effective_chat
    msg = update.effective_message
    text = raw.replace("/everyone", "", 1).strip()
    ids = await get_all_members(chat.id)
    ids = [i for i in ids if i != context.bot.id]
    await _send_mentions_in_chunks(context, msg, ids, text)

async def mention_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, raw = await _mention_common_guard(update, context)
    if not ok:
        return
    chat = update.effective_chat
    msg = update.effective_message
    text = raw.replace("/call", "", 1).strip()
    ids = await get_active_members(chat.id)
    ids = [i for i in ids if i != context.bot.id]
    await _send_mentions_in_chunks(context, msg, ids, text)

async def _send_mentions_in_chunks(context: ContextTypes.DEFAULT_TYPE, msg, user_ids: list[int], text: str):
    # empty list guard
    if not user_ids:
        await context.bot.send_message(chat_id=msg.chat_id,text="⚠️ Member list မရှိသေးပါ။ (အနည်းငယ် message တွေပြောပြီးမှ Mention လုပ်ပါ)"
        )
        return
    # chunk 7 each message
    for i in range(0, len(user_ids), EMOJIS_PER_MESSAGE):
        chunk = user_ids[i:i+EMOJIS_PER_MESSAGE]
        line = build_emoji_mentions(chunk)
        if text:
            body = f"{escape(text)}\n\n{line}"
        else:
            body = f"{line}"
        try:
           await context.bot.send_message(
               chat_id=msg.chat_id,
               text=body,
               parse_mode="HTML",
               disable_web_page_preview=True,
               reply_to_message_id=None
           )
        except Exception:
            # fallback plain text (mentions might not work)
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=((text + "\n\n" if text else "") + " ".join(["🙂"] * len(chunk)))
            )
        await asyncio.sleep(0.7)  # reduce flood

async def track_activity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    if not chat or not user or not msg:
        return
    if chat.type not in ("group", "supergroup"):
        return
    if user.is_bot:
        return
    # record only real messages
    await upsert_member_activity(chat.id, user.id)

# ===============================
# MAIN
# ===============================
def main():
    global pool
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN missing")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("refresh", refresh))
    app.add_handler(CommandHandler("refresh_all", refresh_all))

    # Tag Mention Commands (LOCKED)
    app.add_handler(CommandHandler("admins", mention_admins))
    app.add_handler(CommandHandler("all", mention_all))
    app.add_handler(CommandHandler("everyone", mention_everyone))
    app.add_handler(CommandHandler("call", mention_active))
    app.add_handler(CommandHandler("stop", stop_mentions))

    # Track member activity (build list over time)
    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, track_activity), group=1)
    
    # Donate / Payments
    app.add_handler(CallbackQueryHandler(donate_callback, pattern=r"^donate"))
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))

    # Chat member
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # Broadcast
    app.add_handler(
        MessageHandler(
            filters.User(OWNER_ID) & (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.AUDIO | filters.Document.ALL),
            broadcast
        )
    )
    app.add_handler(CallbackQueryHandler(broadcast_confirm_handler, pattern="broadcast_confirm"))
    app.add_handler(CallbackQueryHandler(broadcast_target_handler, pattern="^bc_target_"))
    app.add_handler(CallbackQueryHandler(broadcast_post_now_handler, pattern="^bc_post_now$"))
    app.add_handler(CallbackQueryHandler(broadcast_auto_button_handler, pattern="^bc_btn_auto$"))
    app.add_handler(CallbackQueryHandler(broadcast_manual_button_handler, pattern="^bc_btn_manual$"))
    app.add_handler(CallbackQueryHandler(broadcast_cancel_handler, pattern="broadcast_cancel"))
    app.add_handler(MessageHandler(filters.User(OWNER_ID)& filters.TEXT& ~filters.COMMAND,broadcast_button_url_receiver))

    # -------------------------------
    # STARTUP HOOK (CORRECT)
    # -------------------------------
    async def on_startup(app):
        global pool
        print("🟡 Starting bot...", flush=True)

        await app.bot.delete_webhook(drop_pending_updates=True)

        # ✅ Fallback sqlite always init first (survive DB down + restart)
        try:
            await init_fallback_sqlite()
            print(f"✅ Fallback SQLite ready: {FALLBACK_DB_PATH}", flush=True)
            # load stopped chats from sqlite (persist /stop across restart)
            rows = await sqlite_execute("SELECT chat_id FROM stops", fetch=True) or []
            STOPPED_CHATS.clear()
            STOPPED_CHATS.update(int(r[0]) for r in rows)
        except Exception as e:
            print("⚠️ Fallback SQLite init failed:", e, flush=True)
        
        try:
            pool = ConnectionPool(
                conninfo=(
                    f"host={DB_HOST} "
                    f"dbname={DB_NAME} "
                    f"user={DB_USER} "
                    f"password={DB_PASS} "
                    f"port={DB_PORT} "
                    f"sslmode=require"
                ),
                min_size=1,
                max_size=5,
                timeout=5,
                kwargs={"prepare_threshold": None}
            )
            print("✅ DB pool created", flush=True)
            global DB_READY
            DB_READY = True
        except Exception as e:
            print("⚠️ DB pool creation failed (BOT WILL RUN WITHOUT DB):", e, flush=True)
            pool = None
            DB_READY = False

        # DB is optional now
        await init_db()
        if DB_READY:
            print("✅ DB init done", flush=True)
            now = await refresh_admin_cache(app)
            print("✅ Admin cache refreshed", flush=True)

        print("🤖 LawSpeaker Tag Bot running (PRODUCTION READY)", flush=True)
    
    async def on_error(update, context):
        if isinstance(context.error, RetryAfter):
            return
        print("ERROR:", context.error)

    app.add_error_handler(on_error)
    
    # ✅ IMPORTANT
    app.post_init = on_startup

    try:
        app.run_polling(close_loop=False)
    finally:
        if pool:
            pool.close()


if __name__ == "__main__":
    main()