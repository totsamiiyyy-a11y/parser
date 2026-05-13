"""
Telegram Parser Bot — single file, Railway-ready
Парсит посты из каналов по ключевым словам, присылает совпадения в бота.

ENV переменные (Railway → Variables):
  BOT_TOKEN       — токен бота от @BotFather
  ADMIN_IDS       — через запятую: 123456,789012
  API_ID          — с my.telegram.org
  API_HASH        — с my.telegram.org
  PHONE_NUMBER    — номер аккаунта для парсинга (+79...)
  SESSION_STRING  — строка сессии Telethon (см. ниже как получить)
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, date
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError

# ═══════════════════════════════════════════════════════════════════════════════
#  КОНФИГ (из переменных окружения)
# ═══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN      = os.environ["BOT_TOKEN"]
API_ID         = int(os.environ["API_ID"])
API_HASH       = os.environ["API_HASH"]
PHONE_NUMBER   = os.environ.get("PHONE_NUMBER", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")   # приоритет над PHONE_NUMBER
ADMIN_IDS      = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
DB_PATH        = os.environ.get("DB_PATH", "/data/parser.db")   # Railway volume или локально

# ═══════════════════════════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("tgparser")

# ═══════════════════════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════════════════════

def _db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def db_init():
    with _db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS channels (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                title    TEXT,
                tg_id    INTEGER,
                active   INTEGER NOT NULL DEFAULT 1,
                added_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS keywords (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                word     TEXT NOT NULL UNIQUE,
                added_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS matches (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                post_id    INTEGER NOT NULL,
                text       TEXT,
                keywords   TEXT,
                found_at   TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_matches ON matches(channel_id, post_id);
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO config VALUES ('interval', '60');
            INSERT OR IGNORE INTO config VALUES ('notify_all', '1');
            INSERT OR IGNORE INTO config VALUES ('last_check', '');
        """)

# ── Каналы ────────────────────────────────────────────────────────────────────

def db_add_channel(username: str, title: str, tg_id: int) -> int:
    with _db() as c:
        r = c.execute(
            "INSERT OR REPLACE INTO channels (username, title, tg_id) VALUES (?,?,?)",
            (username, title, tg_id)
        )
        return r.lastrowid

def db_get_channels(active_only=False) -> list[dict]:
    with _db() as c:
        q = "SELECT * FROM channels WHERE active=1" if active_only else "SELECT * FROM channels"
        return [dict(r) for r in c.execute(q + " ORDER BY id").fetchall()]

def db_toggle_channel(ch_id: int) -> bool:
    with _db() as c:
        cur = c.execute("SELECT active FROM channels WHERE id=?", (ch_id,)).fetchone()["active"]
        new = 0 if cur else 1
        c.execute("UPDATE channels SET active=? WHERE id=?", (new, ch_id))
        return bool(new)

def db_delete_channel(ch_id: int):
    with _db() as c:
        c.execute("DELETE FROM channels WHERE id=?", (ch_id,))

# ── Ключевые слова ────────────────────────────────────────────────────────────

def db_add_keyword(word: str):
    with _db() as c:
        c.execute("INSERT OR IGNORE INTO keywords (word) VALUES (?)", (word,))

def db_get_keywords() -> list[dict]:
    with _db() as c:
        return [dict(r) for r in c.execute("SELECT * FROM keywords ORDER BY word").fetchall()]

def db_delete_keyword(kw_id: int):
    with _db() as c:
        c.execute("DELETE FROM keywords WHERE id=?", (kw_id,))

# ── Совпадения ────────────────────────────────────────────────────────────────

def db_post_seen(channel_id: int, post_id: int) -> bool:
    with _db() as c:
        return c.execute(
            "SELECT 1 FROM matches WHERE channel_id=? AND post_id=?",
            (channel_id, post_id)
        ).fetchone() is not None

def db_save_match(channel_id: int, post_id: int, text: str, keywords: list):
    with _db() as c:
        c.execute(
            "INSERT OR IGNORE INTO matches (channel_id, post_id, text, keywords) VALUES (?,?,?,?)",
            (channel_id, post_id, text[:2000], json.dumps(keywords, ensure_ascii=False))
        )

# ── Конфиг ────────────────────────────────────────────────────────────────────

def db_get_config() -> dict:
    with _db() as c:
        rows = {r["key"]: r["value"] for r in c.execute("SELECT key,value FROM config").fetchall()}
    return {
        "interval": int(rows.get("interval", 60)),
        "notify_all": rows.get("notify_all", "1") == "1",
        "last_check": rows.get("last_check") or None,
    }

def db_set_config(key: str, value: str):
    with _db() as c:
        c.execute("INSERT OR REPLACE INTO config VALUES (?,?)", (key, value))

def db_update_last_check():
    db_set_config("last_check", datetime.now().strftime("%d.%m.%Y %H:%M:%S"))

# ── Статистика ────────────────────────────────────────────────────────────────

def db_get_stats() -> dict:
    with _db() as c:
        total_ch  = c.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
        active_ch = c.execute("SELECT COUNT(*) FROM channels WHERE active=1").fetchone()[0]
        total_kw  = c.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
        total_m   = c.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
        today_m   = c.execute(
            "SELECT COUNT(*) FROM matches WHERE found_at >= ?", (date.today().isoformat(),)
        ).fetchone()[0]
    cfg = db_get_config()
    return {
        "channels": total_ch, "active_channels": active_ch,
        "keywords": total_kw, "matches": total_m,
        "today_matches": today_m, "last_check": cfg["last_check"],
    }

# ═══════════════════════════════════════════════════════════════════════════════
#  TELETHON ПАРСЕР
# ═══════════════════════════════════════════════════════════════════════════════

class ParserClient:
    def __init__(self):
        if SESSION_STRING:
            session = StringSession(SESSION_STRING)
        else:
            session = StringSession()
        self.client = TelegramClient(session, API_ID, API_HASH)
        self._started = False

    async def start(self):
        if self._started:
            return
        if SESSION_STRING:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                raise RuntimeError("SESSION_STRING недействителен, сгенерируйте новую.")
        else:
            await self.client.start(phone=PHONE_NUMBER)
        self._started = True
        log.info("Telethon подключён")

    async def stop(self):
        if self._started:
            await self.client.disconnect()
            self._started = False

    async def get_channel_info(self, username: str) -> Optional[dict]:
        try:
            entity = await self.client.get_entity(username)
            info = {
                "id": entity.id,
                "username": f"@{entity.username}" if getattr(entity, "username", None) else username,
                "title": getattr(entity, "title", username),
                "members_count": "?",
            }
            try:
                p = await self.client.get_participants(entity, limit=0)
                info["members_count"] = p.total
            except Exception:
                pass
            return info
        except (UsernameNotOccupiedError, ValueError):
            return None
        except Exception as e:
            log.error(f"get_channel_info {username}: {e}")
            return None

    async def get_recent_posts(self, username: str, limit: int = 20) -> list[dict]:
        try:
            entity = await self.client.get_entity(username)
            posts = []
            async for msg in self.client.iter_messages(entity, limit=limit):
                if msg.text:
                    posts.append({
                        "id": msg.id,
                        "text": msg.text,
                        "date": msg.date.timestamp(),
                    })
            return posts
        except FloodWaitError as e:
            log.warning(f"FloodWait {e.seconds}s for {username}")
            await asyncio.sleep(e.seconds)
            return []
        except (ChannelPrivateError, Exception) as e:
            log.error(f"get_recent_posts {username}: {e}")
            return []

# ═══════════════════════════════════════════════════════════════════════════════
#  AIOGRAM БОТ
# ═══════════════════════════════════════════════════════════════════════════════

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())
parser: Optional[ParserClient] = None
parser_task: Optional[asyncio.Task] = None

# ── FSM ───────────────────────────────────────────────────────────────────────

class AddChannel(StatesGroup):
    waiting = State()

class AddKeyword(StatesGroup):
    waiting = State()

# ── Клавиатуры ────────────────────────────────────────────────────────────────

def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text="📋 Каналы"),          KeyboardButton(text="🔑 Ключевые слова")],
        [KeyboardButton(text="📊 Статистика"),       KeyboardButton(text="⚙️ Настройки")],
        [KeyboardButton(text="▶️ Запустить"),        KeyboardButton(text="⏹ Остановить")],
    ])

def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text="❌ Отмена")]
    ])

def kb_channels() -> InlineKeyboardMarkup:
    channels = db_get_channels()
    rows = []
    for ch in channels:
        icon = "✅" if ch["active"] else "⏸"
        rows.append([
            InlineKeyboardButton(
                text=f"{icon} {ch['title'] or ch['username']}",
                callback_data=f"ch:toggle:{ch['id']}"
            ),
            InlineKeyboardButton(text="🗑", callback_data=f"ch:del:{ch['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="➕ Добавить канал", callback_data="ch:add")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_keywords() -> InlineKeyboardMarkup:
    keywords = db_get_keywords()
    rows = []
    for kw in keywords:
        rows.append([
            InlineKeyboardButton(text=f"🔑 {kw['word']}", callback_data=f"kw:_:{kw['id']}"),
            InlineKeyboardButton(text="🗑", callback_data=f"kw:del:{kw['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="➕ Добавить слово", callback_data="kw:add")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ── Проверка доступа ──────────────────────────────────────────────────────────

@dp.message.middleware()
async def admin_mw(handler, event: Message, data: dict):
    if ADMIN_IDS and event.from_user.id not in ADMIN_IDS:
        await event.answer("⛔ Нет доступа.")
        return
    return await handler(event, data)

# ── /start ────────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    await msg.answer(
        "👋 <b>Telegram Parser Bot</b>\n\n"
        "Мониторит посты в каналах по ключевым словам "
        "и присылает совпадения сюда.\n\n"
        "Используйте кнопки меню для управления.",
        parse_mode="HTML", reply_markup=kb_main()
    )

# ── КАНАЛЫ ────────────────────────────────────────────────────────────────────

@dp.message(F.text == "📋 Каналы")
async def show_channels(msg: Message):
    chs = db_get_channels()
    await msg.answer(
        f"📋 <b>Каналы</b> ({len(chs)}):",
        parse_mode="HTML", reply_markup=kb_channels()
    )

@dp.callback_query(F.data == "ch:add")
async def cb_ch_add(call: CallbackQuery, state: FSMContext):
    await call.message.answer(
        "Отправьте ссылку или юзернейм канала:\n"
        "<code>@channelname</code> или <code>https://t.me/channelname</code>",
        parse_mode="HTML", reply_markup=kb_cancel()
    )
    await state.set_state(AddChannel.waiting)
    await call.answer()

@dp.message(StateFilter(AddChannel.waiting))
async def process_add_channel(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=kb_main())
        return

    raw = msg.text.strip()
    username = re.sub(r"https?://t\.me/", "@", raw)
    if not username.startswith("@"):
        username = "@" + username

    loading = await msg.answer("⏳ Проверяю канал...")
    global parser
    try:
        if parser is None:
            parser = ParserClient()
            await parser.start()

        info = await parser.get_channel_info(username)
        if not info:
            await loading.edit_text("❌ Канал не найден или нет доступа.")
            await state.clear()
            await msg.answer("Попробуйте другой.", reply_markup=kb_main())
            return

        db_add_channel(info["username"], info["title"], info["id"])
        await loading.edit_text(
            f"✅ <b>{info['title']}</b> добавлен!\n"
            f"👤 {info['username']}  👥 {info['members_count']}",
            parse_mode="HTML"
        )
    except Exception as e:
        log.error(e)
        await loading.edit_text(f"❌ Ошибка: {e}")

    await state.clear()
    await msg.answer("Управление:", reply_markup=kb_main())

@dp.callback_query(F.data.startswith("ch:toggle:"))
async def cb_ch_toggle(call: CallbackQuery):
    ch_id = int(call.data.split(":")[2])
    status = db_toggle_channel(ch_id)
    await call.answer("✅ Активирован" if status else "⏸ Приостановлен")
    await call.message.edit_reply_markup(reply_markup=kb_channels())

@dp.callback_query(F.data.startswith("ch:del:"))
async def cb_ch_delete(call: CallbackQuery):
    ch_id = int(call.data.split(":")[2])
    db_delete_channel(ch_id)
    await call.answer("🗑 Удалён")
    await call.message.edit_reply_markup(reply_markup=kb_channels())

# ── КЛЮЧЕВЫЕ СЛОВА ────────────────────────────────────────────────────────────

@dp.message(F.text == "🔑 Ключевые слова")
async def show_keywords(msg: Message):
    kws = db_get_keywords()
    await msg.answer(
        f"🔑 <b>Ключевые слова</b> ({len(kws)}):",
        parse_mode="HTML", reply_markup=kb_keywords()
    )

@dp.callback_query(F.data == "kw:add")
async def cb_kw_add(call: CallbackQuery, state: FSMContext):
    await call.message.answer(
        "Введите ключевое слово или фразу:",
        reply_markup=kb_cancel()
    )
    await state.set_state(AddKeyword.waiting)
    await call.answer()

@dp.message(StateFilter(AddKeyword.waiting))
async def process_add_keyword(msg: Message, state: FSMContext):
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=kb_main())
        return
    word = msg.text.strip().lower()
    if len(word) < 2:
        await msg.answer("Минимум 2 символа.")
        return
    db_add_keyword(word)
    await state.clear()
    await msg.answer(f"✅ Слово <b>«{word}»</b> добавлено!", parse_mode="HTML", reply_markup=kb_main())

@dp.callback_query(F.data.startswith("kw:del:"))
async def cb_kw_delete(call: CallbackQuery):
    kw_id = int(call.data.split(":")[2])
    db_delete_keyword(kw_id)
    await call.answer("🗑 Удалено")
    await call.message.edit_reply_markup(reply_markup=kb_keywords())

@dp.callback_query(F.data.startswith("kw:_:"))
async def cb_kw_noop(call: CallbackQuery):
    await call.answer()

# ── СТАТИСТИКА ────────────────────────────────────────────────────────────────

@dp.message(F.text == "📊 Статистика")
async def show_stats(msg: Message):
    s = db_get_stats()
    running = parser_task is not None and not parser_task.done()
    await msg.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"{'🟢 Парсер запущен' if running else '🔴 Парсер остановлен'}\n\n"
        f"📋 Каналов: <b>{s['channels']}</b>  (активных: {s['active_channels']})\n"
        f"🔑 Слов: <b>{s['keywords']}</b>\n"
        f"📨 Совпадений всего: <b>{s['matches']}</b>\n"
        f"📅 За сегодня: <b>{s['today_matches']}</b>\n"
        f"🕐 Последняя проверка: <b>{s['last_check'] or 'никогда'}</b>",
        parse_mode="HTML"
    )

# ── НАСТРОЙКИ ─────────────────────────────────────────────────────────────────

@dp.message(F.text == "⚙️ Настройки")
async def show_settings(msg: Message):
    cfg = db_get_config()
    await msg.answer(
        f"⚙️ <b>Настройки</b>\n\n"
        f"⏱ Интервал: <b>{cfg['interval']} сек</b>\n"
        f"📝 Режим: <b>{'Все совпадения' if cfg['notify_all'] else 'Только новые'}</b>\n\n"
        f"<code>/interval 60</code> — изменить интервал\n"
        f"<code>/unique</code> — переключить режим",
        parse_mode="HTML"
    )

@dp.message(Command("interval"))
async def cmd_interval(msg: Message):
    parts = msg.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await msg.answer("Использование: /interval 60")
        return
    v = max(10, int(parts[1]))
    db_set_config("interval", str(v))
    await msg.answer(f"✅ Интервал: {v} сек")

@dp.message(Command("unique"))
async def cmd_unique(msg: Message):
    cfg = db_get_config()
    new = "0" if cfg["notify_all"] else "1"
    db_set_config("notify_all", new)
    await msg.answer(f"✅ Режим: {'Все совпадения' if new == '1' else 'Только новые'}")

# ── ЗАПУСК / СТОП ─────────────────────────────────────────────────────────────

@dp.message(F.text == "▶️ Запустить")
async def cmd_start_parser(msg: Message):
    global parser, parser_task

    if parser_task and not parser_task.done():
        await msg.answer("⚠️ Парсер уже запущен.")
        return

    channels = db_get_channels(active_only=True)
    keywords = db_get_keywords()
    if not channels:
        await msg.answer("❌ Нет активных каналов.")
        return
    if not keywords:
        await msg.answer("❌ Нет ключевых слов.")
        return

    if parser is None:
        parser = ParserClient()

    m = await msg.answer("⏳ Запускаю...")
    try:
        await parser.start()
        parser_task = asyncio.create_task(parser_loop(msg.from_user.id))
        cfg = db_get_config()
        await m.edit_text(
            f"🟢 <b>Парсер запущен!</b>\n\n"
            f"📋 Каналов: {len(channels)}\n"
            f"🔑 Слов: {len(keywords)}\n"
            f"⏱ Интервал: {cfg['interval']} сек",
            parse_mode="HTML"
        )
    except Exception as e:
        await m.edit_text(f"❌ Ошибка: {e}")
        log.error(e)

@dp.message(F.text == "⏹ Остановить")
async def cmd_stop_parser(msg: Message):
    global parser_task, parser
    if not parser_task or parser_task.done():
        await msg.answer("⚠️ Парсер не запущен.")
        return
    parser_task.cancel()
    parser_task = None
    if parser:
        await parser.stop()
        parser = None
    await msg.answer("🔴 <b>Парсер остановлен.</b>", parse_mode="HTML")

# ═══════════════════════════════════════════════════════════════════════════════
#  ЦИКЛ ПАРСИНГА
# ═══════════════════════════════════════════════════════════════════════════════

async def parser_loop(admin_id: int):
    log.info("Цикл парсера запущен")
    while True:
        try:
            cfg      = db_get_config()
            interval = cfg["interval"]
            notify_all = cfg["notify_all"]
            channels = db_get_channels(active_only=True)
            kw_list  = [kw["word"] for kw in db_get_keywords()]

            if channels and kw_list:
                db_update_last_check()
                for ch in channels:
                    try:
                        posts = await parser.get_recent_posts(ch["username"], limit=20)
                        for post in posts:
                            if not post.get("text"):
                                continue
                            text_l  = post["text"].lower()
                            matched = [kw for kw in kw_list if kw in text_l]
                            if not matched:
                                continue

                            pid = post["id"]
                            if not notify_all and db_post_seen(ch["id"], pid):
                                continue

                            db_save_match(ch["id"], pid, post["text"], matched)

                            preview  = post["text"][:300] + ("…" if len(post["text"]) > 300 else "")
                            kw_str   = ", ".join(f"<code>{k}</code>" for k in matched)
                            date_str = datetime.fromtimestamp(post["date"]).strftime("%d.%m.%Y %H:%M")
                            uname    = ch["username"].lstrip("@")

                            text = (
                                f"🔔 <b>Новое совпадение!</b>\n\n"
                                f"📌 <b>{ch['title'] or ch['username']}</b>\n"
                                f"🔑 {kw_str}\n"
                                f"📅 {date_str}\n\n"
                                f"📝 {preview}\n\n"
                                f"🔗 <a href='https://t.me/{uname}/{pid}'>Открыть пост</a>"
                            )
                            for aid in ADMIN_IDS:
                                try:
                                    await bot.send_message(
                                        aid, text,
                                        parse_mode="HTML",
                                        disable_web_page_preview=True
                                    )
                                except Exception as e:
                                    log.error(f"send to {aid}: {e}")

                    except Exception as e:
                        log.error(f"channel {ch['username']}: {e}")
                        await asyncio.sleep(2)

            await asyncio.sleep(interval)

        except asyncio.CancelledError:
            log.info("Цикл парсера остановлен")
            break
        except Exception as e:
            log.error(f"parser_loop: {e}")
            await asyncio.sleep(30)

# ═══════════════════════════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    db_init()
    log.info("Бот стартует...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
