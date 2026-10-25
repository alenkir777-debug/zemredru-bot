"""
Telegram-бот-автопостер: каждый день публикует в канал новости
о земельном законодательстве и земельном рынке.

Как работает:
  1. Забирает свежие материалы из RSS-лент (список в feeds.txt).
  2. Оставляет только то, что касается земли (ключевые слова).
  3. Отсекает уже опубликованное и дубли одной и той же новости.
  4. Нейросеть пишет оригинальный пост своими словами —
     без ссылок, без названий СМИ и сайтов.
  5. Публикует в канал по расписанию (или сначала шлёт админу на одобрение).
"""
import asyncio
import hashlib
import html
import logging
import os
import re
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import feedparser
import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────── Настройки ───────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")            # @my_channel или -100xxxxxxxxxx
ADMIN_ID = int(os.getenv("ADMIN_ID") or 0)          # ваш Telegram ID
TZ = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "10:00").split(",") if t.strip()]
MODERATION = os.getenv("MODERATION", "0") == "1"    # 1 = сначала на одобрение админу
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "48"))
SIGNATURE = os.getenv("SIGNATURE", "")              # подпись в конце поста, напр. "@my_channel"

# Провайдер нейросети:
#   ollama     — бесплатно, нейросеть работает прямо на вашем сервере
#   gemini     — бесплатный тариф Google AI Studio
#   openrouter — есть бесплатные модели (с суффиксом :free)
#   openai     — любой OpenAI-совместимый сервис (укажите OPENAI_BASE_URL)
#   anthropic  — Claude (платно)
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()
LLM_MODEL = os.getenv("LLM_MODEL", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "") or None

PROVIDER_PRESETS = {  # провайдер: (адрес API, модель по умолчанию)
    "ollama": (os.getenv("OLLAMA_URL", "http://localhost:11434") + "/v1", "qwen3:8b"),
    "gemini": ("https://generativelanguage.googleapis.com/v1beta/openai/", "gemini-3.5-flash-lite"),
    "openrouter": ("https://openrouter.ai/api/v1", "qwen/qwen3-235b-a22b:free"),
    "openai": (None, "gpt-4o-mini"),
}

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "news.db"))
FEEDS_FILE = BASE_DIR / "feeds.txt"

log = logging.getLogger("land_news")

# Слова, по которым новость считается «земельной».
LAND_WORDS = [
    "земел", "земл", "участк", "участок", "ижс", "снт", "лпх", "кфх", "дачн",
    "садовод", "огородн", "кадастр", "межеван", "росреестр", "сельхозземл",
    "сельскохозяйственного назначения", "изъяти", "сервитут", "самострой",
    "дальневосточный гектар", "арктический гектар", "вид разрешенного использования",
    "градостроительн", "генплан", "пзз", "земельный налог", "аренда земли",
]
# Темы законодательства — получают приоритет.
LAW_WORDS = [
    "закон", "законопроект", "госдум", "совет федерации", "подписал", "постановлен",
    "приказ", "вступ", "поправк", "кодекс", "верховный суд", "минэкономразвития",
    "минсельхоз", "правительств", "штраф", "налог", "кадастровая стоимость",
]
# Темы рынка.
MARKET_WORDS = [
    "цен", "спрос", "предложени", "рынок", "рынк", "сделк", "ипотек", "стоимост",
    "продаж", "покупател", "аналитик", "подорож", "подешев", "торг", "аукцион",
]


# ─────────────────────────── База данных ───────────────────────────
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(db()) as conn, conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS seen (
                key        TEXT PRIMARY KEY,
                title      TEXT,
                created_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS posts (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                news_key   TEXT,
                text       TEXT,
                status     TEXT,          -- pending | published | rejected
                created_at INTEGER
            );
            """
        )


def mark_seen(key: str, title: str) -> None:
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT OR IGNORE INTO seen(key, title, created_at) VALUES (?,?,?)",
            (key, title, int(time.time())),
        )


def seen_keys() -> set[str]:
    with closing(db()) as conn:
        return {r["key"] for r in conn.execute("SELECT key FROM seen")}


def recent_titles(days: int = 14) -> list[str]:
    since = int(time.time()) - days * 86400
    with closing(db()) as conn:
        return [r["title"] for r in conn.execute(
            "SELECT title FROM seen WHERE created_at >= ?", (since,))]


def save_post(news_key: str, text: str, status: str) -> int:
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO posts(news_key, text, status, created_at) VALUES (?,?,?,?)",
            (news_key, text, status, int(time.time())),
        )
        return cur.lastrowid


def get_post(post_id: int):
    with closing(db()) as conn:
        return conn.execute("SELECT * FROM posts WHERE id=?", (post_id,)).fetchone()


def set_post_status(post_id: int, status: str) -> None:
    with closing(db()) as conn, conn:
        conn.execute("UPDATE posts SET status=? WHERE id=?", (status, post_id))


def stats() -> dict:
    with closing(db()) as conn:
        q = lambda s: conn.execute(s).fetchone()[0]
        return {
            "published": q("SELECT COUNT(*) FROM posts WHERE status='published'"),
            "pending": q("SELECT COUNT(*) FROM posts WHERE status='pending'"),
            "rejected": q("SELECT COUNT(*) FROM posts WHERE status='rejected'"),
            "seen": q("SELECT COUNT(*) FROM seen"),
        }


# ─────────────────────────── Сбор новостей ───────────────────────────
@dataclass
class NewsItem:
    key: str
    title: str
    summary: str
    published: datetime | None
    score: int = 0


def load_feeds() -> list[str]:
    if not FEEDS_FILE.exists():
        return []
    return [
        line.strip() for line in FEEDS_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def clean_text(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def words(text: str) -> set[str]:
    return {w for w in re.findall(r"[а-яёa-z0-9]{4,}", text.lower())}


def similar(a: str, b: str, threshold: float = 0.5) -> bool:
    """Похожи ли два заголовка (одна и та же новость из разных лент)."""
    wa, wb = words(a), words(b)
    if not wa or not wb:
        return False
    return len(wa & wb) / min(len(wa), len(wb)) >= threshold


def score_item(title: str, summary: str) -> int:
    text = f"{title} {summary}".lower()
    land = sum(w in text for w in LAND_WORDS)
    if land == 0:
        return 0
    law = sum(w in text for w in LAW_WORDS)
    market = sum(w in text for w in MARKET_WORDS)
    title_bonus = 3 if any(w in title.lower() for w in LAND_WORDS) else 0
    return land * 2 + law * 2 + market + title_bonus


async def fetch_feed(client: httpx.AsyncClient, url: str) -> list:
    try:
        resp = await client.get(url, timeout=20, follow_redirects=True)
        resp.raise_for_status()
        return feedparser.parse(resp.content).entries
    except Exception as e:  # одна упавшая лента не должна ломать всё
        log.warning("Лента недоступна %s: %s", url, e)
        return []


def entry_date(entry) -> datetime | None:
    for attr in ("published_parsed", "updated_parsed"):
        st = entry.get(attr)
        if st:
            return datetime(*st[:6], tzinfo=ZoneInfo("UTC"))
    return None


async def collect_news() -> list[NewsItem]:
    feeds = load_feeds()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; LandNewsBot/1.0)"}
    async with httpx.AsyncClient(headers=headers) as client:
        results = await asyncio.gather(*(fetch_feed(client, u) for u in feeds))

    already = seen_keys()
    old_titles = recent_titles()
    cutoff = datetime.now(ZoneInfo("UTC")) - timedelta(hours=LOOKBACK_HOURS)
    items: list[NewsItem] = []

    for entries in results:
        for e in entries:
            title = clean_text(e.get("title", ""))
            # Google News добавляет « - Название СМИ» в конец заголовка — убираем
            title = re.sub(r"\s+[-—–]\s+[^-—–]{2,40}$", "", title)
            summary = clean_text(e.get("summary", "") or e.get("description", ""))
            if not title:
                continue
            key = hashlib.sha1((e.get("link") or title).encode()).hexdigest()
            if key in already:
                continue
            pub = entry_date(e)
            if pub and pub < cutoff:
                continue
            sc = score_item(title, summary)
            if sc == 0:
                continue
            if any(similar(title, t) for t in old_titles):
                continue  # эту новость уже публиковали из другой ленты
            if any(similar(title, it.title) for it in items):
                continue
            items.append(NewsItem(key, title, summary[:2500], pub, sc))

    items.sort(key=lambda i: (i.score, i.published or datetime.min.replace(tzinfo=ZoneInfo("UTC"))),
               reverse=True)
    log.info("Подходящих новостей: %d", len(items))
    return items


# ─────────────────────────── Нейросеть ───────────────────────────
SYSTEM_PROMPT = """Ты — редактор Telegram-канала о земле: земельное законодательство и земельный рынок России.
Твоя задача — написать по материалу короткий оригинальный пост для владельцев и покупателей участков.

Правила:
- Пиши полностью своими словами, не копируй фразы из материала.
- Используй ТОЛЬКО факты из материала. Ничего не додумывай: ни цифр, ни дат, ни номеров законов.
- НЕ упоминай источник: никаких названий СМИ, сайтов, агентств, Telegram-каналов,
  никаких «как сообщает», «по данным издания», никаких ссылок и адресов сайтов.
  Госорганы (Госдума, Росреестр, Минэкономразвития, суды) упоминать можно — это суть новости.
- Структура: заголовок в <b>…</b> с подходящим эмодзи; 2–4 коротких абзаца:
  что произошло, что это меняет, что делать владельцу/покупателю (если из материала это следует).
- Длина: 500–900 знаков. Тон — деловой, понятный, без канцелярита и без кликбейта.
- Форматирование — только теги <b> и <i>. Никакого Markdown.
- В конце 2–3 хэштега, например #законодательство #рынокземли #ИЖС.
- Если материал не про землю или из него невозможно понять суть — ответь одним словом SKIP."""


def build_user_prompt(item: NewsItem) -> str:
    date = item.published.astimezone(TZ).strftime("%d.%m.%Y") if item.published else "недавно"
    return (f"Дата материала: {date}\n"
            f"Заголовок: {item.title}\n"
            f"Текст: {item.summary or '(только заголовок)'}\n\n"
            f"Напиши пост по правилам.")


async def ask_llm(system: str, user: str) -> str:
    if LLM_PROVIDER in PROVIDER_PRESETS:
        from openai import AsyncOpenAI
        preset_url, preset_model = PROVIDER_PRESETS[LLM_PROVIDER]
        client = AsyncOpenAI(
            api_key=OPENAI_API_KEY or "ollama",          # Ollama ключ не проверяет
            base_url=OPENAI_BASE_URL or preset_url,
            timeout=900,                                 # локальная модель на процессоре пишет не спеша
        )
        model = LLM_MODEL or preset_model
        if "qwen3" in model.lower():
            user += " /no_think"                         # Qwen3: сразу писать ответ, без долгих «раздумий»
        resp = await client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            temperature=0.5,
            max_tokens=1200,
        )
        return resp.choices[0].message.content or ""
    from anthropic import AsyncAnthropic
    client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
    resp = await client.messages.create(
        model=LLM_MODEL or "claude-sonnet-5-5",
        max_tokens=1200,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")


URL_RE = re.compile(
    r"(https?://\S+|www\.\S+|t\.me/\S+|\b[\w-]+\.(ru|рф|com|net|org|su|info|io|me)\b\S*)",
    re.IGNORECASE)
MENTION_RE = re.compile(r"(?<![\w#])@\w{3,}")
SOURCE_RE = re.compile(
    r"(сообща(ет|ют|ло)|пиш(ет|ут)|переда(ет|ёт|ют)|по данным|по информации|по сведениям|"
    r"источник|подробнее|читайте|смотрите|на сайте|ссылк|telegram-канал|телеграм-канал)",
    re.IGNORECASE)
ALLOWED_TAGS_RE = re.compile(r"</?(?!(?:b|i)\b)[a-zA-Z][^>]*>")
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?…])\s+")


def _bad(fragment: str) -> bool:
    return bool(URL_RE.search(fragment) or MENTION_RE.search(fragment) or SOURCE_RE.search(fragment))


def sanitize_post(text: str) -> str:
    """Страховка: убираем предложения со ссылками, упоминаниями и указанием источника,
    даже если нейросеть их всё-таки вставила."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)  # «размышления» Qwen и др.
    text = text.strip().strip("`")
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)     # markdown-ссылки
    text = text.replace("**", "")
    text = ALLOWED_TAGS_RE.sub("", text)

    paragraphs = []
    for para in text.split("\n"):
        if para.lstrip().startswith("#") or para.lstrip().startswith("<b>"):
            # хэштеги и заголовок: убираем только сами ссылки/упоминания
            para = MENTION_RE.sub("", URL_RE.sub("", para))
        else:
            para = " ".join(s for s in SENTENCE_SPLIT_RE.split(para) if not _bad(s))
        paragraphs.append(para.rstrip())
    text = "\n".join(paragraphs)
    # теги <b>/<i> должны быть сбалансированы, иначе Telegram отклонит сообщение
    for tag in ("b", "i"):
        if text.count(f"<{tag}>") != text.count(f"</{tag}>"):
            text = text.replace(f"<{tag}>", "").replace(f"</{tag}>", "")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if SIGNATURE:
        text += f"\n\n{html.escape(SIGNATURE)}"
    return text[:4000]


async def make_post() -> tuple[NewsItem, str] | None:
    """Находит лучшую свежую новость и пишет по ней пост."""
    items = await collect_news()
    for item in items[:6]:  # пробуем несколько лучших, если нейросеть ответит SKIP
        try:
            raw = await ask_llm(SYSTEM_PROMPT, build_user_prompt(item))
        except Exception as e:
            log.error("Ошибка нейросети: %s", e)
            raise
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
        if raw.upper().startswith("SKIP") or len(raw) < 150:
            mark_seen(item.key, item.title)
            continue
        return item, sanitize_post(raw)
    return None


# ─────────────────────────── Публикация ───────────────────────────
router = Router()
admin_only = F.from_user.id == ADMIN_ID


def moderation_kb(post_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Опубликовать", callback_data=f"pub:{post_id}")
    kb.button(text="🔄 Другая новость", callback_data=f"next:{post_id}")
    kb.button(text="❌ Отклонить", callback_data=f"rej:{post_id}")
    kb.adjust(1, 2)
    return kb.as_markup()


async def notify_admin(bot: Bot, text: str) -> None:
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, text)
        except Exception as e:
            log.warning("Не удалось написать админу: %s", e)


async def run_cycle(bot: Bot, force_moderation: bool | None = None) -> str:
    """Один цикл: собрать → написать → опубликовать/отправить на модерацию."""
    moderation = MODERATION if force_moderation is None else force_moderation
    try:
        result = await make_post()
    except Exception as e:
        msg = f"⚠️ Не удалось подготовить пост: {html.escape(str(e))[:500]}"
        await notify_admin(bot, msg)
        return msg
    if not result:
        msg = "ℹ️ Свежих земельных новостей сейчас нет — пост пропущен."
        await notify_admin(bot, msg)
        return msg

    item, text = result
    mark_seen(item.key, item.title)
    if moderation and ADMIN_ID:
        post_id = save_post(item.key, text, "pending")
        await bot.send_message(ADMIN_ID, "📝 <b>Пост на одобрение:</b>")
        await bot.send_message(ADMIN_ID, text, reply_markup=moderation_kb(post_id))
        return "Пост отправлен вам на одобрение."
    await bot.send_message(CHANNEL_ID, text)
    save_post(item.key, text, "published")
    log.info("Опубликовано: %s", item.title)
    return "Пост опубликован в канале."


@router.message(Command("start", "help"), admin_only)
async def cmd_help(message: Message):
    times = ", ".join(POST_TIMES)
    await message.answer(
        "🏞 <b>Бот земельных новостей</b>\n\n"
        f"Канал: {html.escape(CHANNEL_ID)}\n"
        f"Публикации: каждый день в {times} ({TZ.key})\n"
        f"Модерация: {'включена' if MODERATION else 'выключена'}\n\n"
        "/now — опубликовать пост прямо сейчас\n"
        "/preview — подготовить пост и прислать мне на проверку\n"
        "/stats — статистика\n"
        "/feeds — список лент-источников"
    )


@router.message(Command("now"), admin_only)
async def cmd_now(message: Message, bot: Bot):
    await message.answer("⏳ Ищу свежую новость и готовлю пост…")
    await message.answer(await run_cycle(bot))


@router.message(Command("preview"), admin_only)
async def cmd_preview(message: Message, bot: Bot):
    await message.answer("⏳ Готовлю пост на проверку…")
    result = await run_cycle(bot, force_moderation=True)
    if not result.startswith("Пост отправлен"):
        await message.answer(result)


@router.message(Command("stats"), admin_only)
async def cmd_stats(message: Message):
    s = stats()
    await message.answer(
        f"📊 Опубликовано: <b>{s['published']}</b>\n"
        f"Ждут одобрения: {s['pending']}\nОтклонено: {s['rejected']}\n"
        f"Просмотрено новостей: {s['seen']}"
    )


@router.message(Command("feeds"), admin_only)
async def cmd_feeds(message: Message):
    feeds = load_feeds()
    body = "\n".join(f"• <code>{html.escape(f)}</code>" for f in feeds) or "пусто"
    await message.answer(f"📡 Ленты ({len(feeds)}):\n{body}\n\nРедактируются в файле feeds.txt")


@router.callback_query(F.data.startswith("pub:"), admin_only)
async def cb_publish(cb: CallbackQuery, bot: Bot):
    post_id = int(cb.data.split(":")[1])
    post = get_post(post_id)
    if not post or post["status"] != "pending":
        await cb.answer("Пост уже обработан", show_alert=True)
        return
    await bot.send_message(CHANNEL_ID, post["text"])
    set_post_status(post_id, "published")
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer("Опубликовано ✅")


@router.callback_query(F.data.startswith("rej:"), admin_only)
async def cb_reject(cb: CallbackQuery):
    set_post_status(int(cb.data.split(":")[1]), "rejected")
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer("Отклонено")


@router.callback_query(F.data.startswith("next:"), admin_only)
async def cb_next(cb: CallbackQuery, bot: Bot):
    set_post_status(int(cb.data.split(":")[1]), "rejected")
    await cb.message.edit_reply_markup(reply_markup=None)
    await cb.answer("Готовлю другой пост…")
    result = await run_cycle(bot, force_moderation=True)
    if not result.startswith("Пост отправлен"):
        await cb.message.answer(result)


# ─────────────────────────── Расписание ───────────────────────────
def next_run(now: datetime) -> datetime:
    candidates = []
    for t in POST_TIMES:
        h, m = map(int, t.split(":"))
        run = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if run <= now:
            run += timedelta(days=1)
        candidates.append(run)
    return min(candidates)


async def scheduler(bot: Bot) -> None:
    while True:
        now = datetime.now(TZ)
        nxt = next_run(now)
        log.info("Следующая публикация: %s", nxt.strftime("%d.%m %H:%M"))
        await asyncio.sleep((nxt - now).total_seconds())
        try:
            await run_cycle(bot)
        except Exception as e:
            log.exception("Ошибка цикла публикации")
            await notify_admin(bot, f"⚠️ Ошибка публикации: {html.escape(str(e))[:500]}")


async def run_once() -> int:
    """Режим для GitHub Actions: один пост и выход (расписание задаёт сам GitHub)."""
    init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    try:
        result = await run_cycle(bot, force_moderation=False)
    finally:
        await bot.session.close()
    print(result)
    return 1 if result.startswith("⚠️") else 0


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    missing = [n for n, v in [("BOT_TOKEN", BOT_TOKEN), ("CHANNEL_ID", CHANNEL_ID)] if not v]
    if LLM_PROVIDER == "anthropic" and not ANTHROPIC_API_KEY:
        missing.append("ANTHROPIC_API_KEY")
    if LLM_PROVIDER in ("openai", "gemini", "openrouter") and not OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY")
    if LLM_PROVIDER not in PROVIDER_PRESETS and LLM_PROVIDER != "anthropic":
        raise SystemExit(f"Неизвестный LLM_PROVIDER: {LLM_PROVIDER}")
    if missing:
        raise SystemExit(f"Заполните в .env (или в секретах GitHub): {', '.join(missing)}")

    if "--once" in sys.argv:
        raise SystemExit(await run_once())

    init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    asyncio.create_task(scheduler(bot))
    await notify_admin(bot, f"✅ Бот запущен. Публикации ежедневно в {', '.join(POST_TIMES)}. /help")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
