import os
import json
import logging
import re
import sqlite3
import secrets
import string
from typing import Dict, Any, List, Tuple, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("pandora_faq_bot")

DATA_FILE = "content.json"

BOT_USERNAME_DEFAULT = "PandoraAI_FAQ_bot"
DB_PATH_DEFAULT = "/data/referrals.db"


def load_all_content() -> Dict[str, Any]:
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def get_default_lang(all_content: Dict[str, Any]) -> str:
    default_lang = (all_content.get("default_lang") or "en").strip().lower()
    languages = all_content.get("languages", {})
    if isinstance(languages, dict) and default_lang in languages:
        return default_lang
    if isinstance(languages, dict) and languages:
        return next(iter(languages.keys()))
    return "en"


def user_has_selected_lang(context: ContextTypes.DEFAULT_TYPE, all_content: Dict[str, Any]) -> bool:
    languages = all_content.get("languages", {})
    user_lang = (context.user_data.get("lang") or "").strip().lower()
    return isinstance(languages, dict) and user_lang in languages


def get_lang_from_user(context: ContextTypes.DEFAULT_TYPE, all_content: Dict[str, Any]) -> str:
    languages = all_content.get("languages", {})
    default_lang = get_default_lang(all_content)
    user_lang = (context.user_data.get("lang") or "").strip().lower()
    if isinstance(languages, dict) and user_lang in languages:
        return user_lang
    return default_lang


def get_active_content(context: ContextTypes.DEFAULT_TYPE, all_content: Dict[str, Any]) -> Dict[str, Any]:
    lang = get_lang_from_user(context, all_content)
    languages = all_content.get("languages", {})
    if isinstance(languages, dict) and lang in languages:
        return languages[lang]
    return all_content


def ui_get(content: Dict[str, Any], key: str, fallback: str) -> str:
    ui = content.get("ui", {}) if isinstance(content.get("ui", {}), dict) else {}
    value = ui.get(key)
    return value if isinstance(value, str) and value.strip() else fallback


def get_db_path() -> str:
    return (os.environ.get("REFERRAL_DB_PATH") or DB_PATH_DEFAULT).strip()


def db_connect() -> sqlite3.Connection:
    path = get_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def db_init() -> None:
    conn = db_connect()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS referrers (
            ref_code TEXT PRIMARY KEY,
            owner_telegram_id INTEGER NOT NULL,
            step1_url TEXT NOT NULL,
            step2_url TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            telegram_user_id INTEGER PRIMARY KEY,
            sponsor_code TEXT,
            step1_confirmed INTEGER DEFAULT 0,
            step2_warning_ack INTEGER DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Lightweight migration for older DBs
    try:
        cur.execute("ALTER TABLE users ADD COLUMN step2_warning_ack INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


def generate_ref_code(length: int = 6) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def upsert_user(telegram_user_id: int, sponsor_code: Optional[str] = None) -> None:
    conn = db_connect()
    cur = conn.cursor()

    cur.execute("SELECT telegram_user_id, sponsor_code FROM users WHERE telegram_user_id=?", (telegram_user_id,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            "INSERT INTO users (telegram_user_id, sponsor_code, step1_confirmed, step2_warning_ack) VALUES (?, ?, 0, 0)",
            (telegram_user_id, sponsor_code),
        )
    else:
        existing = row["sponsor_code"]
        if sponsor_code and not existing:
            cur.execute(
                "UPDATE users SET sponsor_code=?, updated_at=CURRENT_TIMESTAMP WHERE telegram_user_id=?",
                (sponsor_code, telegram_user_id),
            )

    conn.commit()
    conn.close()


def get_user_state(telegram_user_id: int) -> Dict[str, Any]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT sponsor_code, step1_confirmed, step2_warning_ack FROM users WHERE telegram_user_id=?",
        (telegram_user_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"sponsor_code": None, "step1_confirmed": False, "step2_warning_ack": False}
    return {
        "sponsor_code": row["sponsor_code"],
        "step1_confirmed": bool(row["step1_confirmed"]),
        "step2_warning_ack": bool(row["step2_warning_ack"]),
    }


def set_step1_confirmed(telegram_user_id: int, confirmed: bool) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (telegram_user_id, sponsor_code, step1_confirmed, step2_warning_ack)
        VALUES (?, NULL, ?, 0)
        ON CONFLICT(telegram_user_id) DO UPDATE SET
            step1_confirmed=excluded.step1_confirmed,
            updated_at=CURRENT_TIMESTAMP
        """,
        (telegram_user_id, 1 if confirmed else 0),
    )
    conn.commit()
    conn.close()


def set_step2_warning_ack(telegram_user_id: int, ack: bool) -> None:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (telegram_user_id, sponsor_code, step1_confirmed, step2_warning_ack)
        VALUES (?, NULL, 0, ?)
        ON CONFLICT(telegram_user_id) DO UPDATE SET
            step2_warning_ack=excluded.step2_warning_ack,
            updated_at=CURRENT_TIMESTAMP
        """,
        (telegram_user_id, 1 if ack else 0),
    )
    conn.commit()
    conn.close()


def get_referrer_by_owner(owner_telegram_id: int) -> Optional[Dict[str, Any]]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT ref_code, step1_url, step2_url FROM referrers WHERE owner_telegram_id=?", (owner_telegram_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {"ref_code": row["ref_code"], "step1_url": row["step1_url"], "step2_url": row["step2_url"]}


def get_referrer_by_code(ref_code: str) -> Optional[Dict[str, Any]]:
    conn = db_connect()
    cur = conn.cursor()
    cur.execute("SELECT ref_code, step1_url, step2_url FROM referrers WHERE ref_code=?", (ref_code,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {"ref_code": row["ref_code"], "step1_url": row["step1_url"], "step2_url": row["step2_url"]}


def upsert_referrer(owner_telegram_id: int, step1_url: str, step2_url: str) -> Dict[str, Any]:
    existing = get_referrer_by_owner(owner_telegram_id)
    conn = db_connect()
    cur = conn.cursor()

    if existing:
        ref_code = existing["ref_code"]
        cur.execute("UPDATE referrers SET step1_url=?, step2_url=? WHERE ref_code=?", (step1_url, step2_url, ref_code))
    else:
        ref_code = generate_ref_code()
        while get_referrer_by_code(ref_code):
            ref_code = generate_ref_code()
        cur.execute(
            "INSERT INTO referrers (ref_code, owner_telegram_id, step1_url, step2_url) VALUES (?, ?, ?, ?)",
            (ref_code, owner_telegram_id, step1_url, step2_url),
        )

    conn.commit()
    conn.close()
    return {"ref_code": ref_code, "step1_url": step1_url, "step2_url": step2_url}


def looks_like_url(text: str) -> bool:
    return bool(re.match(r"^https?://", (text or "").strip(), flags=re.IGNORECASE))


def get_bot_username() -> str:
    return (os.environ.get("BOT_USERNAME") or BOT_USERNAME_DEFAULT).strip()


def build_invite_link(ref_code: str) -> str:
    return f"https://t.me/{get_bot_username()}?start={ref_code}"


def build_main_menu(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(ui_get(content, "menu_language", "🌍 Language"), callback_data="menu:language")],
        [InlineKeyboardButton(ui_get(content, "menu_set_links", "🔗 Set Referral Links"), callback_data="menu:set_links")],
        [InlineKeyboardButton(ui_get(content, "menu_share_invite", "📩 Share My Invite Link"), callback_data="menu:share_invite")],
        [InlineKeyboardButton(ui_get(content, "menu_about", "❓ What is Pandora AI?"), callback_data="menu:about")],
        [InlineKeyboardButton(ui_get(content, "menu_presentations", "🎥 Presentations"), callback_data="menu:presentations")],
        [InlineKeyboardButton(ui_get(content, "menu_join", "🤝 How to Join"), callback_data="menu:join")],
        [InlineKeyboardButton(ui_get(content, "menu_corporate", "🏢 Corporate Info"), callback_data="menu:corporate")],
        [InlineKeyboardButton(ui_get(content, "menu_faq", "📌 FAQ"), callback_data="menu:faq")],
        [InlineKeyboardButton(ui_get(content, "menu_support", "🧑‍💻 Support"), callback_data="menu:support")],
        [InlineKeyboardButton(ui_get(content, "menu_disclaimer", "⚠️ Disclaimer"), callback_data="menu:disclaimer")],
    ]
    return InlineKeyboardMarkup(keyboard)


def back_to_menu_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]])


def links_list_kb(content: Dict[str, Any], items: List[Dict[str, str]], back_target: str) -> InlineKeyboardMarkup:
    keyboard: List[List[InlineKeyboardButton]] = []
    for item in items:
        title = item.get("title", "Link")
        url = item.get("url", "")
        if url:
            keyboard.append([InlineKeyboardButton(title, url=url)])
    keyboard.append([InlineKeyboardButton(ui_get(content, "back", "⬅️ Back"), callback_data=f"menu:{back_target}")])
    keyboard.append([InlineKeyboardButton(ui_get(content, "home", "🏠 Home"), callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)


def about_kb(content: Dict[str, Any], url: str) -> InlineKeyboardMarkup:
    watch_label = ui_get(content, "about_watch_btn", "🎥 Watch the short presentation")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(watch_label, url=url)],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def join_home_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "join_step1_btn", "🤝 Step One – Register and Trade"), callback_data="join:step1")],
        [InlineKeyboardButton(ui_get(content, "join_step2_btn", "🗣 Step Two – Become an Affiliate"), callback_data="join:step2")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def join_step1_kb(content: Dict[str, Any], sponsor_step1_url: Optional[str], step1_doc_url: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if sponsor_step1_url:
        rows.append([InlineKeyboardButton(ui_get(content, "join_open_step1", "🔗 Register & Trade (Sponsor Link)"), url=sponsor_step1_url)])
    if step1_doc_url:
        rows.append([InlineKeyboardButton(ui_get(content, "join_open_step1_doc", "📄 Step 1 Setup Document"), url=step1_doc_url)])
    rows.append([InlineKeyboardButton(ui_get(content, "join_confirm_step1", "✅ I have completed Step 1"), callback_data="join:confirm_step1")])
    rows.append([InlineKeyboardButton(ui_get(content, "join_back", "⬅️ Back"), callback_data="menu:join")])
    rows.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def join_step2_locked_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "join_go_step1", "➡️ Go to Step 1"), callback_data="join:step1")],
        [InlineKeyboardButton(ui_get(content, "join_back", "⬅️ Back"), callback_data="menu:join")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def join_step2_ack_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "join_step2_ack_btn", "✅ I understand this warning"), callback_data="join:ack_step2_warning")],
        [InlineKeyboardButton(ui_get(content, "join_back", "⬅️ Back"), callback_data="menu:join")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def join_step2_kb(content: Dict[str, Any], sponsor_step2_url: Optional[str], step2_doc_url: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if sponsor_step2_url:
        rows.append([InlineKeyboardButton(ui_get(content, "join_open_step2", "🔗 Become an Affiliate (Sponsor Link)"), url=sponsor_step2_url)])
    if step2_doc_url:
        rows.append([InlineKeyboardButton(ui_get(content, "join_open_step2_doc", "📄 Step 2 Application Document"), url=step2_doc_url)])
    rows.append([InlineKeyboardButton(ui_get(content, "join_back", "⬅️ Back"), callback_data="menu:join")])
    rows.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def language_kb(all_content: Dict[str, Any], active_lang: str) -> InlineKeyboardMarkup:
    languages = all_content.get("languages", {})
    rows: List[List[InlineKeyboardButton]] = []
    if isinstance(languages, dict):
        for lang_code in languages.keys():
            lang_block = languages.get(lang_code, {})
            label = (lang_block.get("language_label") or lang_code.upper()).strip()
            prefix = "✅ " if lang_code == active_lang else ""
            rows.append([InlineKeyboardButton(f"{prefix}{label}", callback_data=f"lang:set:{lang_code}")])
    rows.append([InlineKeyboardButton("⬅️", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def faq_topics_kb(content: Dict[str, Any], faq_topics: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    keyboard: List[List[InlineKeyboardButton]] = []
    for topic in faq_topics:
        tid = (topic.get("id") or "").strip()
        title = (topic.get("title") or "FAQ Topic").strip()
        if tid:
            keyboard.append([InlineKeyboardButton(f"📂 {title}", callback_data=f"faq_topic:{tid}")])
    keyboard.append([InlineKeyboardButton(ui_get(content, "faq_search_btn", "🔎 FAQ Search"), callback_data="faq_search:start")])
    keyboard.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)


def faq_questions_kb(content: Dict[str, Any], topic_id: str, questions: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    keyboard: List[List[InlineKeyboardButton]] = []
    for i, item in enumerate(questions):
        q_text = item.get("q", f"Question {i+1}")
        keyboard.append([InlineKeyboardButton(q_text, callback_data=f"faq_q:{topic_id}:{i}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to topics", callback_data="faq_back_topics")])
    keyboard.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)


def faq_answer_kb(content: Dict[str, Any], topic_id: str, item: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("⬅️ Back to questions", callback_data=f"faq_back_topic:{topic_id}")],
        [InlineKeyboardButton("⬅️ Back to topics", callback_data="faq_back_topics")]
    ]
    if (item.get("button_text") or "").strip() and (item.get("button_action") or "").strip():
        rows.append([InlineKeyboardButton(item["button_text"], callback_data=item["button_action"])])
    rows.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def faq_search_result_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Back to topics", callback_data="faq_back_topics")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def flatten_faq_topics(faq_topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    for t in faq_topics:
        for q in t.get("questions", []):
            flat.append(q)
    return flat


async def safe_show_menu_message(query, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    chat_id = query.message.chat.id
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception as e:
        logger.warning("edit_message_text failed, sending new message instead: %s", e)
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    all_content = load_all_content()

    sponsor_code = None
    if context.args and len(context.args) > 0:
        sponsor_code = (context.args[0] or "").strip().upper()
        if not re.match(r"^[A-Z0-9]{4,12}$", sponsor_code):
            sponsor_code = None

    if update.effective_user:
        upsert_user(update.effective_user.id, sponsor_code=sponsor_code)

    if not user_has_selected_lang(context, all_content):
        default_lang = get_default_lang(all_content)
        default_block = all_content.get("languages", {}).get(default_lang, {})
        title = ui_get(default_block, "language_title", "🌍 Language\n\nChoose your language:")
        await update.message.reply_text(title, reply_markup=language_kb(all_content, active_lang=default_lang))
        return

    content = get_active_content(context, all_content)
    context.user_data["faq_search_mode"] = False
    await update.message.reply_text(content.get("welcome_message", "Welcome!"), reply_markup=build_main_menu(content))


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    all_content = load_all_content()
    content = get_active_content(context, all_content)
    await update.message.reply_text(ui_get(content, "help_text", "Use /start to open the menu."), reply_markup=build_main_menu(content))


async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    action = query.data.split(":", 1)[1]

    if action == "home":
        context.user_data["faq_search_mode"] = False
        if not user_has_selected_lang(context, all_content):
            default_lang = get_default_lang(all_content)
            default_block = all_content.get("languages", {}).get(default_lang, {})
            title = ui_get(default_block, "language_title", "🌍 Language\n\nChoose your language:")
            await safe_show_menu_message(query, context, title, language_kb(all_content, active_lang=default_lang))
            return
        await safe_show_menu_message(query, context, content.get("welcome_message", "Choose an option:"), build_main_menu(content))
        return

    if action == "language":
        active_lang = get_lang_from_user(context, all_content)
        title = ui_get(content, "language_title", "🌍 Language\n\nChoose your language:")
        await safe_show_menu_message(query, context, title, language_kb(all_content, active_lang))
        return

    if action == "set_links":
        context.user_data["awaiting_step1_url"] = True
        context.user_data["awaiting_step2_url"] = False
        await safe_show_menu_message(query, context, ui_get(content, "ref_set_step1_prompt", "Paste Step 1 URL:"), back_to_menu_kb(content))
        return

    if action == "share_invite":
        user_id = query.from_user.id
        ref = get_referrer_by_owner(user_id)
        if not ref:
            await safe_show_menu_message(query, context, ui_get(content, "ref_not_set", "Set your links first."), back_to_menu_kb(content))
            return
        invite = build_invite_link(ref["ref_code"])
        share_text = ui_get(content, "ref_share_text", "Share your invite:\n\n{invite}").replace("{invite}", invite)
        await safe_show_menu_message(query, context, share_text, back_to_menu_kb(content))
        return

    if action == "about":
        about_text = (content.get("about_text") or "").strip() or "Not configured."
        about_url = (content.get("about_url") or "").strip() or "https://www.youtube.com/"
        await safe_show_menu_message(query, context, about_text, about_kb(content, about_url))
        return

    if action == "presentations":
        items = content.get("presentations", [])
        text = ui_get(content, "presentations_title", "🎥 Presentations")
        if not items:
            await safe_show_menu_message(query, context, text + "\n\n" + ui_get(content, "no_links", "No links."), back_to_menu_kb(content))
            return
        await safe_show_menu_message(query, context, text, links_list_kb(content, items, back_target="home"))
        return

    if action == "join":
        await safe_show_menu_message(query, context, ui_get(content, "join_title", "🤝 How to Join\n\nChoose an option:"), join_home_kb(content))
        return

    if action == "corporate":
        items = content.get("documents", [])
        text = ui_get(content, "corporate_title", "🏢 Corporate Info")
        if not items:
            await safe_show_menu_message(query, context, text + "\n\n" + ui_get(content, "no_links", "No links."), back_to_menu_kb(content))
            return
        await safe_show_menu_message(query, context, text, links_list_kb(content, items, back_target="home"))
        return

    if action == "faq":
        context.user_data["faq_search_mode"] = False
        faq_topics = content.get("faq_topics", [])
        if not faq_topics:
            await safe_show_menu_message(query, context, ui_get(content, "no_faq", "No FAQ topics."), back_to_menu_kb(content))
            return
        await safe_show_menu_message(query, context, ui_get(content, "faq_topics_title", "📌 FAQ Topics\n\nChoose a topic:"), faq_topics_kb(content, faq_topics))
        return

    if action == "support":
        context.user_data["faq_search_mode"] = False
        await safe_show_menu_message(query, context, content.get("support_text", "Support"), back_to_menu_kb(content))
        return

    if action == "disclaimer":
        context.user_data["faq_search_mode"] = False
        disclaimer_image_url = (content.get("disclaimer_image_url") or "").strip()
        disclaimer_caption = (content.get("disclaimer_text") or "").strip()
        chat_id = query.message.chat.id
        if not disclaimer_image_url:
            await context.bot.send_message(chat_id=chat_id, text=ui_get(content, "disclaimer_missing", "Missing."), reply_markup=back_to_menu_kb(content))
            return
        if disclaimer_caption:
            await context.bot.send_photo(chat_id=chat_id, photo=disclaimer_image_url, caption=disclaimer_caption[:1024], reply_markup=back_to_menu_kb(content))
        else:
            await context.bot.send_photo(chat_id=chat_id, photo=disclaimer_image_url, reply_markup=back_to_menu_kb(content))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), build_main_menu(content))


async def on_language_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    all_content = load_all_content()
    parts = query.data.split(":")
    if len(parts) == 3 and parts[0] == "lang" and parts[1] == "set":
        lang_code = (parts[2] or "").strip().lower()
        languages = all_content.get("languages", {})
        if isinstance(languages, dict) and lang_code in languages:
            context.user_data["lang"] = lang_code

    content = get_active_content(context, all_content)
    context.user_data["faq_search_mode"] = False
    await safe_show_menu_message(query, context, content.get("welcome_message", "Welcome!"), build_main_menu(content))


async def on_join_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    action = query.data.split(":", 1)[1]
    user_id = query.from_user.id

    state = get_user_state(user_id)
    sponsor_code = state.get("sponsor_code")
    step1_confirmed = state.get("step1_confirmed", False)
    step2_ack = state.get("step2_warning_ack", False)

    sponsor_step1_url = None
    sponsor_step2_url = None
    if sponsor_code:
        ref = get_referrer_by_code(sponsor_code)
        if ref:
            sponsor_step1_url = ref.get("step1_url")
            sponsor_step2_url = ref.get("step2_url")

    step1_doc_url = (content.get("join_step1_doc_url") or "").strip()
    step2_doc_url = (content.get("join_step2_doc_url") or "").strip()

    if action == "step1":
        text = ui_get(content, "join_step1_title", "🤝 Step One – Register and Trade")
        if not sponsor_step1_url:
            text = ui_get(content, "join_no_sponsor", "No sponsor link.")
        await safe_show_menu_message(query, context, text, join_step1_kb(content, sponsor_step1_url, step1_doc_url))
        return

    if action == "confirm_step1":
        set_step1_confirmed(user_id, True)
        set_step2_warning_ack(user_id, False)
        await safe_show_menu_message(query, context, ui_get(content, "join_step1_confirmed", "✅ Step 1 confirmed."), join_home_kb(content))
        return

    if action == "step2":
        if not step1_confirmed:
            await safe_show_menu_message(query, context, ui_get(content, "join_step2_locked", "Step 2 locked."), join_step2_locked_kb(content))
            return

        if not step2_ack:
            text = ui_get(content, "join_step2_title", "🗣 Step Two – Become an Affiliate")
            prompt = ui_get(content, "join_step2_ack_prompt", "Please confirm you understand this warning to continue.")
            await safe_show_menu_message(query, context, f"{text}\n\n{prompt}", join_step2_ack_kb(content))
            return

        text = ui_get(content, "join_step2_title", "🗣 Step Two – Become an Affiliate")
        if not sponsor_step2_url:
            text = ui_get(content, "join_no_sponsor_step2", "No sponsor affiliate link.")
        await safe_show_menu_message(query, context, text, join_step2_kb(content, sponsor_step2_url, step2_doc_url))
        return

    if action == "ack_step2_warning":
        set_step2_warning_ack(user_id, True)
        text = ui_get(content, "join_step2_title", "🗣 Step Two – Become an Affiliate")
        if not sponsor_step2_url:
            text = ui_get(content, "join_no_sponsor_step2", "No sponsor affiliate link.")
        await safe_show_menu_message(query, context, text, join_step2_kb(content, sponsor_step2_url, step2_doc_url))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), join_home_kb(content))


def normalize(text: str) -> str:
    return " ".join(text.lower().strip().split())


def best_faq_match(user_text: str, faq_items: List[Dict[str, Any]]) -> Tuple[int, float]:
    user_words = set(normalize(user_text).split())
    best_idx, best_score = -1, 0.0
    for i, item in enumerate(faq_items):
        q_words = set(normalize(item.get("q", "")).split())
        if not q_words:
            continue
        overlap = len(user_words & q_words)
        score = overlap / max(1, len(q_words))
        if score > best_score:
            best_idx, best_score = i, score
    return best_idx, best_score


async def on_faq_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    all_content = load_all_content()
    content = get_active_content(context, all_content)
    faq_topics = content.get("faq_topics", [])
    data = query.data

    if data == "faq_search:start":
        context.user_data["faq_search_mode"] = True
        await safe_show_menu_message(query, context, ui_get(content, "faq_search_prompt", "Type a keyword."), faq_search_result_kb(content))
        return

    if data == "faq_back_topics":
        context.user_data["faq_search_mode"] = False
        await safe_show_menu_message(query, context, ui_get(content, "faq_topics_title", "📌 FAQ Topics\n\nChoose a topic:"), faq_topics_kb(content, faq_topics))
        return

    if data.startswith("faq_back_topic:"):
        context.user_data["faq_search_mode"] = False
        topic_id = data.split(":", 1)[1]
        topic = next((t for t in faq_topics if t.get("id") == topic_id), None)
        if not topic:
            await safe_show_menu_message(query, context, ui_get(content, "topic_not_found", "Topic not found."), back_to_menu_kb(content))
            return
        questions = topic.get("questions", [])
        await safe_show_menu_message(query, context, f"📂 {topic.get('title','FAQ')}\n\n{ui_get(content,'select_question','Select a question:')}", faq_questions_kb(content, topic_id, questions))
        return

    if data.startswith("faq_topic:"):
        context.user_data["faq_search_mode"] = False
        topic_id = data.split(":", 1)[1]
        topic = next((t for t in faq_topics if t.get("id") == topic_id), None)
        if not topic:
            await safe_show_menu_message(query, context, ui_get(content, "topic_not_found", "Topic not found."), back_to_menu_kb(content))
            return
        questions = topic.get("questions", [])
        if not questions:
            await safe_show_menu_message(query, context, ui_get(content, "no_questions", "No questions in this topic yet."), back_to_menu_kb(content))
            return
        await safe_show_menu_message(query, context, f"📂 {topic.get('title','FAQ')}\n\n{ui_get(content,'select_question','Select a question:')}", faq_questions_kb(content, topic_id, questions))
        return

    if data.startswith("faq_q:"):
        context.user_data["faq_search_mode"] = False
        parts = data.split(":")
        if len(parts) != 3:
            await safe_show_menu_message(query, context, ui_get(content, "invalid_selection", "Invalid selection."), back_to_menu_kb(content))
            return
        topic_id = parts[1]
        try:
            q_idx = int(parts[2])
        except ValueError:
            await safe_show_menu_message(query, context, ui_get(content, "invalid_selection", "Invalid selection."), back_to_menu_kb(content))
            return

        topic = next((t for t in faq_topics if t.get("id") == topic_id), None)
        if not topic:
            await safe_show_menu_message(query, context, ui_get(content, "topic_not_found", "Topic not found."), back_to_menu_kb(content))
            return
        questions = topic.get("questions", [])
        if q_idx < 0 or q_idx >= len(questions):
            await safe_show_menu_message(query, context, ui_get(content, "question_not_found", "Question not found."), back_to_menu_kb(content))
            return

        item = questions[q_idx]
        q = item.get("q", "Question")
        a = item.get("a", "Answer")
        extra = (item.get("link", "") or "").strip()

        text = f"{q}\n\n{a}"
        if extra:
            text += f"\n\n{ui_get(content,'more_info','More info:')} {extra}"

        await safe_show_menu_message(query, context, text, faq_answer_kb(content, topic_id, item))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), back_to_menu_kb(content))


async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    msg = update.message.text.strip()
    user_id = update.effective_user.id if update.effective_user else None

    if context.user_data.get("awaiting_step1_url") is True:
        if not looks_like_url(msg):
            await update.message.reply_text(ui_get(content, "ref_invalid_url", "Invalid URL."), reply_markup=build_main_menu(content))
            return
        context.user_data["temp_step1_url"] = msg
        context.user_data["awaiting_step1_url"] = False
        context.user_data["awaiting_step2_url"] = True
        await update.message.reply_text(ui_get(content, "ref_set_step2_prompt", "Now paste Step 2 URL:"), reply_markup=build_main_menu(content))
        return

    if context.user_data.get("awaiting_step2_url") is True:
        if not looks_like_url(msg):
            await update.message.reply_text(ui_get(content, "ref_invalid_url", "Invalid URL."), reply_markup=build_main_menu(content))
            return
        step1_url = (context.user_data.get("temp_step1_url") or "").strip()
        if not step1_url or user_id is None:
            context.user_data["awaiting_step2_url"] = False
            await update.message.reply_text(ui_get(content, "ref_flow_error", "Flow error."), reply_markup=build_main_menu(content))
            return
        ref = upsert_referrer(user_id, step1_url=step1_url, step2_url=msg)
        context.user_data["temp_step1_url"] = ""
        context.user_data["awaiting_step2_url"] = False
        invite = build_invite_link(ref["ref_code"])
        done_text = ui_get(content, "ref_saved_done", "Saved:\n{invite}").replace("{invite}", invite)
        await update.message.reply_text(done_text, reply_markup=build_main_menu(content))
        return

    faq_items = flatten_faq_topics(content.get("faq_topics", []))
    if not faq_items:
        await update.message.reply_text(ui_get(content, "no_faq", "No FAQs configured."), reply_markup=build_main_menu(content))
        return

    if context.user_data.get("faq_search_mode") is True:
        idx, score = best_faq_match(msg, faq_items)
        context.user_data["faq_search_mode"] = False
        if idx == -1 or score < 0.25:
            await update.message.reply_text(ui_get(content, "search_no_match", "No match."), reply_markup=faq_search_result_kb(content))
            return
        item = faq_items[idx]
        q = item.get("q", "Question")
        a = item.get("a", "Answer")
        extra = (item.get("link", "") or "").strip()
        text = f"🔎 {ui_get(content,'search_result','Search result')}:\n\n{q}\n\n{a}"
        if extra:
            text += f"\n\n{ui_get(content,'more_info','More info:')} {extra}"
        await update.message.reply_text(text, reply_markup=faq_search_result_kb(content))
        return

    idx, score = best_faq_match(msg, faq_items)
    if idx == -1 or score < 0.25:
        await update.message.reply_text(ui_get(content, "typed_no_match", "No match."), reply_markup=build_main_menu(content))
        return

    item = faq_items[idx]
    q = item.get("q", "Question")
    a = item.get("a", "Answer")
    extra = (item.get("link", "") or "").strip()
    text = f"{q}\n\n{a}"
    if extra:
        text += f"\n\n{ui_get(content,'more_info','More info:')} {extra}"
    await update.message.reply_text(text, reply_markup=build_main_menu(content))


def main() -> None:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

    db_init()

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))

    app.add_handler(CallbackQueryHandler(on_menu_click, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(on_language_click, pattern=r"^lang:set:"))
    app.add_handler(CallbackQueryHandler(on_join_click, pattern=r"^join:"))
    app.add_handler(CallbackQueryHandler(on_faq_click, pattern=r"^(faq_topic:|faq_q:|faq_back_|faq_search:)"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))

    logger.info("Bot is starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
