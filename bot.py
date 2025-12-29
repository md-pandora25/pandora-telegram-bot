import os
import json
import logging
from typing import Dict, Any, List, Tuple

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


# -----------------------------
# Content loading + language helpers
# -----------------------------
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
    # fallback: legacy single-language file
    return all_content


def ui_get(content: Dict[str, Any], key: str, fallback: str) -> str:
    ui = content.get("ui", {}) if isinstance(content.get("ui", {}), dict) else {}
    value = ui.get(key)
    return value if isinstance(value, str) and value.strip() else fallback


# -----------------------------
# Keyboards / Menus (localized)
# -----------------------------
def build_main_menu(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(ui_get(content, "menu_language", "🌍 Language"), callback_data="menu:language")],
        # NEW: What is Pandora AI? button between Language and Presentations
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
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]]
    )


def join_steps_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(ui_get(content, "join_step1_btn", "🤝 Step One – Register and Trade"), callback_data="join:step1")],
        [InlineKeyboardButton(ui_get(content, "join_step2_btn", "🗣 Step Two – Become an Affiliate"), callback_data="join:step2")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")],
    ]
    return InlineKeyboardMarkup(keyboard)


def links_list_kb(content: Dict[str, Any], items: List[Dict[str, str]], back_target: str) -> InlineKeyboardMarkup:
    keyboard = []
    for item in items:
        title = item.get("title", ui_get(content, "generic_link", "Link"))
        url = item.get("url", "")
        if url:
            keyboard.append([InlineKeyboardButton(title, url=url)])

    keyboard.append([InlineKeyboardButton(ui_get(content, "back", "⬅️ Back"), callback_data=f"menu:{back_target}")])
    keyboard.append([InlineKeyboardButton(ui_get(content, "home", "🏠 Home"), callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)


def about_kb(content: Dict[str, Any], url: str) -> InlineKeyboardMarkup:
    """
    Button layout for the What is Pandora AI screen:
    - Watch presentation (URL button)
    - Back to menu
    """
    watch_label = ui_get(content, "about_watch_btn", "🎥 Watch the short presentation")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(watch_label, url=url)],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


# -----------------------------
# Language Menu
# -----------------------------
def language_kb(all_content: Dict[str, Any], active_lang: str) -> InlineKeyboardMarkup:
    languages = all_content.get("languages", {})
    rows = []
    if isinstance(languages, dict):
        for lang_code in languages.keys():
            lang_block = languages.get(lang_code, {})
            label = (lang_block.get("language_label") or lang_code.upper()).strip()
            prefix = "✅ " if lang_code == active_lang else ""
            rows.append([InlineKeyboardButton(f"{prefix}{label}", callback_data=f"lang:set:{lang_code}")])

    rows.append([InlineKeyboardButton("⬅️", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


# -----------------------------
# FAQ (Topic-based) + Search (localized)
# -----------------------------
def faq_topics_kb(content: Dict[str, Any], faq_topics: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    keyboard = []
    for topic in faq_topics:
        tid = (topic.get("id") or "").strip()
        title = (topic.get("title") or ui_get(content, "faq_topic_fallback", "FAQ Topic")).strip()
        if tid:
            keyboard.append([InlineKeyboardButton(f"📂 {title}", callback_data=f"faq_topic:{tid}")])

    keyboard.append([InlineKeyboardButton(ui_get(content, "faq_search_btn", "🔎 FAQ Search"), callback_data="faq_search:start")])
    keyboard.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)


def faq_questions_kb(content: Dict[str, Any], topic_id: str, questions: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    keyboard = []
    for i, item in enumerate(questions):
        q_text = item.get("q", f"{ui_get(content, 'question', 'Question')} {i+1}")
        keyboard.append([InlineKeyboardButton(q_text, callback_data=f"faq_q:{topic_id}:{i}")])

    keyboard.append([InlineKeyboardButton(ui_get(content, "back_to_topics", "⬅️ Back to topics"), callback_data="faq_back_topics")])
    keyboard.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)


def faq_answer_kb_with_jump(content: Dict[str, Any], topic_id: str, item: Dict[str, Any]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(ui_get(content, "back_to_questions", "⬅️ Back to questions"), callback_data=f"faq_back_topic:{topic_id}")],
        [InlineKeyboardButton(ui_get(content, "back_to_topics", "⬅️ Back to topics"), callback_data="faq_back_topics")],
    ]

    button_text = (item.get("button_text") or "").strip()
    button_action = (item.get("button_action") or "").strip()
    if button_text and button_action:
        rows.append([InlineKeyboardButton(button_text, callback_data=button_action)])

    rows.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def faq_search_result_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "back_to_topics", "⬅️ Back to topics"), callback_data="faq_back_topics")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")],
    ])


def flatten_faq_topics(faq_topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat: List[Dict[str, Any]] = []
    for t in faq_topics:
        for q in t.get("questions", []):
            flat.append(q)
    return flat


# -----------------------------
# Safe render helper
# -----------------------------
async def safe_show_menu_message(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup: InlineKeyboardMarkup
) -> None:
    """
    Try to edit the current message into a text menu.
    If that fails (e.g., current message is a photo), send a new message instead.
    """
    chat_id = query.message.chat.id
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except Exception as e:
        logger.warning("edit_message_text failed, sending new message instead: %s", e)
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)


# -----------------------------
# Commands
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    - If user has NOT selected a language yet: show Language selector FIRST.
    - If user has selected a language: show normal welcome + main menu in that language.
    """
    all_content = load_all_content()

    if not user_has_selected_lang(context, all_content):
        default_lang = get_default_lang(all_content)
        default_block = all_content.get("languages", {}).get(default_lang, {})
        title = ui_get(default_block, "language_title", "🌍 Language\n\nChoose your language:")
        await update.message.reply_text(
            title,
            reply_markup=language_kb(all_content, active_lang=default_lang)
        )
        return

    content = get_active_content(context, all_content)
    welcome = content.get("welcome_message", ui_get(content, "welcome_fallback", "Welcome! Choose an option below."))
    context.user_data["faq_search_mode"] = False
    await update.message.reply_text(welcome, reply_markup=build_main_menu(content))


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    all_content = load_all_content()
    content = get_active_content(context, all_content)
    await update.message.reply_text(
        ui_get(content, "help_text",
               "Use /start to open the menu.\n"
               "You can also type a question and I’ll try to match it to an FAQ.\n"
               "Tip: Open FAQ → FAQ Search to search by keyword."),
        reply_markup=build_main_menu(content),
    )


# -----------------------------
# Callback handlers
# -----------------------------
async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

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

        await safe_show_menu_message(
            query,
            context,
            content.get("welcome_message", ui_get(content, "welcome_fallback", "Choose an option:")),
            build_main_menu(content),
        )
        return

    if action == "language":
        active_lang = get_lang_from_user(context, all_content)
        title = ui_get(content, "language_title", "🌍 Language\n\nChoose your language:")
        await safe_show_menu_message(query, context, title, language_kb(all_content, active_lang))
        return

    # NEW: What is Pandora AI?
    if action == "about":
        about_text = (content.get("about_text") or "").strip()
        about_url = (content.get("about_url") or "").strip()

        if not about_text:
            about_text = ui_get(content, "about_fallback", "Pandora AI overview is not configured yet.")
        if not about_url:
            about_url = "https://www.youtube.com/"

        await safe_show_menu_message(
            query,
            context,
            about_text,
            about_kb(content, about_url),
        )
        return

    if action == "presentations":
        items = content.get("presentations", [])
        text = ui_get(content, "presentations_title", "🎥 Presentations")
        if not items:
            text += "\n\n" + ui_get(content, "no_links", "No links added yet.")
            await safe_show_menu_message(query, context, text, back_to_menu_kb(content))
            return
        await safe_show_menu_message(query, context, text, links_list_kb(content, items, back_target="home"))
        return

    if action == "join":
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "join_title", "🤝 How to Join\n\nChoose an option:"),
            join_steps_kb(content)
        )
        return

    if action == "corporate":
        items = content.get("documents", [])
        text = ui_get(content, "corporate_title", "🏢 Corporate Info")
        if not items:
            text += "\n\n" + ui_get(content, "no_links", "No links added yet.")
            await safe_show_menu_message(query, context, text, back_to_menu_kb(content))
            return
        await safe_show_menu_message(query, context, text, links_list_kb(content, items, back_target="home"))
        return

    if action == "faq":
        context.user_data["faq_search_mode"] = False
        faq_topics = content.get("faq_topics", [])
        if not faq_topics:
            await safe_show_menu_message(query, context, ui_get(content, "no_faq", "No FAQ topics configured yet."), back_to_menu_kb(content))
            return
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "faq_topics_title", "📌 FAQ Topics\n\nChoose a topic:"),
            faq_topics_kb(content, faq_topics),
        )
        return

    if action == "support":
        context.user_data["faq_search_mode"] = False
        support_text = content.get("support_text", ui_get(content, "support_fallback", "🧑‍💻 Support\n\nAdd support instructions here."))
        await safe_show_menu_message(query, context, support_text, back_to_menu_kb(content))
        return

    if action == "disclaimer":
        context.user_data["faq_search_mode"] = False
        disclaimer_image_url = (content.get("disclaimer_image_url") or "").strip()
        disclaimer_caption = (content.get("disclaimer_text") or "").strip()
        chat_id = query.message.chat.id

        if not disclaimer_image_url:
            await context.bot.send_message(
                chat_id=chat_id,
                text=ui_get(content, "disclaimer_missing", "Disclaimer image is not configured yet. Please contact support."),
                reply_markup=back_to_menu_kb(content)
            )
            return

        if disclaimer_caption:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=disclaimer_image_url,
                caption=disclaimer_caption[:1024],
                reply_markup=back_to_menu_kb(content)
            )
        else:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=disclaimer_image_url,
                reply_markup=back_to_menu_kb(content)
            )
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), build_main_menu(content))


async def on_language_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    all_content = load_all_content()
    data = query.data  # lang:set:xx

    parts = data.split(":")
    if len(parts) == 3 and parts[0] == "lang" and parts[1] == "set":
        lang_code = (parts[2] or "").strip().lower()
        languages = all_content.get("languages", {})
        if isinstance(languages, dict) and lang_code in languages:
            context.user_data["lang"] = lang_code

    content = get_active_content(context, all_content)
    context.user_data["faq_search_mode"] = False
    await safe_show_menu_message(
        query,
        context,
        content.get("welcome_message", ui_get(content, "welcome_fallback", "Choose an option:")),
        build_main_menu(content),
    )


async def on_join_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    all_content = load_all_content()
    content = get_active_content(context, all_content)
    action = query.data.split(":", 1)[1]

    if action == "step1":
        text = content.get("join_step1_text", ui_get(content, "join_step1_fallback", "✅ Step One – Register and Trade\n\n(Configure join_step1_text in content.json)"))
        await safe_show_menu_message(query, context, text, join_steps_kb(content))
        return

    if action == "step2":
        text = content.get("join_step2_text", ui_get(content, "join_step2_fallback", "🤝 Step Two – Become an Affiliate\n\n(Configure join_step2_text in content.json)"))
        await safe_show_menu_message(query, context, text, join_steps_kb(content))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), join_steps_kb(content))


# -----------------------------
# FAQ and text matching (active-language only)
# -----------------------------
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
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "faq_search_prompt", "🔎 FAQ Search\n\nType a keyword or question (example: drawdown, broker, affiliate)."),
            faq_search_result_kb(content)
        )
        return

    if data == "faq_back_topics":
        context.user_data["faq_search_mode"] = False
        if not faq_topics:
            await safe_show_menu_message(query, context, ui_get(content, "no_faq", "No FAQ topics configured yet."), back_to_menu_kb(content))
            return
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "faq_topics_title", "📌 FAQ Topics\n\nChoose a topic:"),
            faq_topics_kb(content, faq_topics),
        )
        return

    if data.startswith("faq_back_topic:"):
        context.user_data["faq_search_mode"] = False
        topic_id = data.split(":", 1)[1]
        topic = next((t for t in faq_topics if t.get("id") == topic_id), None)
        if not topic:
            await safe_show_menu_message(query, context, ui_get(content, "topic_not_found", "Topic not found."), back_to_menu_kb(content))
            return

        questions = topic.get("questions", [])
        await safe_show_menu_message(
            query,
            context,
            f"📂 {topic.get('title', ui_get(content, 'faq_topic_fallback', 'FAQ'))}\n\n{ui_get(content, 'select_question', 'Select a question:')}",
            faq_questions_kb(content, topic_id, questions),
        )
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
            await safe_show_menu_message(
                query,
                context,
                f"📂 {topic.get('title', ui_get(content, 'faq_topic_fallback', 'FAQ'))}\n\n{ui_get(content, 'no_questions', 'No questions in this topic yet.')}",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton(ui_get(content, "back_to_topics", "⬅️ Back to topics"), callback_data="faq_back_topics")],
                    [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
                ])
            )
            return

        await safe_show_menu_message(
            query,
            context,
            f"📂 {topic.get('title', ui_get(content, 'faq_topic_fallback', 'FAQ'))}\n\n{ui_get(content, 'select_question', 'Select a question:')}",
            faq_questions_kb(content, topic_id, questions),
        )
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
        q = item.get("q", ui_get(content, "question", "Question"))
        a = item.get("a", ui_get(content, "answer", "Answer"))
        extra = (item.get("link", "") or "").strip()

        text = f"{q}\n\n{a}"
        if extra:
            text += f"\n\n{ui_get(content, 'more_info', 'More info:')} {extra}"

        await safe_show_menu_message(query, context, text, faq_answer_kb_with_jump(content, topic_id, item))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), back_to_menu_kb(content))


async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    faq_items = flatten_faq_topics(content.get("faq_topics", []))
    msg = update.message.text.strip()

    if not faq_items:
        await update.message.reply_text(
            ui_get(content, "no_faq", "No FAQs configured yet. Use /start to see the menu."),
            reply_markup=build_main_menu(content)
        )
        return

    if context.user_data.get("faq_search_mode") is True:
        idx, score = best_faq_match(msg, faq_items)
        context.user_data["faq_search_mode"] = False

        if idx == -1 or score < 0.25:
            await update.message.reply_text(
                ui_get(content, "search_no_match",
                       "🔎 I didn’t find a close match.\n\nTry a different keyword, or browse the FAQ Topics."),
                reply_markup=faq_search_result_kb(content),
            )
            return

        item = faq_items[idx]
        q = item.get("q", ui_get(content, "question", "Question"))
        a = item.get("a", ui_get(content, "answer", "Answer"))
        extra = (item.get("link", "") or "").strip()

        text = f"🔎 {ui_get(content, 'search_result', 'Search result')}:\n\n{q}\n\n{a}"
        if extra:
            text += f"\n\n{ui_get(content, 'more_info', 'More info:')} {extra}"

        await update.message.reply_text(text, reply_markup=faq_search_result_kb(content))
        return

    idx, score = best_faq_match(msg, faq_items)
    if idx == -1 or score < 0.25:
        await update.message.reply_text(
            ui_get(content, "typed_no_match",
                   "I didn’t find a close match. Try the FAQ menu, or rephrase your question.\n\nTip: Open FAQ → FAQ Search to search by keyword.\n\nType /start to open the menu."),
            reply_markup=build_main_menu(content),
        )
        return

    item = faq_items[idx]
    q = item.get("q", ui_get(content, "question", "Question"))
    a = item.get("a", ui_get(content, "answer", "Answer"))
    extra = (item.get("link", "") or "").strip()

    text = f"{q}\n\n{a}"
    if extra:
        text += f"\n\n{ui_get(content, 'more_info', 'More info:')} {extra}"

    await update.message.reply_text(text, reply_markup=build_main_menu(content))


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

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
