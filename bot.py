import os
import json
import logging
from typing import Dict, Any, List, Tuple

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
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
# Content loading
# -----------------------------
def load_content() -> Dict[str, Any]:
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def build_main_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("🎥 Presentations", callback_data="menu:presentations")],
        [InlineKeyboardButton("🤝 How to Join", callback_data="menu:join")],
        [InlineKeyboardButton("🏢 Corporate Info", callback_data="menu:corporate")],
        [InlineKeyboardButton("📌 FAQ", callback_data="menu:faq")],
        [InlineKeyboardButton("🧑‍💻 Support", callback_data="menu:support")],
        [InlineKeyboardButton("⚠️ Disclaimer", callback_data="menu:disclaimer")],
    ]
    return InlineKeyboardMarkup(keyboard)


def back_to_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to menu", callback_data="menu:home")]])

def faq_list_kb(faq_items: List[Dict[str, str]]) -> InlineKeyboardMarkup:
    # Each FAQ item becomes a button
    keyboard = []
    for i, item in enumerate(faq_items):
        keyboard.append([InlineKeyboardButton(item["q"], callback_data=f"faq:{i}")])
    keyboard.append([InlineKeyboardButton("⬅️ Back to menu", callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)

def links_list_kb(items: List[Dict[str, str]], back_target: str) -> InlineKeyboardMarkup:
    keyboard = []
    for item in items:
        # Use URL buttons so Telegram opens the link directly
        keyboard.append([InlineKeyboardButton(item["title"], url=item["url"])])
    keyboard.append([InlineKeyboardButton("⬅️ Back", callback_data=f"menu:{back_target}")])
    keyboard.append([InlineKeyboardButton("🏠 Home", callback_data="menu:home")])
    return InlineKeyboardMarkup(keyboard)

# -----------------------------
# Handlers
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    content = load_content()
    welcome = content.get("welcome_message", "Welcome! Choose an option below.")
    await update.message.reply_text(welcome, reply_markup=build_main_menu())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Use /start to open the menu.\n"
        "You can also type a question and I’ll try to match it to an FAQ.",
        reply_markup=build_main_menu(),
    )

async def on_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    content = load_content()
    action = query.data.split(":", 1)[1]

    if action == "home":
        await query.edit_message_text(content.get("welcome_message", "Choose an option:"), reply_markup=build_main_menu())
        return

    if action == "faq":
        faq_items = content.get("faq", [])
        if not faq_items:
            await query.edit_message_text("No FAQs configured yet.", reply_markup=back_to_menu_kb())
            return
        await query.edit_message_text("Select a question:", reply_markup=faq_list_kb(faq_items))
        return

    if action == "presentations":
        items = content.get("presentations", [])
        text = "🎥 Presentations"
        if not items:
            text += "\n\nNo presentation links added yet."
            await query.edit_message_text(text, reply_markup=back_to_menu_kb())
            return
        await query.edit_message_text(text, reply_markup=links_list_kb(items, back_target="home"))
        return

    if action == "corporate":
    items = content.get("documents", [])
    text = "🏢 Corporate Info"
    if not items:
        text += "\n\nNo corporate info links added yet."
        await query.edit_message_text(text, reply_markup=back_to_menu_kb())
        return
    await query.edit_message_text(text, reply_markup=links_list_kb(items, back_target="home"))
    return

    if action == "join":
        join_text = content.get("join_text", "🤝 How to Join\n\nAdd your join steps here.")
        await query.edit_message_text(join_text, reply_markup=back_to_menu_kb())
        return

    if action == "support":
        support_text = content.get("support_text", "🧑‍💻 Support\n\nAdd support instructions here.")
        await query.edit_message_text(support_text, reply_markup=back_to_menu_kb())
        return

    if action == "disclaimer":
    disclaimer_image_url = (content.get("disclaimer_image_url") or "").strip()
    disclaimer_text = content.get("disclaimer_text", "⚠️ Disclaimer\n\n(Disclaimer text not set.)")

    # If an image URL is provided, send it as a photo; otherwise show text.
    if disclaimer_image_url:
        await query.edit_message_text("⚠️ Disclaimer", reply_markup=back_to_menu_kb())
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=disclaimer_image_url,
            caption=disclaimer_text[:1024]  # Telegram caption limit safety
        )
        return

    await query.edit_message_text(disclaimer_text, reply_markup=back_to_menu_kb())
    return


    await query.edit_message_text("Unknown option.", reply_markup=build_main_menu())

async def on_faq_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    content = load_content()
    faq_items = content.get("faq", [])

    try:
        idx = int(query.data.split(":", 1)[1])
        item = faq_items[idx]
    except Exception:
        await query.edit_message_text("Couldn’t find that FAQ item.", reply_markup=back_to_menu_kb())
        return

    q = item.get("q", "Question")
    a = item.get("a", "Answer")
    extra = item.get("link", "").strip()

    text = f"**{q}**\n\n{a}"
    if extra:
        text += f"\n\nMore info: {extra}"

    # Telegram MarkdownV2 is fussy; easiest is to avoid special characters and use plain text
    await query.edit_message_text(text, reply_markup=back_to_menu_kb())

def normalize(text: str) -> str:
    return " ".join(text.lower().strip().split())

def best_faq_match(user_text: str, faq_items: List[Dict[str, str]]) -> Tuple[int, float]:
    """
    Very simple fuzzy matching:
    score = overlap of words between user question and FAQ question.
    Returns (best_index, best_score).
    """
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

async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    DM-only: user types a question → try match to FAQ.
    """
    content = load_content()
    faq_items = content.get("faq", [])
    msg = update.message.text.strip()

    if not faq_items:
        await update.message.reply_text("No FAQs configured yet. Use /start to see the menu.")
        return

    idx, score = best_faq_match(msg, faq_items)

    # Tune threshold: 0.25 works well for short questions
    if idx == -1 or score < 0.25:
        await update.message.reply_text(
            "I didn’t find a close match. Try the FAQ menu, or rephrase your question.\n\nType /start to open the menu.",
            reply_markup=build_main_menu(),
        )
        return

    item = faq_items[idx]
    q = item.get("q", "Question")
    a = item.get("a", "Answer")
    extra = item.get("link", "").strip()

    text = f"{q}\n\n{a}"
    if extra:
        text += f"\n\nMore info: {extra}"

    await update.message.reply_text(text, reply_markup=build_main_menu())

# -----------------------------
# Main
# -----------------------------
def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))

    app.add_handler(CallbackQueryHandler(on_menu_click, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(on_faq_click, pattern=r"^faq:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))

    logger.info("Bot is starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()

