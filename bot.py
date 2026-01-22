import os
import json
import logging
import re
import sqlite3
import secrets
import string
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
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

    # Add version tracking column
    try:
        cur.execute("ALTER TABLE users ADD COLUMN last_seen_version TEXT DEFAULT '0.0.0'")
    except sqlite3.OperationalError:
        pass

    # Add created_at column for tracking first interaction
    try:
        cur.execute("ALTER TABLE users ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


def generate_ref_code(length: int = 6) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def get_bot_version() -> str:
    """Get current bot version from environment variable."""
    return (os.environ.get("BOT_VERSION") or "1.0.0").strip()


def get_user_version(telegram_user_id: int) -> str:
    """Get the last version this user saw."""
    conn = db_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT last_seen_version FROM users WHERE telegram_user_id=?",
            (telegram_user_id,)
        )
        row = cur.fetchone()
        if row and row["last_seen_version"]:
            return row["last_seen_version"]
        return "0.0.0"
    except Exception:
        return "0.0.0"
    finally:
        conn.close()


def update_user_version(telegram_user_id: int, version: str) -> None:
    """Update user's last seen version."""
    conn = db_connect()
    cur = conn.cursor()
    try:
        # Ensure user exists in database
        cur.execute(
            "INSERT OR IGNORE INTO users (telegram_user_id, last_seen_version) VALUES (?, ?)",
            (telegram_user_id, version)
        )
        # Update version
        cur.execute(
            "UPDATE users SET last_seen_version=? WHERE telegram_user_id=?",
            (version, telegram_user_id)
        )
        conn.commit()
    except Exception as e:
        logger.warning(f"Failed to update user version: {e}")
    finally:
        conn.close()


def version_compare(v1: str, v2: str) -> int:
    """
    Compare two version strings.
    Returns: -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
    """
    try:
        parts1 = [int(x) for x in v1.split('.')]
        parts2 = [int(x) for x in v2.split('.')]
        
        # Pad to same length
        while len(parts1) < len(parts2):
            parts1.append(0)
        while len(parts2) < len(parts1):
            parts2.append(0)
        
        for p1, p2 in zip(parts1, parts2):
            if p1 < p2:
                return -1
            if p1 > p2:
                return 1
        return 0
    except Exception:
        # If version parsing fails, treat as equal
        return 0


async def check_and_show_update_notification(
    update: Update, 
    context: ContextTypes.DEFAULT_TYPE,
    all_content: Dict[str, Any]
) -> bool:
    """
    Check if user needs to see update notification.
    Returns True if notification was shown, False otherwise.
    """
    user_id = update.effective_user.id if update.effective_user else None
    if not user_id:
        return False
    
    current_version = get_bot_version()
    user_version = get_user_version(user_id)
    
    # If user's version is older, show notification
    if version_compare(user_version, current_version) < 0:
        content = get_active_content(context, all_content)
        
        title = ui_get(content, "update_notification_title", "🎉 Bot Updated!")
        text = ui_get(content, "update_notification_text", "The bot has been updated with new features!")
        cta = ui_get(content, "update_notification_cta", "\n\nTap /start to explore!")
        
        full_message = f"{title}\n\n{text}{cta}"
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=full_message,
                parse_mode=None
            )
            # Update user's version so they don't see this again
            update_user_version(user_id, current_version)
            return True
        except Exception as e:
            logger.warning(f"Failed to send update notification: {e}")
            # Still update version to avoid repeated failures
            update_user_version(user_id, current_version)
            return False
    
    return False


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


def get_team_stats(ref_code: str) -> Dict[str, Any]:
    """Get statistics for a team based on ref code."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Total team members (people who used this ref code)
    cur.execute(
        "SELECT COUNT(*) as count FROM users WHERE sponsor_code = ?",
        (ref_code,)
    )
    total_team = cur.fetchone()["count"]
    
    # Team members who set up their own links
    cur.execute(
        """
        SELECT COUNT(*) as count 
        FROM users u 
        INNER JOIN referrers r ON u.telegram_user_id = r.owner_telegram_id 
        WHERE u.sponsor_code = ?
        """,
        (ref_code,)
    )
    team_with_links = cur.fetchone()["count"]
    
    # Team members who confirmed Step 1
    cur.execute(
        "SELECT COUNT(*) as count FROM users WHERE sponsor_code = ? AND step1_confirmed = 1",
        (ref_code,)
    )
    team_step1_confirmed = cur.fetchone()["count"]
    
    conn.close()
    
    return {
        "total_team": total_team,
        "team_with_links": team_with_links,
        "team_step1_confirmed": team_step1_confirmed
    }


def calculate_activity_score(visitors: int, active_members: int) -> Tuple[float, str]:
    """
    Calculate activity score (0-5 stars) based on conversion rate and team size.
    Returns (score, stars_display)
    """
    # Member conversion rate score (0-3 stars)
    conversion_rate = (active_members / visitors * 100) if visitors > 0 else 0
    if conversion_rate >= 60:
        conversion_stars = 3.0
    elif conversion_rate >= 40:
        conversion_stars = 2.0
    elif conversion_rate >= 20:
        conversion_stars = 1.0
    else:
        conversion_stars = 0.0
    
    # Team size bonus (0-2 stars)
    if visitors >= 50:
        size_stars = 2.0
    elif visitors >= 30:
        size_stars = 1.5
    elif visitors >= 20:
        size_stars = 1.0
    else:
        size_stars = 0.0
    
    total_score = conversion_stars + size_stars
    
    # Generate star display
    full_stars = int(total_score)
    has_half = (total_score % 1) >= 0.5
    
    stars = "⭐" * full_stars
    if has_half:
        stars += "½"
    
    return (total_score, stars)


def get_user_rank(user_id: int) -> Dict[str, Any]:
    """Get user's rank among all affiliates."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Get user's ref code
    cur.execute("SELECT ref_code FROM referrers WHERE owner_telegram_id = ?", (user_id,))
    user_ref = cur.fetchone()
    
    if not user_ref:
        conn.close()
        return {"rank": 0, "total": 0, "percentile": 0}
    
    user_ref_code = user_ref["ref_code"]
    
    # Get user's team size
    cur.execute("SELECT COUNT(*) as count FROM users WHERE sponsor_code = ?", (user_ref_code,))
    user_team_size = cur.fetchone()["count"]
    
    # Count total affiliates
    cur.execute("SELECT COUNT(DISTINCT ref_code) as count FROM referrers")
    total_affiliates = cur.fetchone()["count"]
    
    # Count how many have larger teams
    cur.execute("""
        SELECT COUNT(DISTINCT sponsor_code) as count
        FROM users
        WHERE sponsor_code IS NOT NULL
        GROUP BY sponsor_code
        HAVING COUNT(*) > ?
    """, (user_team_size,))
    
    better_count = len(cur.fetchall())
    rank = better_count + 1
    
    percentile = int((rank / total_affiliates * 100)) if total_affiliates > 0 else 0
    
    conn.close()
    
    return {
        "rank": rank,
        "total": total_affiliates,
        "percentile": percentile,
        "team_size": user_team_size
    }


def get_growth_stats(ref_code: str) -> Dict[str, Any]:
    """Get growth statistics for the last week and month."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Check if created_at column exists
    cur.execute("PRAGMA table_info(users)")
    columns = [row["name"] for row in cur.fetchall()]
    has_created_at = "created_at" in columns
    
    stats = {
        "visitors_7d": 0,
        "members_7d": 0,
        "visitors_30d": 0,
        "members_30d": 0,
        "has_time_data": has_created_at
    }
    
    if not has_created_at:
        conn.close()
        return stats
    
    try:
        # Visitors in last 7 days
        cur.execute("""
            SELECT COUNT(*) as count FROM users
            WHERE sponsor_code = ?
            AND created_at IS NOT NULL
            AND datetime(created_at) > datetime('now', '-7 days')
        """, (ref_code,))
        stats["visitors_7d"] = cur.fetchone()["count"]
        
        # Members (set links) in last 7 days
        cur.execute("""
            SELECT COUNT(*) as count FROM referrers
            WHERE created_at IS NOT NULL
            AND datetime(created_at) > datetime('now', '-7 days')
            AND owner_telegram_id IN (
                SELECT telegram_user_id FROM users WHERE sponsor_code = ?
            )
        """, (ref_code,))
        stats["members_7d"] = cur.fetchone()["count"]
        
        # Visitors in last 30 days
        cur.execute("""
            SELECT COUNT(*) as count FROM users
            WHERE sponsor_code = ?
            AND created_at IS NOT NULL
            AND datetime(created_at) > datetime('now', '-30 days')
        """, (ref_code,))
        stats["visitors_30d"] = cur.fetchone()["count"]
        
        # Members in last 30 days
        cur.execute("""
            SELECT COUNT(*) as count FROM referrers
            WHERE created_at IS NOT NULL
            AND datetime(created_at) > datetime('now', '-30 days')
            AND owner_telegram_id IN (
                SELECT telegram_user_id FROM users WHERE sponsor_code = ?
            )
        """, (ref_code,))
        stats["members_30d"] = cur.fetchone()["count"]
        
    except Exception:
        pass
    
    conn.close()
    return stats


def get_user_streak(user_id: int) -> int:
    """Get user's current streak (consecutive days active)."""
    # TODO: Implement streak tracking with activity log table
    # For now, return 0 (placeholder)
    return 0


def get_average_stats() -> Dict[str, Any]:
    """Get average statistics across all affiliates."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Get all referrers
    cur.execute("SELECT ref_code FROM referrers")
    all_refs = cur.fetchall()
    
    if not all_refs:
        conn.close()
        return {
            "avg_visitors": 0,
            "avg_members": 0,
            "avg_conversion": 0
        }
    
    total_visitors = 0
    total_members = 0
    
    for ref in all_refs:
        ref_code = ref["ref_code"]
        
        # Count visitors
        cur.execute("SELECT COUNT(*) as count FROM users WHERE sponsor_code = ?", (ref_code,))
        total_visitors += cur.fetchone()["count"]
        
        # Count members
        cur.execute("""
            SELECT COUNT(*) as count FROM users u
            INNER JOIN referrers r ON u.telegram_user_id = r.owner_telegram_id
            WHERE u.sponsor_code = ?
        """, (ref_code,))
        total_members += cur.fetchone()["count"]
    
    count = len(all_refs)
    avg_visitors = int(total_visitors / count) if count > 0 else 0
    avg_members = int(total_members / count) if count > 0 else 0
    avg_conversion = int((total_members / total_visitors * 100)) if total_visitors > 0 else 0
    
    conn.close()
    
    return {
        "avg_visitors": avg_visitors,
        "avg_members": avg_members,
        "avg_conversion": avg_conversion
    }


def get_top10_stats() -> Dict[str, Any]:
    """Get average statistics for top 10% of affiliates."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Get all team sizes
    cur.execute("""
        SELECT sponsor_code, COUNT(*) as team_size
        FROM users
        WHERE sponsor_code IS NOT NULL
        GROUP BY sponsor_code
        ORDER BY team_size DESC
    """)
    
    all_teams = cur.fetchall()
    
    if not all_teams:
        conn.close()
        return {
            "top10_visitors": 0,
            "top10_members": 0
        }
    
    # Get top 10%
    top10_count = max(1, int(len(all_teams) * 0.1))
    top_teams = all_teams[:top10_count]
    
    total_visitors = 0
    total_members = 0
    
    for team in top_teams:
        ref_code = team["sponsor_code"]
        total_visitors += team["team_size"]
        
        # Count members for this team
        cur.execute("""
            SELECT COUNT(*) as count FROM users u
            INNER JOIN referrers r ON u.telegram_user_id = r.owner_telegram_id
            WHERE u.sponsor_code = ?
        """, (ref_code,))
        total_members += cur.fetchone()["count"]
    
    avg_visitors = int(total_visitors / top10_count) if top10_count > 0 else 0
    avg_members = int(total_members / top10_count) if top10_count > 0 else 0
    
    conn.close()
    
    return {
        "top10_visitors": avg_visitors,
        "top10_members": avg_members
    }


def get_personal_stats(user_id: int) -> Dict[str, Any]:
    """Get comprehensive personal statistics for a user."""
    db_init()
    
    # Get user's referrer info
    ref = get_referrer_by_owner(user_id)
    if not ref:
        return None
    
    ref_code = ref["ref_code"]
    
    # Get basic team stats
    team_stats = get_team_stats(ref_code)
    visitors = team_stats["total_team"]
    active_members = team_stats["team_with_links"]
    
    # Get rank
    rank_info = get_user_rank(user_id)
    
    # Get activity score
    score, stars = calculate_activity_score(visitors, active_members)
    
    # Get growth stats
    growth = get_growth_stats(ref_code)
    
    # Get streak
    streak = get_user_streak(user_id)
    
    # Calculate conversion rate
    conversion = int((active_members / visitors * 100)) if visitors > 0 else 0
    
    return {
        "ref_code": ref_code,
        "visitors": visitors,
        "active_members": active_members,
        "conversion": conversion,
        "rank": rank_info["rank"],
        "total_affiliates": rank_info["total"],
        "percentile": rank_info["percentile"],
        "activity_score": score,
        "activity_stars": stars,
        "growth": growth,
        "streak": streak
    }


def get_admin_statistics() -> Dict[str, Any]:
    """Get comprehensive bot statistics for admin."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Total unique users
    cur.execute("SELECT COUNT(*) as count FROM users")
    total_users = cur.fetchone()["count"]
    
    # Generic bot visitors (no sponsor code)
    cur.execute("SELECT COUNT(*) as count FROM users WHERE sponsor_code IS NULL OR sponsor_code = ''")
    generic_visitors = cur.fetchone()["count"]
    
    # Users via referral
    referred_users = total_users - generic_visitors
    
    # Users who set their own links
    cur.execute("SELECT COUNT(*) as count FROM referrers")
    users_with_links = cur.fetchone()["count"]
    
    # Users who confirmed Step 1
    cur.execute("SELECT COUNT(*) as count FROM users WHERE step1_confirmed = 1")
    step1_confirmed = cur.fetchone()["count"]
    
    # Users who acknowledged Step 2
    cur.execute("SELECT COUNT(*) as count FROM users WHERE step2_warning_ack = 1")
    step2_ack = cur.fetchone()["count"]
    
    # Check if created_at column exists
    cur.execute("PRAGMA table_info(users)")
    columns = [row["name"] for row in cur.fetchall()]
    has_created_at = "created_at" in columns
    
    # Users in last 24 hours (if created_at exists)
    users_24h = 0
    links_24h = 0
    users_7d = 0
    links_7d = 0
    
    if has_created_at:
        try:
            cur.execute("""
                SELECT COUNT(*) as count FROM users 
                WHERE created_at IS NOT NULL 
                AND datetime(created_at) > datetime('now', '-1 day')
            """)
            users_24h = cur.fetchone()["count"]
            
            # New links in last 24 hours
            cur.execute("""
                SELECT COUNT(*) as count FROM referrers 
                WHERE created_at IS NOT NULL 
                AND datetime(created_at) > datetime('now', '-1 day')
            """)
            links_24h = cur.fetchone()["count"]
            
            # Users in last 7 days
            cur.execute("""
                SELECT COUNT(*) as count FROM users 
                WHERE created_at IS NOT NULL 
                AND datetime(created_at) > datetime('now', '-7 days')
            """)
            users_7d = cur.fetchone()["count"]
            
            # New links in last 7 days
            cur.execute("""
                SELECT COUNT(*) as count FROM referrers 
                WHERE created_at IS NOT NULL 
                AND datetime(created_at) > datetime('now', '-7 days')
            """)
            links_7d = cur.fetchone()["count"]
        except Exception:
            # If queries fail, just use 0
            pass
    
    conn.close()
    
    return {
        "total_users": total_users,
        "generic_visitors": generic_visitors,
        "referred_users": referred_users,
        "users_with_links": users_with_links,
        "step1_confirmed": step1_confirmed,
        "step2_ack": step2_ack,
        "users_24h": users_24h,
        "links_24h": links_24h,
        "users_7d": users_7d,
        "links_7d": links_7d,
        "has_time_tracking": has_created_at
    }


def get_top_performers(limit: int = 10) -> List[Dict[str, Any]]:
    """Get top performing referrers by team size with engagement metrics."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Check if created_at column exists for growth tracking
    cur.execute("PRAGMA table_info(users)")
    columns = [row["name"] for row in cur.fetchall()]
    has_created_at = "created_at" in columns
    
    # Get top referrers with their team sizes and engagement metrics
    cur.execute("""
        SELECT 
            u.sponsor_code as ref_code,
            COUNT(*) as team_size,
            r.owner_telegram_id,
            COUNT(CASE WHEN team_ref.ref_code IS NOT NULL THEN 1 END) as team_with_links,
            COUNT(CASE WHEN u.step1_confirmed = 1 THEN 1 END) as team_step1_confirmed
        FROM users u
        LEFT JOIN referrers r ON u.sponsor_code = r.ref_code
        LEFT JOIN referrers team_ref ON u.telegram_user_id = team_ref.owner_telegram_id
        WHERE u.sponsor_code IS NOT NULL AND u.sponsor_code != ''
        GROUP BY u.sponsor_code
        ORDER BY team_size DESC
        LIMIT ?
    """, (limit,))
    
    rows = cur.fetchall()
    
    performers = []
    for row in rows:
        ref_code = row["ref_code"]
        team_size = row["team_size"]
        team_with_links = row["team_with_links"]
        team_step1_confirmed = row["team_step1_confirmed"]
        
        # Calculate growth rate (last 7 days) if created_at exists
        team_growth_7d = 0
        if has_created_at:
            try:
                cur.execute("""
                    SELECT COUNT(*) as count FROM users
                    WHERE sponsor_code = ?
                    AND created_at IS NOT NULL
                    AND datetime(created_at) > datetime('now', '-7 days')
                """, (ref_code,))
                team_growth_7d = cur.fetchone()["count"]
            except Exception:
                team_growth_7d = 0
        
        performers.append({
            "ref_code": ref_code,
            "team_size": team_size,
            "team_with_links": team_with_links,
            "team_step1_confirmed": team_step1_confirmed,
            "team_growth_7d": team_growth_7d,
            "owner_telegram_id": row["owner_telegram_id"],
            "has_growth_data": has_created_at
        })
    
    conn.close()
    return performers


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

from urllib.parse import urlparse

def url_domain_contains(url: str, domain: str) -> bool:
    try:
        return domain.lower() in urlparse((url or "").strip()).netloc.lower()
    except Exception:
        return False


# PRIORITY 2 IMPROVEMENT: Dedicated URL validation functions
def validate_step1_url(url: str) -> bool:
    """
    Validate Step 1 URL format.
    Must be an axisfunded.com URL and contain 'pandora' (case-insensitive).
    """
    url_lower = url.lower()
    return ("pandora" in url_lower) and url_domain_contains(url, "axisfunded.com")


def validate_step2_url(url: str) -> bool:
    """
    Validate Step 2 URL format.
    Must be an axisfunded.com URL and contain 'axisfundedaffiliates' (case-insensitive).
    """
    url_lower = url.lower()
    return ("axisfundedaffiliates" in url_lower) and url_domain_contains(url, "axisfunded.com")


def detect_url_type(url: str) -> Optional[str]:
    """
    Detect if URL is step1, step2, or invalid.
    Returns 'step1', 'step2', or None.
    """
    if validate_step1_url(url):
        return 'step1'
    if validate_step2_url(url):
        return 'step2'
    return None


def get_bot_username() -> str:
    return (os.environ.get("BOT_USERNAME") or BOT_USERNAME_DEFAULT).strip()


def build_invite_link(ref_code: str, content: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate invite link. Optional content parameter for future localization if needed.
    """
    return f"https://t.me/{get_bot_username()}?start={ref_code}"


def build_main_menu(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    # Order requested:
    # What is Pandora AI
    # Presentations
    # How to Join
    # Corporate Info
    # FAQ
    # Affiliate Tools (new submenu containing Set Links, Share Invite, Check Links, Stats)
    # Language
    # Official Telegram Channel
    # Support
    # Disclaimer
    official_url = (content.get("official_channel_url") or "https://t.me/Pandora_AI_info").strip()
    keyboard = [
        [InlineKeyboardButton(ui_get(content, "menu_about", "❓ What is Pandora AI?"), callback_data="menu:about")],
        [InlineKeyboardButton(ui_get(content, "menu_presentations", "🎥 Presentations"), callback_data="menu:presentations")],
        [InlineKeyboardButton(ui_get(content, "menu_join", "🤝 How to Join"), callback_data="menu:join")],
        [InlineKeyboardButton(ui_get(content, "menu_corporate", "🏢 Corporate Info"), callback_data="menu:corporate")],
        [InlineKeyboardButton(ui_get(content, "menu_faq", "📌 FAQ"), callback_data="menu:faq")],
        [InlineKeyboardButton(ui_get(content, "menu_affiliate_tools", "🛠 Affiliate Tools"), callback_data="menu:affiliate_tools")],
        [InlineKeyboardButton(ui_get(content, "menu_language", "🌍 Language"), callback_data="menu:language")],
        [InlineKeyboardButton(ui_get(content, "menu_official_channel", "👉🏼 Official Telegram Channel"), url=official_url)],
        [InlineKeyboardButton(ui_get(content, "menu_support", "🧑‍💻 Support"), callback_data="menu:support")],
        [InlineKeyboardButton(ui_get(content, "menu_disclaimer", "⚠️ Disclaimer"), callback_data="menu:disclaimer")],
    ]
    return InlineKeyboardMarkup(keyboard)


def back_to_menu_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]])


def sharing_tools_submenu_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Keyboard with 'Back to Sharing Tools' and 'Back to menu' buttons."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "back_to_sharing_tools", "⬅️ Back to Sharing Tools"), callback_data="menu:affiliate_tools")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def my_stats_hub_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Main My Stats hub with 4 options."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "btn_personal_stats", "📊 Personal Stats"), callback_data="mystats:personal")],
        [InlineKeyboardButton(ui_get(content, "btn_team_stats", "👥 Team Stats"), callback_data="mystats:team_hub")],
        [InlineKeyboardButton(ui_get(content, "btn_my_actions", "⚡ My Actions"), callback_data="mystats:actions")],
        [InlineKeyboardButton(ui_get(content, "btn_my_milestones", "🎖️ My Milestones"), callback_data="mystats:milestones")],
        [InlineKeyboardButton(ui_get(content, "back_to_sharing_tools", "⬅️ Back to Sharing Tools"), callback_data="menu:affiliate_tools")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def personal_stats_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Personal Stats screen keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "btn_activity_help", "❓ How is this calculated?"), callback_data="mystats:activity_help")],
        [InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:hub")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def team_stats_hub_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Team Stats hub keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "btn_team_details", "👥 Team Details"), callback_data="mystats:team_details")],
        [InlineKeyboardButton(ui_get(content, "btn_team_comparison", "📊 Team Comparison"), callback_data="mystats:team_comparison")],
        [InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:hub")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def team_details_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Team Details screen keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "back_to_team_stats", "⬅️ Back to Team Stats"), callback_data="mystats:team_hub")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def team_comparison_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Team Comparison screen keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "back_to_team_stats", "⬅️ Back to Team Stats"), callback_data="mystats:team_hub")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def my_actions_kb(content: Dict[str, Any], ref_code: str, actions: List[str]) -> InlineKeyboardMarkup:
    """My Actions screen with dynamic action buttons for all 9 suggestion types."""
    buttons = []
    
    for action in actions:
        if action == "convert":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_send_followup", "📧 Send Follow-Up Template"), callback_data=f"action:followup:{ref_code}")])
        elif action == "climb":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_share_invite", "📤 Share Invite Link"), callback_data="affiliate:share_invite")])
        elif action == "streak":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_come_back", "🔥 Come Back Tomorrow"), callback_data="action:streak_reminder")])
        elif action == "quality":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_conversion_tips", "📚 Learn Conversion Tips"), callback_data="action:conversion_tips")])
        elif action == "milestone":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_share_to_goal", "📤 Share to Reach Goal"), callback_data="affiliate:share_invite")])
        elif action == "reengage":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_reengage_message", "📧 Send Re-engagement Message"), callback_data=f"action:reengage:{ref_code}")])
        elif action == "celebrate":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_share_achievement", "📣 Share Achievement"), callback_data="affiliate:share_invite")])
        elif action == "weekly_goal":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_set_goal", "⚡ Set Weekly Goal"), callback_data="action:weekly_goal")])
        elif action == "best_time":
            buttons.append([InlineKeyboardButton(ui_get(content, "btn_set_reminder", "⏰ Set Reminder"), callback_data="action:best_time")])
    
    buttons.append([InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:hub")])
    buttons.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    
    return InlineKeyboardMarkup(buttons)


def my_milestones_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """My Milestones screen keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:hub")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def activity_help_popup_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Activity score help popup keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Got it!", callback_data="mystats:personal")]
    ])


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



def ref_links_help_kb(content: Dict[str, Any], help_url: str) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if help_url:
        rows.append([InlineKeyboardButton(ui_get(content, "ref_links_help_btn", "📄 How to find my referral links"), url=help_url)])
    rows.append([InlineKeyboardButton(ui_get(content, "ref_links_have_now_btn", "✅ I have my links now"), callback_data="ref:have_now")])
    rows.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


def my_invite_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Keyboard for My Invite Link submenu with three options."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "share_invite_btn", "📤 Share My Invite Link"), callback_data="invite:share")],
        [InlineKeyboardButton(ui_get(content, "check_ref_links_btn", "🔍 Check My Referral Links"), callback_data="invite:check_links")],
        [InlineKeyboardButton(ui_get(content, "my_team_stats_btn", "📊 My Team Stats"), callback_data="invite:team_stats")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def check_ref_links_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Keyboard for Check My Referral Links screen with share button."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "share_invite_btn", "📤 Share My Invite Link"), callback_data="invite:share")],
        [InlineKeyboardButton(ui_get(content, "back_to_sharing_tools", "⬅️ Back to Sharing Tools"), callback_data="menu:affiliate_tools")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def affiliate_tools_kb(content: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Keyboard for Sharing Tools submenu."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(ui_get(content, "share_invite_btn", "📤 Share My Invite Link"), callback_data="affiliate:share_invite")],
        [InlineKeyboardButton(ui_get(content, "menu_set_links", "🔗 Set Referral Links"), callback_data="affiliate:set_links")],
        [InlineKeyboardButton(ui_get(content, "check_ref_links_btn", "🔍 Check My Referral Links"), callback_data="affiliate:check_links")],
        [InlineKeyboardButton(ui_get(content, "my_team_stats_btn", "📊 My Stats"), callback_data="mystats:hub")],
        [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
    ])


def about_kb(content: Dict[str, Any], url: str) -> InlineKeyboardMarkup:
    """Keyboard for the 'What is Pandora AI?' section.

    Shows (optional) 90s intro button first, then the main 15m presentation button,
    then a back-to-menu button.
    """
    watch_90_label = ui_get(content, "about_watch_90_btn", "🎥 Watch the 90 second intro")
    watch_15_label = ui_get(content, "about_watch_btn", "🎥 Watch the 15m presentation")
    url_90 = (content.get("about_90_url") or "").strip()

    rows: List[List[InlineKeyboardButton]] = []
    if url_90:
        rows.append([InlineKeyboardButton(watch_90_label, url=url_90)])

    if url:
        rows.append([InlineKeyboardButton(watch_15_label, url=url)])

    rows.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


# PRIORITY 3 IMPROVEMENT: Context-aware invalid link keyboard
def ref_invalid_link_kb(content: Dict[str, Any], step: str = "generic") -> InlineKeyboardMarkup:
    """
    Shown when a user pastes an invalid referral URL.
    step: "step1", "step2", or "generic" to provide appropriate context
    """
    help_url = (content.get("ref_links_help_doc_url") or "").strip()
    rows: List[List[InlineKeyboardButton]] = []
    if help_url:
        rows.append([InlineKeyboardButton(ui_get(content, "ref_links_help_btn", "📄 How to find my referral links"), url=help_url)])
    rows.append([InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


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


def get_sponsor_welcome_stats(sponsor_code: str) -> Optional[Dict[str, Any]]:
    """Get sponsor stats for personalized welcome message."""
    conn = db_connect()
    cur = conn.cursor()
    
    # Get sponsor info
    cur.execute("SELECT owner_telegram_id FROM referrers WHERE ref_code = ?", (sponsor_code,))
    sponsor = cur.fetchone()
    
    if not sponsor:
        conn.close()
        return None
    
    owner_id = sponsor["owner_telegram_id"]
    
    # Get team size (total who clicked this sponsor's link)
    cur.execute("SELECT COUNT(*) as count FROM users WHERE sponsor_code = ?", (sponsor_code,))
    team_size = cur.fetchone()["count"]
    
    # Get team with links (people positioned for affiliate income)
    cur.execute("""
        SELECT COUNT(*) as count FROM users u
        LEFT JOIN referrers r ON u.telegram_user_id = r.owner_telegram_id
        WHERE u.sponsor_code = ? AND r.ref_code IS NOT NULL
    """, (sponsor_code,))
    team_with_links = cur.fetchone()["count"]
    
    conn.close()
    
    return {
        "owner_telegram_id": owner_id,
        "team_size": team_size,
        "team_with_links": team_with_links
    }


async def build_personalized_welcome(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    content: Dict[str, Any],
    sponsor_code: Optional[str]
) -> str:
    """Build personalized welcome message based on sponsor's team size."""
    
    # Get new user's first name
    first_name = update.effective_user.first_name or ""
    first_name_with_comma = f", {first_name}" if first_name else ""
    
    # No sponsor - generic welcome
    if not sponsor_code:
        template = ui_get(content, "welcome_generic", "Welcome!")
        return template.replace("{first_name_with_comma}", first_name_with_comma).replace("{first_name}", first_name or "there")
    
    # Get sponsor stats
    stats = get_sponsor_welcome_stats(sponsor_code)
    
    if not stats:
        # Invalid sponsor code - generic welcome
        template = ui_get(content, "welcome_generic", "Welcome!")
        return template.replace("{first_name_with_comma}", first_name_with_comma).replace("{first_name}", first_name or "there")
    
    # Get sponsor's Telegram info
    try:
        sponsor_user = await context.bot.get_chat(stats["owner_telegram_id"])
        sponsor_first_name = sponsor_user.first_name or "Your sponsor"
        sponsor_last_name = sponsor_user.last_name or ""
        sponsor_username = f"@{sponsor_user.username}" if sponsor_user.username else ""
        
        # Build full name
        sponsor_name = sponsor_first_name
        if sponsor_last_name:
            sponsor_name += f" {sponsor_last_name}"
        if sponsor_username:
            sponsor_name += f" {sponsor_username}"
    except Exception:
        sponsor_first_name = "Your sponsor"
        sponsor_name = "Your sponsor"
    
    # Choose template based on team_with_links count
    if stats["team_with_links"] >= 10:
        # Large team - show stats
        template = ui_get(content, "welcome_large_team", "Welcome!")
        message = template.replace("{first_name}", first_name or "there")
        message = message.replace("{sponsor_name}", sponsor_name)
        message = message.replace("{sponsor_first_name}", sponsor_first_name)
        message = message.replace("{team_with_links}", str(stats["team_with_links"]))
        message = message.replace("{team_size}", str(stats["team_size"]))
    else:
        # Small team - encouraging message
        template = ui_get(content, "welcome_small_team", "Welcome!")
        message = template.replace("{first_name}", first_name or "there")
        message = message.replace("{sponsor_name}", sponsor_name)
        message = message.replace("{sponsor_first_name}", sponsor_first_name)
    
    return message


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

    # Check and show update notification if needed
    await check_and_show_update_notification(update, context, all_content)

    if not user_has_selected_lang(context, all_content):
        default_lang = get_default_lang(all_content)
        default_block = all_content.get("languages", {}).get(default_lang, {})
        title = ui_get(default_block, "language_title", "🌍 Language\n\nChoose your language:")
        await update.message.reply_text(title, reply_markup=language_kb(all_content, active_lang=default_lang))
        return

    content = get_active_content(context, all_content)
    context.user_data["faq_search_mode"] = False
    
    # Build personalized welcome message
    welcome_message = await build_personalized_welcome(update, context, content, sponsor_code)
    
    await update.message.reply_text(welcome_message, reply_markup=build_main_menu(content))


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    all_content = load_all_content()
    content = get_active_content(context, all_content)
    await update.message.reply_text(ui_get(content, "help_text", "Use /start to open the menu."), reply_markup=build_main_menu(content))


async def adminstats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only command to view bot statistics."""
    db_init()
    
    # Check if user is admin
    user_id = update.effective_user.id
    admin_ids_str = os.getenv("ADMIN_USER_IDS", "")
    
    # Debug: Show what we're checking
    debug_msg = f"🔍 Debug Info:\n"
    debug_msg += f"Your User ID: {user_id}\n"
    debug_msg += f"ADMIN_USER_IDS env var: '{admin_ids_str}'\n"
    
    if not admin_ids_str:
        # No admins configured
        await update.message.reply_text(
            "❌ ADMIN_USER_IDS environment variable is not set in Railway.\n\n"
            f"Your Telegram User ID is: {user_id}\n\n"
            "Please add this to Railway:\n"
            "Variable: ADMIN_USER_IDS\n"
            f"Value: {user_id}"
        )
        return
    
    try:
        admin_ids = [int(id.strip()) for id in admin_ids_str.split(",") if id.strip()]
        debug_msg += f"Parsed Admin IDs: {admin_ids}\n"
    except ValueError as e:
        # Invalid admin IDs configured
        await update.message.reply_text(
            f"❌ Error parsing ADMIN_USER_IDS: {e}\n\n"
            f"Current value: '{admin_ids_str}'\n"
            f"Your User ID: {user_id}\n\n"
            "Expected format: 123456789,987654321"
        )
        return
    
    debug_msg += f"Is {user_id} in {admin_ids}? {user_id in admin_ids}\n"
    
    if user_id not in admin_ids:
        # Not an admin - show debug info
        await update.message.reply_text(
            f"❌ Access Denied\n\n"
            f"{debug_msg}\n"
            f"You are not in the admin list.\n\n"
            "To add yourself:\n"
            "1. Go to Railway\n"
            "2. Update ADMIN_USER_IDS to include: {user_id}"
        )
        return
    
    # User is admin - generate statistics
    try:
        stats = get_admin_statistics()
        performers = get_top_performers(limit=10)
        
        # Calculate conversion rates
        visitor_to_links = (stats["users_with_links"] / stats["total_users"] * 100) if stats["total_users"] > 0 else 0
        visitor_to_step1 = (stats["step1_confirmed"] / stats["total_users"] * 100) if stats["total_users"] > 0 else 0
        links_to_step1 = (stats["step1_confirmed"] / stats["users_with_links"] * 100) if stats["users_with_links"] > 0 else 0
        
        # Build the report
        report = f"""📊 **Pandora AI Bot Analytics**
Generated: {datetime.now().strftime('%b %d, %Y %I:%M %p')}

{'═'*35}
👥 **USER STATISTICS**
{'═'*35}
Total Unique Users: **{stats['total_users']:,}**
├─ Generic Bot Visitors: {stats['generic_visitors']:,} ({stats['generic_visitors']/stats['total_users']*100:.0f}%)
└─ Via Referral Link: {stats['referred_users']:,} ({stats['referred_users']/stats['total_users']*100:.0f}%)

Users Who Set Links: **{stats['users_with_links']:,}** ({visitor_to_links:.1f}%)
├─ Confirmed Step 1: {stats['step1_confirmed']:,} ({links_to_step1:.0f}%)
└─ Acknowledged Step 2: {stats['step2_ack']:,}

{'═'*35}
🏆 **TOP 10 PERFORMERS** (by team size)
{'═'*35}
"""
        
        # Get user info for top performers
        if performers:
            for i, performer in enumerate(performers, 1):
                owner_id = performer["owner_telegram_id"]
                try:
                    # Try to get user info from Telegram
                    user = await context.bot.get_chat(owner_id)
                    name = user.first_name or "Unknown"
                    username = f"@{user.username}" if user.username else ""
                    display_name = f"{name} {username}".strip()
                except Exception:
                    # If we can't get info, just show ID
                    display_name = f"User {owner_id}"
                
                # Calculate metrics
                team_size = performer['team_size']
                team_with_links = performer['team_with_links']
                team_step1_confirmed = performer['team_step1_confirmed']
                team_growth_7d = performer['team_growth_7d']
                has_growth_data = performer['has_growth_data']
                
                links_percentage = (team_with_links / team_size * 100) if team_size > 0 else 0
                step1_percentage = (team_step1_confirmed / team_size * 100) if team_size > 0 else 0
                
                # Calculate Activity Score (0-5 stars based on engagement)
                # Factors: set links %, step1 confirmed %
                activity_score = 0
                if links_percentage >= 60: activity_score += 2
                elif links_percentage >= 40: activity_score += 1.5
                elif links_percentage >= 20: activity_score += 1
                
                if step1_percentage >= 60: activity_score += 2
                elif step1_percentage >= 40: activity_score += 1.5
                elif step1_percentage >= 20: activity_score += 1
                
                # Add bonus for large teams
                if team_size >= 30: activity_score += 0.5
                elif team_size >= 20: activity_score += 0.3
                
                # Cap at 5 stars
                activity_score = min(5, activity_score)
                stars = "⭐" * int(activity_score)
                if activity_score % 1 >= 0.5:
                    stars += "½"
                
                # Build performer entry
                report += f"{i}. {performer['ref_code']} - {display_name}\n"
                report += f"   • Team Size: **{team_size}**"
                
                # Add growth indicator if available
                if has_growth_data and team_growth_7d > 0:
                    report += f" (+{team_growth_7d} this week)"
                
                report += "\n"
                report += f"   • Set Links: **{team_with_links}** ({links_percentage:.0f}%)\n"
                report += f"   • Confirmed Step 1: **{team_step1_confirmed}** ({step1_percentage:.0f}%)\n"
                report += f"   • Team Activity: {stars} ({activity_score:.1f}/5)\n"
                
                if i < len(performers):
                    report += "\n"
        else:
            report += "No referrers yet.\n\n"
        
        report += f"""
{'═'*35}
📈 **CONVERSION RATES**
{'═'*35}
Visitor → Set Links: {visitor_to_links:.1f}%
Visitor → Confirm Step 1: {visitor_to_step1:.1f}%
Set Links → Confirm Step 1: {links_to_step1:.1f}%

{'═'*35}
📅 **RECENT ACTIVITY**
{'═'*35}
"""
        
        if stats.get('has_time_tracking', False):
            report += f"""**Last 24 Hours:**
• New Users: {stats['users_24h']}
• New Link Setups: {stats['links_24h']}

**Last 7 Days:**
• New Users: {stats['users_7d']}
• New Link Setups: {stats['links_7d']}
"""
        else:
            report += """**Time-based tracking not available yet.**
New users will be tracked from now on.
Check back tomorrow for 24h/7d stats!
"""
        
        report += f"""
{'─'*35}
Updated: Just now
"""
        
        # Send report to admin
        await update.message.reply_text(report, parse_mode='Markdown')
        
    except Exception as e:
        # Show any errors that occur
        await update.message.reply_text(
            f"❌ Error generating statistics:\n\n"
            f"{type(e).__name__}: {str(e)}\n\n"
            "Check Railway logs for details."
        )


async def send_daily_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send daily report to admin users (scheduled task)."""
    db_init()
    
    # Get admin IDs
    admin_ids_str = os.getenv("ADMIN_USER_IDS", "")
    if not admin_ids_str:
        return
    
    try:
        admin_ids = [int(id.strip()) for id in admin_ids_str.split(",") if id.strip()]
    except ValueError:
        return
    
    # Get statistics
    stats = get_admin_statistics()
    
    # Build simplified daily report
    report = f"""📊 **Daily Pandora AI Bot Report**
{datetime.now().strftime('%A, %B %d, %Y')}

{'═'*35}
📊 **CURRENT TOTALS**
{'═'*35}
Total Users: **{stats['total_users']:,}**
Users with Links: **{stats['users_with_links']:,}**
Generic Visitors: {stats['generic_visitors']:,}

{'═'*35}
📈 **YESTERDAY'S ACTIVITY**
{'═'*35}
"""
    
    if stats.get('has_time_tracking', False):
        report += f"""New Users: **{stats['users_24h']}**
New Link Setups: **{stats['links_24h']}**

{'═'*35}
📅 **WEEKLY PROGRESS**
{'═'*35}
New Users (7 days): **{stats['users_7d']}**
New Links (7 days): **{stats['links_7d']}**
"""
    else:
        report += """Time tracking not yet available.
New users will be tracked from now on.
"""
    
    report += f"""
{'─'*35}
Use /adminstats for detailed analytics
"""
    
    # Send to all admin users
    for admin_id in admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=report,
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to send daily report to admin {admin_id}: {e}")


# -----------------------------
# Reset (safe user-only reset)
# Usage:
#   /reset           -> shows confirmation instruction
#   /reset confirm   -> clears ONLY your own bot state (sponsor, step confirmations, in-memory flags)
# Optional: set env RESET_REQUIRE_CONFIRM=false to allow /reset without 'confirm'
# -----------------------------
async def reset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    require_confirm = (os.environ.get("RESET_REQUIRE_CONFIRM", "true").strip().lower() != "false")
    args = context.args or []

    if require_confirm and (len(args) == 0 or (args[0].lower() != "confirm")):
        await update.message.reply_text(
            ui_get(
                content,
                "reset_confirm_prompt", """🔄 Reset test data

This will clear ONLY your personal bot data (sponsor link + Step 1/2 confirmations) so you can retest someone else's invite link.

To confirm, type:
/reset confirm"""),
            reply_markup=build_main_menu(content),
        )
        return

    user_id = update.effective_user.id if update.effective_user else None
    if user_id is None:
        await update.message.reply_text(
            ui_get(content, "reset_error", "Sorry — I couldn't identify your user account."),
            reply_markup=build_main_menu(content),
        )
        return

    conn = db_connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE telegram_user_id = ?", (user_id,))
    conn.commit()
    conn.close()

    context.user_data.clear()

    await update.message.reply_text(
        ui_get(
            content,
            "reset_done", """✅ Your bot data has been reset.

Now open the new invite link you want to test, or type /start.""" ),
        reply_markup=build_main_menu(content),
    )


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
        # Ask a confirmation question before starting link capture
        context.user_data["awaiting_step1_url"] = False
        context.user_data["awaiting_step2_url"] = False
        context.user_data["temp_step1_url"] = ""
        context.user_data["temp_step2_url"] = ""

        question = ui_get(content, "ref_ready_question", "Do you have your Step 1 and Step 2 referral links ready to go?")
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(ui_get(content, "ref_ready_yes", "✅ Yes"), callback_data="ref:ready:yes"),
                InlineKeyboardButton(ui_get(content, "ref_ready_no", "❌ No"), callback_data="ref:ready:no"),
            ],
            [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")],
        ])
        await safe_show_menu_message(query, context, question, kb)
        return

    if action == "share_invite":
        user_id = query.from_user.id
        ref = get_referrer_by_owner(user_id)
        if not ref:
            await safe_show_menu_message(query, context, ui_get(content, "ref_not_set", "Set your links first."), back_to_menu_kb(content))
            return
        # Show submenu with two options
        title = ui_get(content, "my_invite_title", "📩 My Invite Link\n\nChoose an option:")
        await safe_show_menu_message(query, context, title, my_invite_kb(content))
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

    if action == "affiliate_tools":
        title = ui_get(content, "affiliate_tools_title", "🛠 Affiliate Tools\n\nSelect an option:")
        await safe_show_menu_message(query, context, title, affiliate_tools_kb(content))
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



async def on_ref_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    data = query.data or ""

    if data == "ref:ready:yes" or data == "ref:have_now":
        context.user_data["awaiting_step1_url"] = True
        context.user_data["awaiting_step2_url"] = False
        context.user_data["temp_step1_url"] = ""
        context.user_data["temp_step2_url"] = ""
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "ref_set_step1_prompt", "🔗 Set your referral links\n\nPlease paste your full Step 1 (Register & Trade) referral URL now:"),
            back_to_menu_kb(content),
        )
        return

    if data == "ref:ready:no":
        help_url = (content.get("ref_links_help_doc_url") or "").strip()
        help_text = ui_get(
            content,
            "ref_links_help_text",
            "No problem — open the guide below to see where to find your Step 1 and Step 2 referral links.\n\nWhen you have them, tap 'I have my links now'.",
        )
        await safe_show_menu_message(query, context, help_text, ref_links_help_kb(content, help_url))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), back_to_menu_kb(content))


async def on_invite_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler for My Invite Link submenu actions."""
    query = update.callback_query
    await query.answer()

    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    data = query.data or ""
    user_id = query.from_user.id

    # Get user's referral info
    ref = get_referrer_by_owner(user_id)
    if not ref:
        await safe_show_menu_message(query, context, ui_get(content, "ref_not_set", "Set your links first."), back_to_menu_kb(content))
        return

    if data == "invite:share":
        # Share invite link (original functionality)
        invite = build_invite_link(ref["ref_code"], content)
        share_text = ui_get(content, "ref_share_text", "Share your invite:\n\n{invite}").replace("{invite}", invite)
        await safe_show_menu_message(query, context, share_text, back_to_menu_kb(content))
        return

    if data == "invite:check_links":
        # Show user's saved referral links
        step1_url = ref.get("step1_url", "Not set")
        step2_url = ref.get("step2_url", "Not set")
        
        links_template = ui_get(
            content, 
            "my_ref_links_text", 
            "📋 Here are your saved referral links:\n\n🔗 Step 1:\n{step1}\n\n🔗 Step 2:\n{step2}"
        )
        links_text = links_template.replace("{step1}", step1_url).replace("{step2}", step2_url)
        
        title = ui_get(content, "my_ref_links_title", "🔍 Your Referral Links")
        full_text = f"{title}\n\n{links_text}"
        
        await safe_show_menu_message(query, context, full_text, check_ref_links_kb(content))
        return

    if data == "invite:team_stats":
        # Show user's team statistics
        ref_code = ref.get("ref_code", "")
        
        # Get team stats
        stats = get_team_stats(ref_code)
        
        # Build invite link
        invite_link = build_invite_link(ref_code, content)
        
        # Determine growth message
        if stats["total_team"] > 0:
            growth_message = ui_get(content, "my_team_stats_growth", "📈 Your team is growing! Keep sharing!")
        else:
            growth_message = ui_get(content, "my_team_stats_no_team", "No one has used your invite link yet. Share it to start building your team! 🚀")
        
        # Build stats text
        stats_template = ui_get(
            content,
            "my_team_stats_text",
            "Your Ref Code: {ref_code}\nPeople who used your link: {total_team}"
        )
        
        stats_text = stats_template.replace("{ref_code}", ref_code) \
                                    .replace("{invite_link}", invite_link) \
                                    .replace("{total_team}", str(stats["total_team"])) \
                                    .replace("{team_with_links}", str(stats["team_with_links"])) \
                                    .replace("{team_step1_confirmed}", str(stats["team_step1_confirmed"])) \
                                    .replace("{growth_message}", growth_message)
        
        title = ui_get(content, "my_team_stats_title", "📊 Your Team Stats")
        full_text = f"{title}\n\n{stats_text}"
        
        await safe_show_menu_message(query, context, full_text, back_to_menu_kb(content))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), back_to_menu_kb(content))


async def on_affiliate_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle Affiliate Tools submenu actions."""
    query = update.callback_query
    await query.answer()

    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)

    data = query.data or ""
    user_id = query.from_user.id
    action = data.split(":", 1)[1] if ":" in data else ""

    # Get user's referral info (needed for most actions)
    ref = get_referrer_by_owner(user_id)

    if action == "share_invite":
        # Share invite link - requires links to be set
        if not ref:
            await safe_show_menu_message(query, context, ui_get(content, "ref_not_set", "Set your links first."), sharing_tools_submenu_kb(content))
            return
        invite = build_invite_link(ref["ref_code"], content)
        share_text = ui_get(content, "ref_share_text", "Share your invite:\n\n{invite}").replace("{invite}", invite)
        await safe_show_menu_message(query, context, share_text, sharing_tools_submenu_kb(content))
        return

    if action == "set_links":
        # Set referral links - same as menu:set_links
        context.user_data["awaiting_step1_url"] = False
        context.user_data["awaiting_step2_url"] = False
        context.user_data["temp_step1_url"] = ""
        context.user_data["temp_step2_url"] = ""

        question = ui_get(content, "ref_ready_question", "Do you have your Step 1 and Step 2 referral links ready to go?")
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(ui_get(content, "ref_ready_yes", "✅ Yes"), callback_data="ref:ready:yes"),
                InlineKeyboardButton(ui_get(content, "ref_ready_no", "❌ No"), callback_data="ref:ready:no"),
            ],
            [InlineKeyboardButton(ui_get(content, "back_to_sharing_tools", "⬅️ Back to Sharing Tools"), callback_data="menu:affiliate_tools")],
            [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")],
        ])
        await safe_show_menu_message(query, context, question, kb)
        return

    if action == "check_links":
        # Check referral links - requires links to be set
        if not ref:
            await safe_show_menu_message(query, context, ui_get(content, "ref_not_set", "Set your links first."), sharing_tools_submenu_kb(content))
            return
        
        step1_url = ref.get("step1_url", "Not set")
        step2_url = ref.get("step2_url", "Not set")
        
        links_template = ui_get(
            content, 
            "my_ref_links_text", 
            "📋 Here are your saved referral links:\n\n🔗 Step 1:\n{step1}\n\n🔗 Step 2:\n{step2}"
        )
        links_text = links_template.replace("{step1}", step1_url).replace("{step2}", step2_url)
        
        title = ui_get(content, "my_ref_links_title", "🔍 Your Referral Links")
        full_text = f"{title}\n\n{links_text}"
        
        await safe_show_menu_message(query, context, full_text, check_ref_links_kb(content))
        return

    if action == "stats":
        # Show team stats - requires links to be set
        if not ref:
            await safe_show_menu_message(query, context, ui_get(content, "ref_not_set", "Set your links first."), sharing_tools_submenu_kb(content))
            return
        
        ref_code = ref.get("ref_code", "")
        
        # Get team stats
        stats = get_team_stats(ref_code)
        
        # Build invite link
        invite_link = build_invite_link(ref_code, content)
        
        # Determine growth message
        if stats["total_team"] > 0:
            growth_message = ui_get(content, "my_team_stats_growth", "📈 Your team is growing! Keep sharing!")
        else:
            growth_message = ui_get(content, "my_team_stats_no_team", "No one has used your invite link yet. Share it to start building your team! 🚀")
        
        # Build stats text
        stats_template = ui_get(
            content,
            "my_team_stats_text",
            "Your Ref Code: {ref_code}\nPeople who used your link: {total_team}"
        )
        
        stats_text = stats_template.replace("{ref_code}", ref_code) \
                                    .replace("{invite_link}", invite_link) \
                                    .replace("{total_team}", str(stats["total_team"])) \
                                    .replace("{team_with_links}", str(stats["team_with_links"])) \
                                    .replace("{team_step1_confirmed}", str(stats["team_step1_confirmed"])) \
                                    .replace("{growth_message}", growth_message)
        
        title = ui_get(content, "my_team_stats_title", "📊 Your Pandora AI Bot Link Stats")
        full_text = f"{title}\n\n{stats_text}"
        
        await safe_show_menu_message(query, context, full_text, sharing_tools_submenu_kb(content))
        return

    await safe_show_menu_message(query, context, ui_get(content, "unknown_option", "Unknown option."), back_to_menu_kb(content))


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

    # Check and show update notification if needed (before processing message)
    await check_and_show_update_notification(update, context, all_content)

    msg = update.message.text.strip()
    user_id = update.effective_user.id if update.effective_user else None

    # Handle Step 1 URL capture
    if context.user_data.get("awaiting_step1_url") is True:
        if not looks_like_url(msg):
            await update.message.reply_text(
                ui_get(content, "ref_invalid_url", "Invalid URL."), 
                reply_markup=back_to_menu_kb(content)
            )
            return

        url_type = detect_url_type(msg)

        # User pasted Step 2 link first
        if url_type == 'step2':
            context.user_data["temp_step2_url"] = msg
            await update.message.reply_text(
                ui_get(content, "ref_detected_step2_first", "⚠️ I think you pasted your Step 2 link first."),
                reply_markup=back_to_menu_kb(content),
            )
            return

        # Invalid Step 1 URL
        if url_type != 'step1':
            await update.message.reply_text(
                ui_get(content, "ref_invalid_step1_text", "❌ Invalid Step 1 link. Please paste again."),
                reply_markup=ref_invalid_link_kb(content, "step1"),
            )
            return

        # Valid Step 1 URL
        context.user_data["temp_step1_url"] = msg
        context.user_data["awaiting_step1_url"] = False

        # Check if we already have Step 2 from earlier
        pre_step2 = (context.user_data.get("temp_step2_url") or "").strip()
        if pre_step2 and user_id is not None:
            ref = upsert_referrer(user_id, step1_url=msg, step2_url=pre_step2)
            context.user_data["temp_step1_url"] = ""
            context.user_data["temp_step2_url"] = ""
            context.user_data["awaiting_step2_url"] = False
            invite = build_invite_link(ref["ref_code"], content)
            done_tpl = ui_get(content, "ref_saved_done", "✅ Saved! {invite}")
            done_text = done_tpl.replace("{invite}", invite)
            await update.message.reply_text(done_text, reply_markup=build_main_menu(content))
            return

        # Prompt for Step 2
        context.user_data["awaiting_step2_url"] = True
        await update.message.reply_text(
            ui_get(content, "ref_set_step2_prompt", "Now paste Step 2 URL:"), 
            reply_markup=back_to_menu_kb(content)
        )
        return

    # Handle Step 2 URL capture
    if context.user_data.get("awaiting_step2_url") is True:
        if not looks_like_url(msg):
            await update.message.reply_text(
                ui_get(content, "ref_invalid_url", "Invalid URL."), 
                reply_markup=back_to_menu_kb(content)
            )
            return

        url_type = detect_url_type(msg)

        # User pasted Step 1 link here
        if url_type == 'step1':
            context.user_data["temp_step1_url"] = msg
            await update.message.reply_text(
                ui_get(content, "ref_detected_step1_in_step2", "⚠️ I think you pasted your Step 1 link here."),
                reply_markup=back_to_menu_kb(content),
            )
            return

        # Invalid Step 2 URL
        if url_type != 'step2':
            await update.message.reply_text(
                ui_get(content, "ref_invalid_step2_text", "❌ Invalid Step 2 link. Please paste again."),
                reply_markup=ref_invalid_link_kb(content, "step2"),
            )
            return

        # Valid Step 2 URL - save both links
        step1_url = (context.user_data.get("temp_step1_url") or "").strip()
        if not step1_url or user_id is None:
            context.user_data["awaiting_step2_url"] = False
            await update.message.reply_text(
                ui_get(content, "ref_flow_error", "Flow error."), 
                reply_markup=back_to_menu_kb(content)
            )
            return

        ref = upsert_referrer(user_id, step1_url=step1_url, step2_url=msg)
        context.user_data["temp_step1_url"] = ""
        context.user_data["temp_step2_url"] = ""
        context.user_data["awaiting_step2_url"] = False
        invite = build_invite_link(ref["ref_code"], content)
        done_tpl = ui_get(content, "ref_saved_done", "✅ Saved! {invite}")
        done_text = done_tpl.replace("{invite}", invite)
        await update.message.reply_text(done_text, reply_markup=build_main_menu(content))
        return

    # Handle FAQ search or general text matching
    faq_items = flatten_faq_topics(content.get("faq_topics", []))
    if not faq_items:
        await update.message.reply_text(
            ui_get(content, "no_faq", "No FAQs configured."), 
            reply_markup=build_main_menu(content)
        )
        return

    if context.user_data.get("faq_search_mode") is True:
        idx, score = best_faq_match(msg, faq_items)
        context.user_data["faq_search_mode"] = False
        if idx == -1 or score < 0.25:
            await update.message.reply_text(
                ui_get(content, "search_no_match", "No match."), 
                reply_markup=faq_search_result_kb(content)
            )
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

    # General text matching against FAQs
    idx, score = best_faq_match(msg, faq_items)
    if idx == -1 or score < 0.25:
        await update.message.reply_text(
            ui_get(content, "typed_no_match", "No match."), 
            reply_markup=build_main_menu(content)
        )
        return

    item = faq_items[idx]
    q = item.get("q", "Question")
    a = item.get("a", "Answer")
    extra = (item.get("link", "") or "").strip()
    text = f"{q}\n\n{a}"
    if extra:
        text += f"\n\n{ui_get(content,'more_info','More info:')} {extra}"
    await update.message.reply_text(text, reply_markup=build_main_menu(content))


async def on_mystats_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle My Stats menu navigation."""
    query = update.callback_query
    await query.answer()
    
    db_init()
    all_content = load_all_content()
    content = get_active_content(context, all_content)
    
    data = query.data or ""
    user_id = query.from_user.id
    action = data.split(":", 1)[1] if ":" in data else ""
    
    # Check if user has referral links set
    ref = get_referrer_by_owner(user_id)
    if not ref:
        await safe_show_menu_message(
            query, 
            context, 
            ui_get(content, "ref_not_set", "Set your links first."), 
            sharing_tools_submenu_kb(content)
        )
        return
    
    # Route to appropriate handler
    if action == "hub":
        # My Stats Hub
        await show_mystats_hub(query, context, content)
    
    elif action == "personal":
        # Personal Stats
        await show_personal_stats(query, context, content, user_id)
    
    elif action == "activity_help":
        # Activity Score Help Popup
        await show_activity_help(query, context, content, user_id)
    
    elif action == "team_hub":
        # Team Stats Hub
        await show_team_stats_hub(query, context, content)
    
    elif action == "team_details":
        # Team Details screen
        await show_team_details(query, context, content, user_id)
    
    elif action == "team_comparison":
        # Team Comparison screen
        await show_team_comparison(query, context, content, user_id)
    
    elif action == "actions":
        # My Actions screen
        await show_my_actions(query, context, content, user_id)
    
    elif action == "milestones":
        # My Milestones screen
        await show_my_milestones(query, context, content, user_id)


async def show_mystats_hub(query, context, content):
    """Show My Stats hub screen."""
    title = ui_get(content, "my_stats_hub_title", "📊 MY STATS\n\nChoose what you'd like to view:")
    await safe_show_menu_message(query, context, title, my_stats_hub_kb(content))


async def show_personal_stats(query, context, content, user_id: int):
    """Show Personal Stats screen."""
    # Get personal stats
    stats = get_personal_stats(user_id)
    
    if not stats:
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "ref_not_set", "Set your links first."),
            sharing_tools_submenu_kb(content)
        )
        return
    
    # Build the screen text
    sections = []
    
    # Title
    sections.append(ui_get(content, "personal_stats_title", "📊 YOUR PERSONAL STATS"))
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Rank Section
    sections.append(ui_get(content, "your_rank_section", "🏆 YOUR RANK"))
    sections.append("━━━━━━━━━━━━━━━")
    
    rank_text = ui_get(content, "rank_display", "#{rank} of {total} affiliates\n(Top {percentage}%)")
    rank_text = rank_text.replace("{rank}", str(stats["rank"]))
    rank_text = rank_text.replace("{total}", str(stats["total_affiliates"]))
    rank_text = rank_text.replace("{percentage}", str(stats["percentile"]))
    sections.append(rank_text)
    
    # Add rank tip if applicable
    if stats["rank"] > 1:
        next_rank = stats["rank"] - 1
        gap = 2  # Simplified - could calculate actual gap
        unit = ui_get(content, "visitors_unit", "visitors")
        tip = ui_get(content, "rank_tip", "💡 Just {gap} more {unit} to reach #{next_rank}!")
        tip = tip.replace("{gap}", str(gap))
        tip = tip.replace("{unit}", unit)
        tip = tip.replace("{next_rank}", str(next_rank))
        sections.append("")
        sections.append(tip)
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Team Overview Section
    sections.append(ui_get(content, "team_overview_section", "👥 YOUR TEAM OVERVIEW"))
    sections.append("━━━━━━━━━━━━━━━")
    
    visitors_text = ui_get(content, "total_visitors", "Total Unique Visitors: {count}")
    visitors_text = visitors_text.replace("{count}", str(stats["visitors"]))
    sections.append(visitors_text)
    
    members_text = ui_get(content, "active_members", "Active Members: {count} ({percent}%)")
    members_text = members_text.replace("{count}", str(stats["active_members"]))
    members_text = members_text.replace("{percent}", str(stats["conversion"]))
    sections.append(members_text)
    
    # Progress bar - use success context (shows mint/aqua if 60%+)
    progress_bar = create_progress_bar(stats["conversion"], context="success")
    sections.append(progress_bar)
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Activity Score Section
    sections.append(ui_get(content, "activity_score_section", "⭐ TEAM ACTIVITY SCORE"))
    sections.append("━━━━━━━━━━━━━━━")
    
    score_text = ui_get(content, "activity_score_display", "{stars} ({score}/5)")
    score_text = score_text.replace("{stars}", stats["activity_stars"])
    score_text = score_text.replace("{score}", f"{stats['activity_score']:.1f}")
    sections.append(score_text)
    
    sections.append("")
    
    percentile_text = ui_get(content, "activity_percentile", "You're in the top {percent}% of affiliates!")
    percentile_text = percentile_text.replace("{percent}", str(stats["percentile"]))
    sections.append(percentile_text)
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Weekly Growth Section
    sections.append(ui_get(content, "this_week_section", "📈 THIS WEEK"))
    sections.append("━━━━━━━━━━━━━━━")
    
    if stats["growth"]["has_time_data"]:
        new_visitors = ui_get(content, "new_visitors", "• New Visitors: {count}")
        new_visitors = new_visitors.replace("{count}", str(stats["growth"]["visitors_7d"]))
        sections.append(new_visitors)
        
        new_members = ui_get(content, "new_members", "• New Members: {count}")
        new_members = new_members.replace("{count}", str(stats["growth"]["members_7d"]))
        sections.append(new_members)
        
        # Calculate growth rate
        if stats["visitors"] > 0:
            growth_rate = int((stats["growth"]["visitors_7d"] / stats["visitors"]) * 100)
            growth_text = ui_get(content, "growth_rate", "• Growth Rate: +{percent}%")
            growth_text = growth_text.replace("{percent}", str(growth_rate))
            sections.append(growth_text)
    else:
        sections.append("• Growth tracking coming soon!")
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # 7-Day Trend
    sections.append(ui_get(content, "trend_section", "📈 7-DAY TREND"))
    sections.append("━━━━━━━━━━━━━━━")
    
    if stats["growth"]["has_time_data"]:
        chart = "▁▂▃▅▆█▇"  # Simplified chart
        trend_text = ui_get(content, "members_trend", "Members: {chart} (+{count} this week!)")
        trend_text = trend_text.replace("{chart}", chart)
        trend_text = trend_text.replace("{count}", str(stats["growth"]["members_7d"]))
        sections.append(trend_text)
    else:
        sections.append("Trend tracking coming soon!")
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Streak Section
    sections.append(ui_get(content, "streak_section", "🔥 ACTIVE STREAK"))
    sections.append("━━━━━━━━━━━━━━━")
    
    if stats["streak"] > 0:
        streak_text = ui_get(content, "streak_display", "{days} days in a row!\nKeep it going! 💪")
        streak_text = streak_text.replace("{days}", str(stats["streak"]))
        sections.append(streak_text)
    else:
        sections.append(ui_get(content, "no_streak", "No active streak yet.\nCome back tomorrow to start one! 🔥"))
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Monthly Summary
    sections.append(ui_get(content, "monthly_section", "📅 THIS MONTH"))
    sections.append("━━━━━━━━━━━━━━━")
    
    if stats["growth"]["has_time_data"]:
        monthly_visitors = ui_get(content, "monthly_visitors", "• Unique Visitors Added: {count}")
        monthly_visitors = monthly_visitors.replace("{count}", str(stats["growth"]["visitors_30d"]))
        sections.append(monthly_visitors)
        
        monthly_members = ui_get(content, "monthly_members", "• New Members: {count}")
        monthly_members = monthly_members.replace("{count}", str(stats["growth"]["members_30d"]))
        sections.append(monthly_members)
        
        best_week = ui_get(content, "best_week", "• Best Week: {count} new members")
        best_week = best_week.replace("{count}", str(max(stats["growth"]["members_7d"], 1)))
        sections.append(best_week)
    else:
        sections.append("• Monthly tracking coming soon!")
    
    # Combine all sections
    full_text = "\n".join(sections)
    
    await safe_show_menu_message(query, context, full_text, personal_stats_kb(content))


async def show_activity_help(query, context, content, user_id: int):
    """Show activity score help popup."""
    stats = get_personal_stats(user_id)
    
    if not stats:
        await query.answer("Unable to load stats", show_alert=True)
        return
    
    # Calculate breakdown
    conversion = stats["conversion"]
    visitors = stats["visitors"]
    
    # Conversion stars
    if conversion >= 60:
        conversion_stars = 3.0
    elif conversion >= 40:
        conversion_stars = 2.0
    elif conversion >= 20:
        conversion_stars = 1.0
    else:
        conversion_stars = 0.0
    
    # Size stars
    if visitors >= 50:
        size_stars = 2.0
    elif visitors >= 30:
        size_stars = 1.5
    elif visitors >= 20:
        size_stars = 1.0
    else:
        size_stars = 0.0
    
    # Build help text
    help_text = ui_get(content, "activity_help_text", "Activity score explanation")
    help_text = help_text.replace("{conversion}", str(conversion))
    help_text = help_text.replace("{conversion_stars}", f"{conversion_stars:.1f}")
    help_text = help_text.replace("{team_size}", str(visitors))
    help_text = help_text.replace("{size_stars}", f"{size_stars:.1f}")
    help_text = help_text.replace("{total_score}", f"{stats['activity_score']:.1f}")
    help_text = help_text.replace("{stars}", stats["activity_stars"])
    
    title = ui_get(content, "activity_help_title", "⭐ ACTIVITY SCORE EXPLAINED")
    full_text = f"{title}\n\n{help_text}"
    
    await safe_show_menu_message(query, context, full_text, activity_help_popup_kb(content))


async def show_team_stats_hub(query, context, content):
    """Show Team Stats hub screen."""
    title = ui_get(content, "team_stats_hub_title", "👥 TEAM STATS\n\nChoose a view:")
    await safe_show_menu_message(query, context, title, team_stats_hub_kb(content))


async def show_team_details(query, context, content, user_id: int):
    """Show Team Details screen."""
    stats = get_personal_stats(user_id)
    
    if not stats:
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "ref_not_set", "Set your links first."),
            sharing_tools_submenu_kb(content)
        )
        return
    
    sections = []
    
    # Title
    sections.append(ui_get(content, "team_details_title", "👥 TEAM DETAILS"))
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Team Composition Section
    sections.append(ui_get(content, "team_composition_section", "👥 TEAM COMPOSITION"))
    sections.append("━━━━━━━━━━━━━━━")
    
    # Build composition display
    comp_display = ui_get(content, "team_comp_display", "Total Visitors: {total}\nActive Members: {active} ({percent}%)")
    
    # Get activity breakdown (simplified for now - would need more DB queries for real data)
    active_24h = int(stats["active_members"] * 0.3)  # Estimate: 30% active in 24h
    recent_7d = int(stats["active_members"] * 0.5)   # Estimate: 50% active in 7d
    inactive = stats["visitors"] - recent_7d
    
    comp_display = comp_display.replace("{total}", str(stats["visitors"]))
    comp_display = comp_display.replace("{active}", str(stats["active_members"]))
    comp_display = comp_display.replace("{percent}", str(stats["conversion"]))
    comp_display = comp_display.replace("{active_24h}", str(active_24h))
    comp_display = comp_display.replace("{recent_7d}", str(recent_7d))
    comp_display = comp_display.replace("{inactive}", str(inactive))
    comp_display = comp_display.replace("{conversion}", str(stats["conversion"]))
    
    sections.append(comp_display)
    
    # Progress bar for conversion
    progress_bar = create_progress_bar(stats["conversion"], context="success")
    sections.append(progress_bar)
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Team Activity Section
    sections.append(ui_get(content, "team_activity_section", "👥 RECENT TEAM ACTIVITY"))
    sections.append("━━━━━━━━━━━━━━━")
    
    # Simplified activity feed (would need actual activity log for real data)
    if stats["growth"]["has_time_data"] and stats["growth"]["members_7d"] > 0:
        activity_text = ui_get(content, "became_member", "• {name} became a member ({time} ago) 🟢")
        activity_text = activity_text.replace("{name}", "Team member")
        activity_text = activity_text.replace("{time}", "recently")
        sections.append(activity_text)
        sections.append(f"• {stats['growth']['members_7d']} new members this week 🟢")
    else:
        sections.append(ui_get(content, "no_activity", "No recent activity to show."))
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Team Quality Section
    sections.append(ui_get(content, "team_quality_section", "👥 TEAM QUALITY"))
    sections.append("━━━━━━━━━━━━━━━")
    
    quality_display = ui_get(content, "quality_display", "Active Members: {active}/{total} ({percent}%)\n\nQuality Score: {stars} ({score}/5)")
    quality_display = quality_display.replace("{active}", str(stats["active_members"]))
    quality_display = quality_display.replace("{total}", str(stats["visitors"]))
    quality_display = quality_display.replace("{percent}", str(stats["conversion"]))
    quality_display = quality_display.replace("{stars}", stats["activity_stars"])
    quality_display = quality_display.replace("{score}", f"{stats['activity_score']:.1f}")
    
    sections.append(quality_display)
    
    # Progress bar for quality
    progress_bar = create_progress_bar(stats["conversion"], context="success")
    sections.append(progress_bar)
    
    # Combine all sections
    full_text = "\n".join(sections)
    
    await safe_show_menu_message(query, context, full_text, team_details_kb(content))


async def show_team_comparison(query, context, content, user_id: int):
    """Show Team Comparison screen."""
    stats = get_personal_stats(user_id)
    
    if not stats:
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "ref_not_set", "Set your links first."),
            sharing_tools_submenu_kb(content)
        )
        return
    
    # Get platform averages
    avg_stats = get_average_stats()
    top10_stats = get_top10_stats()
    
    sections = []
    
    # Title
    sections.append(ui_get(content, "team_comparison_title", "📊 TEAM COMPARISON"))
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # VS Average Section
    sections.append(ui_get(content, "vs_average_section", "📊 VS AVERAGE AFFILIATE"))
    sections.append("━━━━━━━━━━━━━━━")
    
    # Calculate differences
    visitors_diff = stats["visitors"] - avg_stats["avg_visitors"]
    visitors_percent = int((visitors_diff / avg_stats["avg_visitors"] * 100)) if avg_stats["avg_visitors"] > 0 else 0
    
    members_diff = stats["active_members"] - avg_stats["avg_members"]
    members_percent = int((members_diff / avg_stats["avg_members"] * 100)) if avg_stats["avg_members"] > 0 else 0
    
    # Determine above/below
    above_below_visitors = ui_get(content, "above", "above") if visitors_diff >= 0 else ui_get(content, "below", "below")
    above_below_members = ui_get(content, "above", "above") if members_diff >= 0 else ui_get(content, "below", "below")
    
    emoji_visitors = "🔥" if visitors_diff >= 0 else "📊"
    emoji_members = "🔥" if members_diff >= 0 else "📊"
    
    vs_avg_display = ui_get(content, "vs_average_display", "Your Visitors: {your_visitors}\nAverage: {avg_visitors} visitors")
    vs_avg_display = vs_avg_display.replace("{your_visitors}", str(stats["visitors"]))
    vs_avg_display = vs_avg_display.replace("{avg_visitors}", str(avg_stats["avg_visitors"]))
    vs_avg_display = vs_avg_display.replace("{percent_visitors}", str(abs(visitors_percent)))
    vs_avg_display = vs_avg_display.replace("{above_below}", above_below_visitors)
    vs_avg_display = vs_avg_display.replace("{emoji_visitors}", emoji_visitors)
    vs_avg_display = vs_avg_display.replace("{your_members}", str(stats["active_members"]))
    vs_avg_display = vs_avg_display.replace("{your_conversion}", str(stats["conversion"]))
    vs_avg_display = vs_avg_display.replace("{avg_members}", str(avg_stats["avg_members"]))
    vs_avg_display = vs_avg_display.replace("{avg_conversion}", str(avg_stats["avg_conversion"]))
    vs_avg_display = vs_avg_display.replace("{percent_members}", str(abs(members_percent)))
    vs_avg_display = vs_avg_display.replace("{above_below_members}", above_below_members)
    vs_avg_display = vs_avg_display.replace("{emoji_members}", emoji_members)
    
    sections.append(vs_avg_display)
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # VS Top 10% Section
    sections.append(ui_get(content, "vs_top10_section", "📊 VS TOP 10%"))
    sections.append("━━━━━━━━━━━━━━━")
    
    # Calculate gap to top 10%
    visitors_gap = top10_stats["top10_visitors"] - stats["visitors"]
    members_gap = top10_stats["top10_members"] - stats["active_members"]
    
    visitors_progress = int((stats["visitors"] / top10_stats["top10_visitors"] * 100)) if top10_stats["top10_visitors"] > 0 else 0
    members_progress = int((stats["active_members"] / top10_stats["top10_members"] * 100)) if top10_stats["top10_members"] > 0 else 0
    
    # Check if already in top 10%
    if visitors_progress >= 100:
        encouragement = ui_get(content, "youre_in_top10", "You're in the top 10%! Amazing! 🔥")
    else:
        encouragement = ui_get(content, "keep_building", "Keep building! 💪")
    
    vs_top10_display = ui_get(content, "vs_top10_display", "Top 10% Average: {top_visitors} visitors\nYour Visitors: {your_visitors}")
    vs_top10_display = vs_top10_display.replace("{top_visitors}", str(top10_stats["top10_visitors"]))
    vs_top10_display = vs_top10_display.replace("{your_visitors}", str(stats["visitors"]))
    vs_top10_display = vs_top10_display.replace("{gap_visitors}", str(max(0, visitors_gap)))
    vs_top10_display = vs_top10_display.replace("{percent_visitors}", str(min(100, visitors_progress)))
    vs_top10_display = vs_top10_display.replace("{top_members}", str(top10_stats["top10_members"]))
    vs_top10_display = vs_top10_display.replace("{your_members}", str(stats["active_members"]))
    vs_top10_display = vs_top10_display.replace("{gap_members}", str(max(0, members_gap)))
    vs_top10_display = vs_top10_display.replace("{percent_members}", str(min(100, members_progress)))
    vs_top10_display = vs_top10_display.replace("{encouragement}", encouragement)
    
    sections.append(vs_top10_display)
    
    # Progress bars for gaps
    sections.append("")
    sections.append("Visitors Progress:")
    progress_bar = create_progress_bar(min(100, visitors_progress))
    sections.append(progress_bar)
    
    sections.append("")
    sections.append("Members Progress:")
    progress_bar = create_progress_bar(min(100, members_progress))
    sections.append(progress_bar)
    
    # Combine all sections
    full_text = "\n".join(sections)
    
    await safe_show_menu_message(query, context, full_text, team_comparison_kb(content))


async def on_action_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle action button clicks for all 9 suggestion types."""
    query = update.callback_query
    await query.answer()
    
    all_content = load_all_content()
    content_obj = get_active_content(context, all_content)
    
    data = query.data or ""
    parts = data.split(":", 2)
    
    if len(parts) < 2:
        return
    
    action_type = parts[1]
    
    if action_type == "followup":
        # Show follow-up template
        if len(parts) >= 3:
            ref_code = parts[2]
            await show_followup_template(query, context, content_obj, ref_code)
    
    elif action_type == "streak_reminder":
        # Show streak reminder
        await show_streak_reminder(query, context, content_obj)
    
    elif action_type == "conversion_tips":
        # Show conversion tips
        await show_conversion_tips(query, context, content_obj)
    
    elif action_type == "reengage":
        # Show re-engagement template
        if len(parts) >= 3:
            ref_code = parts[2]
            await show_reengage_template(query, context, content_obj, ref_code)
    
    elif action_type == "weekly_goal":
        # Show weekly goal setter (placeholder for now)
        await query.answer("Weekly goal setting coming soon! 🎯", show_alert=True)
    
    elif action_type == "best_time":
        # Show best time reminder (placeholder for now)
        await query.answer("Reminder feature coming soon! ⏰", show_alert=True)


async def show_my_actions(query, context, content, user_id: int):
    """Show My Actions screen with TOP 3 most impactful smart suggestions."""
    stats = get_personal_stats(user_id)
    
    if not stats:
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "ref_not_set", "Set your links first."),
            sharing_tools_submenu_kb(content)
        )
        return
    
    # Get platform averages for comparison
    avg_stats = get_average_stats()
    
    # Build ALL 9 suggestions with impact scores
    all_suggestions = []
    
    # 1. CONVERT VISITORS TO MEMBERS (High Impact if < 80% conversion)
    if stats["conversion"] < 80:
        unconverted = stats["visitors"] - stats["active_members"]
        if unconverted > 0:
            # Impact: Higher if more unconverted AND lower conversion
            impact = unconverted * (100 - stats["conversion"]) / 100
            all_suggestions.append({
                "type": "convert",
                "impact": impact,
                "text": ui_get(content, "action_convert_visitors", "📧 Convert Visitors to Members\n{count} visitors haven't become members yet").replace("{count}", str(unconverted)),
                "button": "btn_send_followup"
            })
    
    # 2. CLIMB LEADERBOARD (Medium-High Impact if not #1)
    if stats["rank"] > 1:
        # Impact: Higher if closer to top
        impact = 100 - stats["percentile"]
        next_rank = stats["rank"] - 1
        all_suggestions.append({
            "type": "climb",
            "impact": impact,
            "text": ui_get(content, "action_climb_leaderboard", "🎯 Climb the Leaderboard\nYou're close to #{rank} rank").replace("{rank}", str(next_rank)),
            "button": "btn_share_invite"
        })
    
    # 3. MAINTAIN/START STREAK (Medium Impact - habit building)
    if stats["streak"] > 0:
        # Impact: Higher with longer streaks (don't want to break)
        impact = min(stats["streak"] * 5, 60)
        all_suggestions.append({
            "type": "streak",
            "impact": impact,
            "text": ui_get(content, "action_maintain_streak", "🔥 Maintain Your Streak\n{days} days active - keep it going!").replace("{days}", str(stats["streak"])),
            "button": "btn_come_back"
        })
    else:
        # Starting streak has medium impact
        all_suggestions.append({
            "type": "streak",
            "impact": 40,
            "text": ui_get(content, "action_start_streak", "🔥 Start Your Streak\nBuild consistency - come back daily!"),
            "button": "btn_come_back"
        })
    
    # 4. QUALITY FOCUS (High Impact if significantly below average)
    if stats["conversion"] < avg_stats["avg_conversion"]:
        conversion_gap = avg_stats["avg_conversion"] - stats["conversion"]
        if conversion_gap >= 10:  # At least 10% below average
            # Impact: Higher the bigger the gap
            impact = conversion_gap * 2
            all_suggestions.append({
                "type": "quality",
                "impact": impact,
                "text": ui_get(content, "action_quality_focus", "🎯 Improve Your Conversion\nYour conversion is {conversion}% - platform average is {average}%").replace("{conversion}", str(stats["conversion"])).replace("{average}", str(avg_stats["avg_conversion"])),
                "button": "btn_conversion_tips"
            })
    
    # 5. REACH NEXT MILESTONE (Very High Impact if within 5 of milestone)
    milestones = [10, 25, 50, 100, 250, 500]
    for milestone in milestones:
        if stats["visitors"] < milestone:
            gap = milestone - stats["visitors"]
            if gap <= 5:
                # Impact: Very high when close to milestone
                impact = 100 - (gap * 10)
                unit = ui_get(content, "visitors_unit", "visitors")
                all_suggestions.append({
                    "type": "milestone",
                    "impact": impact,
                    "text": ui_get(content, "action_reach_milestone", "🎖️ Almost There!\nJust {gap} more {unit} to reach {milestone} milestone").replace("{gap}", str(gap)).replace("{unit}", unit).replace("{milestone}", str(milestone)),
                    "button": "btn_share_to_goal"
                })
            break
    
    # 6. RE-ENGAGE INACTIVE MEMBERS (Medium-High if has inactive)
    if stats["visitors"] > stats["active_members"]:
        inactive_estimate = int((stats["visitors"] - stats["active_members"]) * 0.7)
        if inactive_estimate >= 5:
            # Impact: Higher with more inactive
            impact = min(inactive_estimate * 3, 75)
            all_suggestions.append({
                "type": "reengage",
                "impact": impact,
                "text": ui_get(content, "action_reengage", "💌 Re-engage Inactive Members\n{count} visitors haven't checked in this week").replace("{count}", str(inactive_estimate)),
                "button": "btn_reengage_message"
            })
    
    # 7. CELEBRATE RECENT WIN (High Impact if just achieved something)
    recent_achievement = None
    if stats["rank"] <= 10:
        recent_achievement = f"#{stats['rank']} rank"
    elif stats["visitors"] in [10, 25, 50, 100]:
        recent_achievement = f"{stats['visitors']} visitors"
    
    if recent_achievement:
        # Impact: High for celebrations (motivational)
        impact = 80
        all_suggestions.append({
            "type": "celebrate",
            "impact": impact,
            "text": ui_get(content, "action_celebrate", "🎉 Celebrate Your Win!\nYou just reached {achievement} - share your success!").replace("{achievement}", recent_achievement),
            "button": "btn_share_achievement"
        })
    
    # 8. WEEKLY GOAL SETTING (Medium Impact - planning)
    if stats["growth"]["has_time_data"]:
        last_week_growth = stats["growth"]["members_7d"]
        # Impact: Medium for goal setting
        impact = 50
        all_suggestions.append({
            "type": "weekly_goal",
            "impact": impact,
            "text": ui_get(content, "action_weekly_goal", "🎯 Set This Week's Goal\nLast week: +{last_week} members. What's your goal this week?").replace("{last_week}", str(last_week_growth)),
            "button": "btn_set_goal"
        })
    
    # 9. BEST TIME TO SHARE (Low-Medium Impact - optimization)
    # Simplified: assume 6-9 PM is best time
    impact = 35
    all_suggestions.append({
        "type": "best_time",
        "impact": impact,
        "text": ui_get(content, "action_best_time", "⏰ Prime Sharing Time\nYour team is most active {time_range} - share then!").replace("{time_range}", "6-9 PM"),
        "button": "btn_set_reminder"
    })
    
    # SORT BY IMPACT AND TAKE TOP 3
    all_suggestions.sort(key=lambda x: x["impact"], reverse=True)
    top_suggestions = all_suggestions[:3]
    
    # Build display
    sections = []
    actions_list = []
    
    sections.append(ui_get(content, "my_actions_title", "⚡ MY ACTIONS\n\n💡 Suggested actions based on your stats:"))
    sections.append("")
    
    for i, suggestion in enumerate(top_suggestions):
        if i > 0:
            sections.append("━━━━━━━━━━━━━━━")
        sections.append("")
        sections.append(suggestion["text"])
        sections.append("")
        actions_list.append(suggestion["type"])
    
    sections.append("━━━━━━━━━━━━━━━")
    
    # If no suggestions somehow, show encouragement
    if not actions_list:
        sections.append("")
        sections.append(ui_get(content, "no_actions", "Great job! No urgent actions needed.\nKeep up the excellent work! 🌟"))
    
    # Combine all sections
    full_text = "\n".join(sections)
    
    await safe_show_menu_message(query, context, full_text, my_actions_kb(content, stats["ref_code"], actions_list))


async def show_followup_template(query, context, content, ref_code: str):
    """Show follow-up message template."""
    # Get invite link
    invite_link = build_invite_link(ref_code, content)
    
    # Get team stats for personalization
    team_stats = get_team_stats(ref_code)
    
    template_text = ui_get(content, "followup_template", "📧 FOLLOW-UP TEMPLATE")
    template_text = template_text.replace("{count}", str(team_stats["team_with_links"]))
    template_text = template_text.replace("{link}", invite_link)
    
    await safe_show_menu_message(
        query,
        context,
        template_text,
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Copy Link", url=invite_link)],
            [InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:actions")],
            [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
        ])
    )


async def show_streak_reminder(query, context, content):
    """Show streak reminder message."""
    # Get user's streak (placeholder for now)
    days = 5  # Would get from database
    
    reminder_text = ui_get(content, "streak_reminder", "🔥 STREAK REMINDER")
    reminder_text = reminder_text.replace("{days}", str(days))
    
    await safe_show_menu_message(
        query,
        context,
        reminder_text,
        InlineKeyboardMarkup([
            [InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:actions")],
            [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
        ])
    )


async def show_conversion_tips(query, context, content):
    """Show conversion improvement tips."""
    tips_text = ui_get(content, "conversion_tips", "📚 CONVERSION TIPS")
    
    await safe_show_menu_message(
        query,
        context,
        tips_text,
        InlineKeyboardMarkup([
            [InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:actions")],
            [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
        ])
    )


async def show_reengage_template(query, context, content, ref_code: str):
    """Show re-engagement message template."""
    # Get invite link
    invite_link = build_invite_link(ref_code, content)
    
    # Get team stats for personalization
    team_stats = get_team_stats(ref_code)
    
    template_text = ui_get(content, "reengage_template", "💌 RE-ENGAGEMENT TEMPLATE")
    template_text = template_text.replace("{members}", str(team_stats["team_with_links"]))
    template_text = template_text.replace("{link}", invite_link)
    
    await safe_show_menu_message(
        query,
        context,
        template_text,
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Copy Link", url=invite_link)],
            [InlineKeyboardButton(ui_get(content, "back_to_my_stats", "⬅️ Back to My Stats"), callback_data="mystats:actions")],
            [InlineKeyboardButton(ui_get(content, "back_to_menu", "⬅️ Back to menu"), callback_data="menu:home")]
        ])
    )


async def show_my_milestones(query, context, content, user_id: int):
    """Show My Milestones screen with next milestone, achievements, and recent wins."""
    stats = get_personal_stats(user_id)
    
    if not stats:
        await safe_show_menu_message(
            query,
            context,
            ui_get(content, "ref_not_set", "Set your links first."),
            sharing_tools_submenu_kb(content)
        )
        return
    
    sections = []
    
    # Title
    sections.append(ui_get(content, "my_milestones_title", "🎖️ MY MILESTONES"))
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Next Milestone Section
    sections.append(ui_get(content, "next_milestone_section", "🎯 NEXT MILESTONE"))
    sections.append("━━━━━━━━━━━━━━━")
    
    # Find next milestone
    milestones = [10, 25, 50, 100, 250, 500]
    next_milestone = None
    for ms in milestones:
        if stats["visitors"] < ms:
            next_milestone = ms
            break
    
    if next_milestone:
        current = stats["visitors"]
        remaining = next_milestone - current
        percentage = int((current / next_milestone) * 100)
        
        milestone_title = ui_get(content, f"milestone_{next_milestone}_members", f"{next_milestone} Team Members")
        
        # Determine encouragement based on percentage
        if percentage >= 90:
            encouragement = ui_get(content, "milestone_close", "You're so close! 🔥")
        elif percentage >= 50:
            encouragement = ui_get(content, "milestone_halfway", "Halfway there! 💪")
        else:
            encouragement = ui_get(content, "milestone_keep_going", "Keep pushing! 🚀")
        
        milestone_display = ui_get(content, "milestone_display", "{title}\n\nCurrent: {current} ({percent}%)\n\nJust {remaining} more! {encouragement}")
        milestone_display = milestone_display.replace("{title}", milestone_title)
        milestone_display = milestone_display.replace("{current}", str(current))
        milestone_display = milestone_display.replace("{percent}", str(percentage))
        milestone_display = milestone_display.replace("{remaining}", str(remaining))
        milestone_display = milestone_display.replace("{encouragement}", encouragement)
        
        sections.append(milestone_display)
        sections.append("")
        
        # Progress bar with milestone context
        progress_bar = create_progress_bar(percentage, context="milestone")
        sections.append(progress_bar)
    else:
        sections.append("🎉 You've reached all milestones! Amazing!")
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Achievements Section
    unlocked_count = 0
    total_achievements = 15
    
    # Check which achievements are unlocked
    achievements = []
    
    # First Steps
    if stats["visitors"] >= 1:
        achievements.append(("unlocked", ui_get(content, "achievement_first_steps", "✅ First Steps - Made 1st referral")))
        unlocked_count += 1
    
    # Team Builder milestones
    if stats["visitors"] >= 10:
        achievements.append(("unlocked", ui_get(content, "achievement_team_builder_10", "✅ Team Builder - 10 members")))
        unlocked_count += 1
    elif stats["visitors"] >= 5:
        progress = int((stats["visitors"] / 10) * 100)
        achievements.append(("locked", ui_get(content, "locked_achievement", "🔒 {title} ({progress}%)").replace("{title}", "Team Builder - 10 members").replace("{progress}", str(progress))))
    
    if stats["visitors"] >= 25:
        achievements.append(("unlocked", ui_get(content, "achievement_team_builder_25", "✅ Growing Strong - 25 members")))
        unlocked_count += 1
    elif stats["visitors"] >= 15:
        progress = int((stats["visitors"] / 25) * 100)
        achievements.append(("locked", ui_get(content, "locked_achievement", "🔒 {title} ({progress}%)").replace("{title}", "Growing Strong - 25 members").replace("{progress}", str(progress))))
    
    if stats["visitors"] >= 50:
        achievements.append(("unlocked", ui_get(content, "achievement_team_builder_50", "✅ Power Player - 50 members")))
        unlocked_count += 1
    elif stats["visitors"] >= 35:
        progress = int((stats["visitors"] / 50) * 100)
        achievements.append(("locked", ui_get(content, "locked_achievement", "🔒 {title} ({progress}%)").replace("{title}", "Power Player - 50 members").replace("{progress}", str(progress))))
    
    if stats["visitors"] >= 100:
        achievements.append(("unlocked", ui_get(content, "achievement_century_club", "✅ Century Club - 100 members")))
        unlocked_count += 1
    elif stats["visitors"] >= 75:
        progress = int((stats["visitors"] / 100) * 100)
        achievements.append(("locked", ui_get(content, "locked_achievement", "🔒 {title} ({progress}%)").replace("{title}", "Century Club - 100 members").replace("{progress}", str(progress))))
    
    # Ranking achievements
    if stats["percentile"] <= 50:
        achievements.append(("unlocked", ui_get(content, "achievement_rising_star", "✅ Rising Star - Top 50%")))
        unlocked_count += 1
    
    if stats["percentile"] <= 25:
        achievements.append(("unlocked", ui_get(content, "achievement_top_quarter", "✅ Top Performer - Top 25%")))
        unlocked_count += 1
    elif stats["percentile"] <= 40:
        achievements.append(("locked", "🔒 Top Performer - Top 25%"))
    
    if stats["percentile"] <= 10:
        achievements.append(("unlocked", ui_get(content, "achievement_elite_status", "✅ Elite Status - Top 10%")))
        unlocked_count += 1
    elif stats["percentile"] <= 20:
        achievements.append(("locked", "🔒 Elite Status - Top 10%"))
    
    # Streak achievements
    if stats["streak"] >= 7:
        achievements.append(("unlocked", ui_get(content, "achievement_week_warrior", "✅ Week Warrior - 7-day streak")))
        unlocked_count += 1
    
    if stats["streak"] >= 30:
        achievements.append(("unlocked", ui_get(content, "achievement_month_master", "✅ Month Master - 30-day streak")))
        unlocked_count += 1
    
    # Conversion achievements
    if stats["conversion"] >= 70:
        achievements.append(("unlocked", ui_get(content, "achievement_quality_focus", "✅ Quality Focus - 70%+ conversion")))
        unlocked_count += 1
    elif stats["conversion"] >= 50:
        progress = int((stats["conversion"] / 70) * 100)
        achievements.append(("locked", ui_get(content, "locked_achievement", "🔒 {title} ({progress}%)").replace("{title}", "Quality Focus - 70%+ conversion").replace("{progress}", str(progress))))
    
    if stats["conversion"] >= 90:
        achievements.append(("unlocked", ui_get(content, "achievement_conversion_king", "✅ Conversion King - 90%+ conversion")))
        unlocked_count += 1
    elif stats["conversion"] >= 75:
        progress = int((stats["conversion"] / 90) * 100)
        achievements.append(("locked", ui_get(content, "locked_achievement", "🔒 {title} ({progress}%)").replace("{title}", "Conversion King - 90%+ conversion").replace("{progress}", str(progress))))
    
    # Early adopter (placeholder - would check actual join date)
    achievements.append(("unlocked", ui_get(content, "achievement_early_adopter", "✅ Early Adopter - Joined early 2026")))
    unlocked_count += 1
    
    # Consistency achievements (placeholder)
    achievements.append(("unlocked", ui_get(content, "achievement_consistent", "✅ Consistent - 3 weeks active")))
    unlocked_count += 1
    
    sections.append(ui_get(content, "achievements_section", "🏅 ACHIEVEMENTS UNLOCKED ({unlocked}/{total})").replace("{unlocked}", str(unlocked_count)).replace("{total}", str(total_achievements)))
    sections.append("━━━━━━━━━━━━━━━")
    
    # Show achievements (up to 11 total: 8 unlocked + 3 locked)
    shown = 0
    for status, achievement_text in achievements:
        if shown >= 11:
            break
        sections.append(achievement_text)
        shown += 1
    
    sections.append("")
    sections.append("━━━━━━━━━━━━━━━")
    
    # Recent Wins Section
    sections.append(ui_get(content, "recent_wins_section", "🎉 RECENT WINS"))
    sections.append("━━━━━━━━━━━━━━━")
    
    # Generate recent wins based on stats
    wins = []
    
    if stats["visitors"] >= 25:
        win_text = ui_get(content, "win_reached_members", "✅ Reached {count} members ({time} ago)")
        win_text = win_text.replace("{count}", str(stats["visitors"]))
        win_text = win_text.replace("{time}", "recently")
        wins.append(win_text)
    
    if stats["rank"] <= 50:
        win_text = ui_get(content, "win_climbed_rank", "✅ Climbed to #{rank} rank ({time} ago)")
        win_text = win_text.replace("{rank}", str(stats["rank"]))
        win_text = win_text.replace("{time}", "recently")
        wins.append(win_text)
    
    if stats["streak"] >= 5:
        win_text = ui_get(content, "win_streak", "✅ {days}-day streak achieved ({time})")
        win_text = win_text.replace("{days}", str(stats["streak"]))
        win_text = win_text.replace("{time}", "today")
        wins.append(win_text)
    
    if wins:
        for win in wins[:3]:  # Show max 3 recent wins
            sections.append(win)
    else:
        sections.append(ui_get(content, "no_recent_wins", "Keep building to unlock wins! 🚀"))
    
    # Combine all sections
    full_text = "\n".join(sections)
    
    await safe_show_menu_message(query, context, full_text, my_milestones_kb(content))


def create_progress_bar(percentage: int, length: int = 10, context: str = "default") -> str:
    """
    Create a visual progress bar using Pandora AI brand colors.
    
    Colors:
    - Ocean Blue (🟦) = #0b87ba - Primary/trust/foundation
    - Mint/Aqua (🟩) = #8fe1cc - Success/achievement/growth
    
    Context options:
    - "default" = Ocean Blue for standard progress
    - "success" = Mint/Aqua for high performance (60%+)
    - "milestone" = Mint if ≥80%, else Blue
    """
    filled = int((percentage / 100) * length)
    
    # Choose color based on context and value
    if context == "success" and percentage >= 60:
        # Mint/Aqua for high performance (represented as green 🟩)
        bar = "🟩" * filled + "⬜" * (length - filled)
    elif context == "milestone" and percentage >= 80:
        # Mint/Aqua for near-complete milestones
        bar = "🟩" * filled + "⬜" * (length - filled)
    else:
        # Ocean Blue for standard progress
        bar = "🟦" * filled + "⬜" * (length - filled)
    
    return f"{bar} {percentage}%"


def main() -> None:
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN env var")

    db_init()

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("adminstats", adminstats_cmd))

    app.add_handler(CommandHandler("reset", reset_cmd))

    app.add_handler(CallbackQueryHandler(on_menu_click, pattern=r"^menu:"))

    app.add_handler(CallbackQueryHandler(on_ref_click, pattern=r"^ref:"))
    app.add_handler(CallbackQueryHandler(on_invite_click, pattern=r"^invite:"))
    app.add_handler(CallbackQueryHandler(on_affiliate_click, pattern=r"^affiliate:"))
    app.add_handler(CallbackQueryHandler(on_mystats_click, pattern=r"^mystats:"))
    app.add_handler(CallbackQueryHandler(on_action_click, pattern=r"^action:"))
    app.add_handler(CallbackQueryHandler(on_language_click, pattern=r"^lang:set:"))
    app.add_handler(CallbackQueryHandler(on_join_click, pattern=r"^join:"))
    app.add_handler(CallbackQueryHandler(on_faq_click, pattern=r"^(faq_topic:|faq_q:|faq_back_|faq_search:)"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))

    # Schedule daily report at 9:00 AM (UTC)
    # Admins can configure DAILY_REPORT_HOUR in environment (default: 9)
    report_hour = int(os.getenv("DAILY_REPORT_HOUR", "9"))
    job_queue = app.job_queue
    if job_queue:
        # Schedule daily report
        from datetime import time
        job_queue.run_daily(
            send_daily_report,
            time=time(hour=report_hour, minute=0, second=0),
            name="daily_report"
        )
        logger.info(f"Daily report scheduled for {report_hour:02d}:00 UTC")

    logger.info("Bot is starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
