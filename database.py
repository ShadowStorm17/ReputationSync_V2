# database.py
# ============================================================
# ReputationSync — Database Layer
# SQLite schema, all DB functions, cache system, dedup logic
#
# Tables:
#   entities          — Monitored entities with type and description
#   reputation_history — Score history for Engine 4 Prediction
#   mentions          — Individual mention storage (deduped)
#   analysis_cache    — Full 5-engine result cache (120 min freshness)
#   youtube_quota     — B3: Daily YouTube API unit tracking
#
# B3 FIX: YouTube quota tracking added.
#   Tracks daily API usage in units (10,000/day limit)
#   Resets at midnight Pacific time (YouTube's reset zone)
#   Prevents silent 403 exhaustion mid-monitoring-cycle
# ============================================================

import os
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "reputation.db"

# ── YouTube Quota Constants ───────────────────────────────────────────────────
YOUTUBE_DAILY_QUOTA    = 10000  # Total units per day
YOUTUBE_COST_PER_CALL  = 100    # Units per search.list call
YOUTUBE_WARN_THRESHOLD = 8000   # Warn at 80% usage


def get_connection():
    return sqlite3.connect(DB_PATH)


# ── Schema Initialization ─────────────────────────────────────────────────────

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # Entities — all monitored brands, people, politicians etc.
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS entities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        type TEXT,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Reputation history — score timeline for Engine 4 Prediction
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reputation_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        brand TEXT,
        positive INTEGER,
        negative INTEGER,
        neutral INTEGER,
        score INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Mentions — individual content items from all Engine 1 sources
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity TEXT,
        source TEXT,
        text TEXT,
        sentiment TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(entity, text)
    )""")

    # Analysis cache — full 5-engine result per entity (120 min freshness)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS analysis_cache (
        brand TEXT PRIMARY KEY,
        result JSON,
        cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # B3 — YouTube quota tracking table
    # Stores daily API unit consumption keyed by Pacific-time date
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS youtube_quota (
        date TEXT PRIMARY KEY,
        usage_units INTEGER DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Migration guard — adds description column if upgrading from older schema
    try:
        cursor.execute("ALTER TABLE entities ADD COLUMN description TEXT")
    except Exception:
        pass

    conn.commit()
    conn.close()
    print("[DB] Database initialized — all tables ready")


# ── Score Deduplication ───────────────────────────────────────────────────────

def should_save_score(brand: str, new_score: int) -> bool:
    """
    Prevents redundant score storage.
    Only saves if 25+ minutes have passed OR score changed by 3+ points.
    Keeps reputation_history lean and meaningful for Engine 4.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT score, created_at
    FROM reputation_history
    WHERE brand = ?
    ORDER BY created_at DESC
    LIMIT 1
    """, (brand,))

    row = cursor.fetchone()
    conn.close()

    if not row:
        return True

    last_score   = row[0]
    last_time    = datetime.fromisoformat(row[1])
    now          = datetime.utcnow()
    time_passed  = now - last_time
    score_changed = abs(new_score - last_score) > 3

    if time_passed >= timedelta(minutes=25):
        return True

    if score_changed:
        return True

    return False


# ── Reputation Score Storage ──────────────────────────────────────────────────

def save_result(brand: str, sentiment: dict, score: int):
    """
    Saves reputation score to history table.
    Deduped via should_save_score() to prevent noise in Engine 4 data.
    """
    if not should_save_score(brand, score):
        print(f"[DB] Score unchanged for {brand}, skipping save")
        return

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO reputation_history
    (brand, positive, negative, neutral, score)
    VALUES (?, ?, ?, ?, ?)
    """, (
        brand,
        sentiment.get("positive", 0),
        sentiment.get("negative", 0),
        sentiment.get("neutral", 0),
        score
    ))

    conn.commit()
    conn.close()
    print(f"[DB] Score saved for {brand}: {score}")


# ── Mention Storage ───────────────────────────────────────────────────────────

def save_mention(entity: str, source: str, text: str, sentiment: str):
    """
    Saves individual mention from any Engine 1 source.
    UNIQUE(entity, text) constraint prevents duplicate content.
    INSERT OR IGNORE silently skips duplicates.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT OR IGNORE INTO mentions (entity, source, text, sentiment)
        VALUES (?, ?, ?, ?)
        """, (entity, source, text, sentiment))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


# ── History Retrieval ─────────────────────────────────────────────────────────

def get_history(brand: str) -> list:
    """
    Returns full score history for a brand.
    Used by /history endpoint and Engine 4 Prediction.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT brand, positive, negative, neutral, score, created_at
    FROM reputation_history
    WHERE brand = ?
    ORDER BY created_at DESC
    """, (brand,))

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "brand":    row[0],
            "positive": row[1],
            "negative": row[2],
            "neutral":  row[3],
            "score":    row[4],
            "time":     row[5]
        }
        for row in rows
    ]


def get_latest_result(brand: str) -> dict | None:
    """
    Returns most recent score record for a brand.
    Used by Engine 4 for current score baseline.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT brand, positive, negative, neutral, score, created_at
    FROM reputation_history
    WHERE brand = ?
    ORDER BY created_at DESC
    LIMIT 1
    """, (brand,))

    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            "brand":    row[0],
            "positive": row[1],
            "negative": row[2],
            "neutral":  row[3],
            "score":    row[4],
            "time":     row[5]
        }
    return None


# ── Entity Management ─────────────────────────────────────────────────────────

def add_entity(name: str, entity_type: str = "brand", description: str = ""):
    """
    Adds entity to monitoring.
    INSERT OR IGNORE — safe to call repeatedly on startup (seed_entities).
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT OR IGNORE INTO entities (name, type, description)
    VALUES (?, ?, ?)
    """, (name, entity_type, description))

    conn.commit()
    conn.close()


def get_entity_description(name: str) -> str:
    """
    Returns stored description for an entity.
    Used as fallback when no description is provided in API call.
    Prevents ambiguous entity searches (partial B2 mitigation).
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    SELECT description FROM entities WHERE name = ?
    """, (name,))

    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return row[0]
    return ""


def get_all_entities() -> list:
    """
    Returns all monitored entities with type and description.
    Used by monitor.py and /alerts endpoint.
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT name, type, description FROM entities")

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "name":        r[0],
            "type":        r[1],
            "description": r[2] or ""
        }
        for r in rows
    ]


# ── Analysis Cache ────────────────────────────────────────────────────────────

def save_analysis_cache(brand: str, result: dict):
    """
    Saves full 5-engine analysis result to cache.
    120 minute freshness window — served instantly on repeat requests.
    Skips empty results to prevent bad data from overwriting good cache.
    """
    if not result:
        return

    if result.get("sentiment", {}).get("reason") == "No mentions found":
        print(f"[Cache] Skipping empty result for '{brand}'")
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT OR REPLACE INTO analysis_cache (brand, result, cached_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (brand, json.dumps(result)))
        conn.commit()
        print(f"[Cache] Saved analysis for '{brand}'")
    except Exception as e:
        print(f"[Cache] Save error for '{brand}': {e}")
    finally:
        conn.close()


def get_analysis_cache(brand: str, max_age_minutes: int = 120) -> dict | None:
    """
    Returns cached analysis if within freshness window.
    Returns None if stale or missing — triggers full re-analysis.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        SELECT result, cached_at
        FROM analysis_cache
        WHERE brand = ?
        """, (brand,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        cached_at   = datetime.fromisoformat(row[1])
        age         = datetime.utcnow() - cached_at
        age_minutes = age.total_seconds() / 60

        if age_minutes > max_age_minutes:
            print(f"[Cache] '{brand}' cache is {age_minutes:.1f} min old — stale")
            return None

        print(f"[Cache] '{brand}' cache is {age_minutes:.1f} min old — fresh")
        return json.loads(row[0])

    except Exception as e:
        print(f"[Cache] Error reading cache for '{brand}': {e}")
        return None


# ── B3: YouTube Quota Tracking ────────────────────────────────────────────────
# YouTube Data API v3: 10,000 units/day
# Each search.list call: 100 units
# Reset time: Midnight Pacific Time (UTC-7)
#
# Strategy:
#   - Store usage by Pacific-time date string (YYYY-MM-DD)
#   - Check before every YouTube monitoring cycle
#   - Warn at 8,000 units (80%) — still time to reduce calls
#   - Block at 10,000 units — prevents wasted failed requests
#   - Old date records auto-deleted on new day check

def _get_pacific_date() -> str:
    """
    Returns current date string in Pacific time (UTC-7).
    YouTube quota resets at midnight Pacific — we use this
    as our day boundary for all quota calculations.
    """
    pacific_now = datetime.now(timezone.utc) - timedelta(hours=7)
    return pacific_now.strftime("%Y-%m-%d")


def get_youtube_quota_usage() -> int:
    """
    Returns YouTube API units consumed today (Pacific time).
    Returns 0 if no record exists — new day or first run.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        today = _get_pacific_date()
        cursor.execute(
            "SELECT usage_units FROM youtube_quota WHERE date = ?",
            (today,)
        )
        result = cursor.fetchone()
        return result[0] if result else 0

    finally:
        conn.close()


def set_youtube_quota_usage(units: int):
    """
    Sets YouTube API usage counter for today.
    Uses INSERT OR REPLACE — creates or updates as needed.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        today = _get_pacific_date()
        cursor.execute(
            """
            INSERT OR REPLACE INTO youtube_quota (date, usage_units, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (today, units)
        )
        conn.commit()

    finally:
        conn.close()


def increment_youtube_quota(calls_made: int = 1) -> int:
    """
    Increments YouTube API usage counter by number of calls made.
    Each call costs YOUTUBE_COST_PER_CALL units (100).

    Returns new total usage for logging.
    """
    current   = get_youtube_quota_usage()
    new_total = current + (calls_made * YOUTUBE_COST_PER_CALL)
    set_youtube_quota_usage(new_total)

    print(
        f"[YouTube Quota] +{calls_made * YOUTUBE_COST_PER_CALL} units | "
        f"Total: {new_total:,} / {YOUTUBE_DAILY_QUOTA:,}"
    )
    return new_total


def reset_youtube_quota_if_new_day():
    """
    Deletes quota records older than today (Pacific time).
    Call this at the start of every YouTube monitoring cycle.
    Ensures counter resets cleanly at YouTube's midnight Pacific boundary.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        today = _get_pacific_date()
        cursor.execute(
            "DELETE FROM youtube_quota WHERE date < ?",
            (today,)
        )
        deleted = cursor.rowcount
        conn.commit()

        if deleted > 0:
            print(f"[YouTube Quota] New day detected — quota counter reset ✓")

    finally:
        conn.close()


def get_youtube_quota_status() -> dict:
    """
    Returns a complete quota status summary.
    Used by /status endpoint to expose quota visibility to operators.
    """
    reset_youtube_quota_if_new_day()
    current_usage = get_youtube_quota_usage()
    remaining     = YOUTUBE_DAILY_QUOTA - current_usage
    percentage    = (current_usage / YOUTUBE_DAILY_QUOTA) * 100
    calls_left    = remaining // YOUTUBE_COST_PER_CALL

    if current_usage >= YOUTUBE_DAILY_QUOTA:
        status = "exhausted"
    elif current_usage >= YOUTUBE_WARN_THRESHOLD:
        status = "warning"
    else:
        status = "healthy"

    return {
        "status":        status,
        "used_units":    current_usage,
        "total_units":   YOUTUBE_DAILY_QUOTA,
        "remaining":     remaining,
        "percentage":    round(percentage, 1),
        "calls_remaining": calls_left,
        "resets_at":     "Midnight Pacific Time (UTC-7)",
        "date_pacific":  _get_pacific_date()
    }