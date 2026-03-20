import os
import json
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "reputation.db"


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS entities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        type TEXT,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS analysis_cache (
        brand TEXT PRIMARY KEY,
        result JSON,
        cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    try:
        cursor.execute("ALTER TABLE entities ADD COLUMN description TEXT")
    except Exception:
        pass

    conn.commit()
    conn.close()
    print("[DB] Connected to local SQLite")


def should_save_score(brand: str, new_score: int) -> bool:
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

    last_score = row[0]
    last_time = datetime.fromisoformat(row[1])
    now = datetime.utcnow()

    time_passed = now - last_time
    score_changed = abs(new_score - last_score) > 3

    if time_passed >= timedelta(minutes=25):
        return True

    if score_changed:
        return True

    return False


def save_result(brand: str, sentiment: dict, score: int):

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


def save_mention(entity: str, source: str, text: str, sentiment: str):

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


def get_history(brand: str) -> list:

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
            "brand": row[0],
            "positive": row[1],
            "negative": row[2],
            "neutral": row[3],
            "score": row[4],
            "time": row[5]
        }
        for row in rows
    ]


def get_latest_result(brand: str) -> dict | None:

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
            "brand": row[0],
            "positive": row[1],
            "negative": row[2],
            "neutral": row[3],
            "score": row[4],
            "time": row[5]
        }
    return None


def add_entity(name: str, entity_type: str = "brand", description: str = ""):

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT OR IGNORE INTO entities (name, type, description)
    VALUES (?, ?, ?)
    """, (name, entity_type, description))

    conn.commit()
    conn.close()


def get_entity_description(name: str) -> str:

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

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT name, type, description FROM entities")

    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "name": r[0],
            "type": r[1],
            "description": r[2] or ""
        }
        for r in rows
    ]


def save_analysis_cache(brand: str, result: dict):

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

        cached_at = datetime.fromisoformat(row[1])
        age = datetime.utcnow() - cached_at
        age_minutes = age.total_seconds() / 60

        if age_minutes > max_age_minutes:
            print(f"[Cache] '{brand}' cache is {age_minutes:.1f} min old — stale")
            return None

        print(f"[Cache] '{brand}' cache is {age_minutes:.1f} min old — fresh")
        return json.loads(row[0])

    except Exception as e:
        print(f"[Cache] Error reading cache for '{brand}': {e}")
        return None