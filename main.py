# main.py
# ============================================================
# ReputationSync — FastAPI Server
# All endpoints, startup lifecycle, entity seeding
#
# B3 FIX: YouTube quota tracking integrated into fetch_and_filter()
# Quota is now tracked on both paths:
#   - API endpoint path (/analyze, /playbook)
#   - Background monitor path (monitor_youtube())
# ============================================================

from fastapi import FastAPI
from engine_understanding import analyze_with_ai
from engine_actors import analyze_actors
from engine_prediction import predict_trajectory
from engine_action import generate_playbook
from database import (
    init_db,
    save_result,
    save_analysis_cache,
    get_analysis_cache,
    get_history,
    add_entity,
    save_mention,
    get_entity_description,
    get_all_entities,
    # B3 — YouTube quota tracking
    get_youtube_quota_usage,
    set_youtube_quota_usage,
    reset_youtube_quota_if_new_day,
    get_youtube_quota_status,
)
from monitor import start_monitor
from sources.news_source import get_news_mentions
from sources.youtube_source import get_youtube_mentions
from sources.googlenews_source import get_googlenews_mentions
from sources.hackernews_source import get_hackernews_mentions
from filter import filter_relevant
from sentiment import analyze_sentiment
import threading
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

# ── YouTube Quota Constants ───────────────────────────────────────────────────
YOUTUBE_DAILY_QUOTA   = 10000  # units per day
YOUTUBE_COST_PER_CALL = 100    # units per search.list call

app = FastAPI(
    title="ReputationSync Intelligence Engine",
    description="AI-powered reputation intelligence platform",
    version="3.0.0"
)

# ── Default Entities ──────────────────────────────────────────────────────────
DEFAULT_ENTITIES = [
    ("Tesla",      "brand",  "Electric car company Elon Musk"),
    ("Nike",       "brand",  "Sports apparel footwear company"),
    ("Amazon",     "brand",  "Jeff Bezos ecommerce cloud technology"),
    ("Nvidia",     "brand",  "GPU semiconductor AI chips company"),
    ("Elon Musk",  "person", "Tesla SpaceX Twitter founder CEO"),
]


def seed_entities():
    """
    Re-adds default entities on every server startup.
    Uses INSERT OR IGNORE so existing entities are not overwritten.
    Ensures monitor always has entities to track after any restart.
    """
    for name, entity_type, description in DEFAULT_ENTITIES:
        add_entity(name, entity_type, description)
    print(f"[Startup] {len(DEFAULT_ENTITIES)} default entities seeded")


def start_monitor_delayed():
    print("[Monitor] Waiting 30s for server to fully start...")
    time.sleep(60)
    start_monitor()


# ── Startup ───────────────────────────────────────────────────────────────────
init_db()
seed_entities()
threading.Thread(target=start_monitor_delayed, daemon=True).start()


# ── Root ──────────────────────────────────────────────────────────────────────

@app.get("/")
def home():
    return {
        "product": "ReputationSync Intelligence Engine",
        "version": "3.0.0",
        "engines": [
            "Engine 1: Listening (NewsAPI + Google News + HackerNews + YouTube)",
            "Engine 2: Understanding (AI sentiment, topics, narrative)",
            "Engine 3: Actor Intelligence (who drives the story)",
            "Engine 4: Prediction (crisis probability, trajectory)",
            "Engine 5: Action Playbook (how to change the narrative)"
        ],
        "endpoints": [
            "GET  /analyze?brand=Tesla&entity_type=brand&description=electric car company",
            "GET  /playbook?brand=Tesla&entity_type=brand",
            "GET  /history?brand=Tesla",
            "GET  /alerts",
            "GET  /status",
            "POST /add_entity?name=Apple&entity_type=brand&description=iPhone Mac technology"
        ]
    }


# ── Core Fetch + Filter ───────────────────────────────────────────────────────

def fetch_and_filter(brand: str, entity_type: str, description: str):
    """
    Fetches from all Engine 1 sources and returns filtered, capped posts.

    B3 FIX: YouTube quota tracking now integrated into this function.
    Both the API endpoint path (/analyze, /playbook) and the background
    monitor path (monitor_youtube) now track quota against the same
    database counter — preventing silent over-consumption.

    B4 FIX: description passed to all sources including YouTube.
    Anchors ambiguous entity names to correct context at search level.
    """

    # ── News Sources (no quota concern) ──────────────────────────────────────
    news_posts  = get_news_mentions(brand, entity_type, description)
    gnews_posts = get_googlenews_mentions(brand, entity_type, description)
    hn_posts    = get_hackernews_mentions(brand)

    # ── YouTube Source (quota-gated) ──────────────────────────────────────────
    # B3: Check quota before making YouTube API call.
    # Both /analyze and /playbook routes go through this function,
    # so all YouTube consumption is tracked in one place.
    reset_youtube_quota_if_new_day()
    current_quota = get_youtube_quota_usage()

    if current_quota < YOUTUBE_DAILY_QUOTA:
        youtube_posts = get_youtube_mentions(brand, entity_type, description)

        # Increment counter only after successful fetch
        new_quota = current_quota + YOUTUBE_COST_PER_CALL
        set_youtube_quota_usage(new_quota)

        logger.info(
            f"[YouTube Quota] API path | +{YOUTUBE_COST_PER_CALL} units | "
            f"Total: {new_quota:,} / {YOUTUBE_DAILY_QUOTA:,} | "
            f"Entity: '{brand}'"
        )
    else:
        logger.warning(
            f"[YouTube Quota] EXHAUSTED | "
            f"Skipping YouTube for '{brand}' | "
            f"Resets at midnight Pacific"
        )
        youtube_posts = []

    # ── Filter + Cap ──────────────────────────────────────────────────────────
    news_posts    = filter_relevant(news_posts, brand)[:30]
    youtube_posts = filter_relevant(youtube_posts, brand)[:15]
    gnews_posts   = filter_relevant(gnews_posts, brand)[:30]
    hn_posts      = filter_relevant(hn_posts, brand)[:15]

    all_posts = news_posts + gnews_posts + hn_posts + youtube_posts

    return all_posts, {
        "newsapi":      len(news_posts),
        "google_news":  len(gnews_posts),
        "hacker_news":  len(hn_posts),
        "youtube":      len(youtube_posts)
    }


# ── /analyze ──────────────────────────────────────────────────────────────────

@app.get("/analyze")
def analyze(brand: str, entity_type: str = "brand", description: str = ""):
    logger.info(f"[Analyze] Request for: {brand} ({entity_type})")

    # Check cache first — serves instantly if fresh
    cached = get_analysis_cache(brand, max_age_minutes=120)
    if cached:
        logger.info(f"[Analyze] Returning cached result for '{brand}'")
        cached["served_from_cache"] = True
        return cached

    # No cache — fall back to stored description if none provided
    if not description:
        description = get_entity_description(brand)

    logger.info(f"[Analyze] No cache — running full analysis...")

    all_posts, source_counts = fetch_and_filter(brand, entity_type, description)

    logger.info(
        f"[Analyze] NewsAPI: {source_counts['newsapi']} | "
        f"GoogleNews: {source_counts['google_news']} | "
        f"HN: {source_counts['hacker_news']} | "
        f"YouTube: {source_counts['youtube']}"
    )

    if not all_posts:
        return {"brand": brand, "error": "No mentions found"}

    # Save mentions to database
    for post in all_posts:
        text = post["text"]
        source = post["source_name"]
        sentiment = analyze_sentiment([text])
        label = max(sentiment, key=sentiment.get)
        save_mention(brand, source, text, label)

    all_texts = [p["text"] for p in all_posts]

    # ── Engine 2: Understanding ───────────────────────────────────────────────
    logger.info(f"[Analyze] Running AI understanding on {len(all_texts)} posts...")
    ai_result = analyze_with_ai(brand, all_texts, entity_type)
    time.sleep(4)

    # ── Engine 3: Actor Intelligence ──────────────────────────────────────────
    logger.info(f"[Analyze] Running actor analysis...")
    actor_result = analyze_actors(brand, all_posts)
    time.sleep(4)

    # Save reputation score to history
    sentiment_counts = {
        "positive": ai_result["sentiment"]["positive_count"],
        "negative": ai_result["sentiment"]["negative_count"],
        "neutral":  ai_result["sentiment"]["neutral_count"]
    }
    score = ai_result["sentiment"]["score"]
    save_result(brand, sentiment_counts, score)

    # ── Engine 4: Prediction ──────────────────────────────────────────────────
    logger.info(f"[Analyze] Running prediction...")
    prediction = predict_trajectory(brand, ai_result, actor_result)

    result = {
        "brand":            brand,
        "entity_type":      entity_type,
        "mentions":         len(all_posts),
        "sources":          source_counts,
        "reputation_score": score,
        "sentiment":        ai_result["sentiment"],
        "topics":           ai_result["topics"],
        "narrative":        ai_result["narrative"],
        "signals":          ai_result["signals"],
        "summary":          ai_result["summary"],
        "actors":           actor_result,
        "prediction":       prediction,
        "served_from_cache": False
    }

    save_analysis_cache(brand, result)
    return result


# ── /playbook ─────────────────────────────────────────────────────────────────

@app.get("/playbook")
def playbook(brand: str, entity_type: str = "brand", description: str = ""):
    logger.info(f"[Playbook] Generating for: {brand} ({entity_type})")

    if not description:
        description = get_entity_description(brand)

    # Check if fresh analysis already exists in cache
    # If so, use it directly — skip Engines 1-4 re-run
    cached_analysis = get_analysis_cache(brand, max_age_minutes=120)

    if cached_analysis:
        logger.info(f"[Playbook] Using cached analysis for '{brand}' — skipping re-fetch")
        score    = cached_analysis.get("reputation_score", 50)
        ai_result = {
            "sentiment": cached_analysis.get("sentiment", {}),
            "narrative": cached_analysis.get("narrative", {}),
            "signals":   cached_analysis.get("signals", {}),
            "summary":   cached_analysis.get("summary", ""),
            "topics":    cached_analysis.get("topics", []),
        }
        actor_result = cached_analysis.get("actors", {})
        prediction   = cached_analysis.get("prediction", {})

    else:
        logger.info(f"[Playbook] No cache — running full analysis...")
        all_posts, source_counts = fetch_and_filter(brand, entity_type, description)

        if not all_posts:
            return {"brand": brand, "error": "No mentions found"}

        all_texts = [p["text"] for p in all_posts]

        ai_result = analyze_with_ai(brand, all_texts, entity_type)
        time.sleep(4)

        actor_result = analyze_actors(brand, all_posts)
        time.sleep(4)

        sentiment_counts = {
            "positive": ai_result["sentiment"]["positive_count"],
            "negative": ai_result["sentiment"]["negative_count"],
            "neutral":  ai_result["sentiment"]["neutral_count"]
        }
        score = ai_result["sentiment"]["score"]
        save_result(brand, sentiment_counts, score)

        prediction = predict_trajectory(brand, ai_result, actor_result)
        time.sleep(4)

    # Add reputation_score to analysis dict for Engine 5
    analysis = {
        "reputation_score": score,
        "sentiment":        ai_result.get("sentiment", {}),
        "narrative":        ai_result.get("narrative", {}),
        "signals":          ai_result.get("signals", {}),
        "summary":          ai_result.get("summary", ""),
    }

    logger.info(f"[Playbook] Running Engine 5 — Action...")
    action_plan = generate_playbook(
        entity=brand,
        entity_type=entity_type,
        analysis=analysis,
        actors=actor_result,
        prediction=prediction
    )

    return {
        "brand":              brand,
        "entity_type":        entity_type,
        "reputation_score":   score,
        "risk_level":         prediction.get("risk_level"),
        "crisis_probability": prediction.get("crisis_probability"),
        "playbook":           action_plan
    }


# ── /history ──────────────────────────────────────────────────────────────────

@app.get("/history")
def history(brand: str):
    data = get_history(brand)

    if not data:
        return {"brand": brand, "history": [], "trend": "no_data"}

    scores = [d["score"] for d in data]
    delta  = scores[0] - scores[-1] if len(scores) >= 2 else 0

    if delta >= 10:
        trend = "improving"
    elif delta <= -10:
        trend = "declining"
    else:
        trend = "stable"

    return {
        "brand":         brand,
        "current_score": scores[0],
        "trend":         trend,
        "score_delta":   delta,
        "history":       data
    }


# ── /alerts ───────────────────────────────────────────────────────────────────

@app.get("/alerts")
def get_alerts():
    entities   = get_all_entities()
    all_alerts = []

    for entity_data in entities:
        entity     = entity_data["name"] if isinstance(entity_data, dict) else entity_data
        prediction = predict_trajectory(entity)
        urgency_alerts = [
            a for a in prediction.get("alerts", [])
            if a.get("urgency") in ("high", "critical")
        ]

        if urgency_alerts:
            all_alerts.append({
                "entity":             entity,
                "current_score":      prediction.get("current_score"),
                "risk_level":         prediction.get("risk_level"),
                "crisis_probability": prediction.get("crisis_probability"),
                "alerts":             urgency_alerts
            })

    return {
        "total_entities_monitored": len(entities),
        "entities_with_alerts":     len(all_alerts),
        "alerts":                   all_alerts
    }


# ── /status ───────────────────────────────────────────────────────────────────

@app.get("/status")
def status():
    entities      = get_all_entities()
    quota_status  = get_youtube_quota_status()  # B3 — quota visibility
    entity_status = []

    for entity_data in entities:
        entity = entity_data["name"] if isinstance(entity_data, dict) else entity_data
        cached = get_analysis_cache(entity, max_age_minutes=9999)
        entity_status.append({
            "entity":      entity,
            "type":        entity_data.get("type", "brand") if isinstance(entity_data, dict) else "brand",
            "description": entity_data.get("description", "") if isinstance(entity_data, dict) else "",
            "cache":       "fresh" if cached else "stale"
        })

    return {
        "monitor":        "active",
        "schedule":       "News every 2 hours | YouTube every 2 hours",
        "youtube_quota":  quota_status,   # B3 — now visible in /status
        "entities":       entity_status
    }


# ── /add_entity ───────────────────────────────────────────────────────────────

@app.post("/add_entity")
def add_entity_api(
    name:        str,
    entity_type: str = "brand",
    description: str = ""
):
    add_entity(name, entity_type, description)
    return {
        "message":     f"'{name}' added to ReputationSync monitoring",
        "name":        name,
        "entity_type": entity_type,
        "description": description,
        "monitoring":  "active"
    }