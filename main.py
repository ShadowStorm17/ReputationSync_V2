from fastapi import FastAPI
from engine_understanding import analyze_with_ai
from engine_actors import analyze_actors
from engine_prediction import predict_trajectory
from engine_action import generate_playbook
from database import init_db, save_result, save_analysis_cache, get_analysis_cache
from database import get_history, add_entity, save_mention, get_entity_description
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

app = FastAPI(
    title="ReputationSync Intelligence Engine",
    description="AI-powered reputation intelligence platform",
    version="3.0.0"
)

# Default entities to monitor
# Edit this list to change what gets monitored on every server start
DEFAULT_ENTITIES = [
    ("Tesla", "brand", "Electric car company Elon Musk"),
    ("Nike", "brand", "Sports apparel footwear company"),
    ("Amazon", "brand", "Jeff Bezos ecommerce cloud technology"),
    ("Nvidia", "brand", "GPU semiconductor AI chips company"),
    ("Elon Musk", "person", "Tesla SpaceX Twitter founder CEO"),
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
    time.sleep(30)
    start_monitor()


# Initialize database, seed entities, start monitor
init_db()
seed_entities()
threading.Thread(target=start_monitor_delayed, daemon=True).start()


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


def fetch_and_filter(brand: str, entity_type: str, description: str):
    
    news_posts   = get_news_mentions(brand, entity_type, description)
    youtube_posts = get_youtube_mentions(brand, entity_type, description)  # ← B4 fix: description added
    gnews_posts  = get_googlenews_mentions(brand, entity_type, description)
    hn_posts     = get_hackernews_mentions(brand)

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


@app.get("/analyze")
def analyze(brand: str, entity_type: str = "brand", description: str = ""):
    print(f"[Analyze] Request for: {brand} ({entity_type})")

    # Check cache first
    cached = get_analysis_cache(brand, max_age_minutes=120)
    if cached:
        print(f"[Analyze] Returning cached result for '{brand}'")
        cached["served_from_cache"] = True
        return cached

    # No cache — check stored description
    if not description:
        description = get_entity_description(brand)

    print(f"[Analyze] No cache — running full analysis...")

    all_posts, source_counts = fetch_and_filter(brand, entity_type, description)

    print(f"[Analyze] NewsAPI: {source_counts['newsapi']} | "
          f"GoogleNews: {source_counts['google_news']} | "
          f"HN: {source_counts['hacker_news']} | "
          f"YouTube: {source_counts['youtube']}")

    if not all_posts:
        return {"brand": brand, "error": "No mentions found"}

    # Save mentions
    for post in all_posts:
        text = post["text"]
        source = post["source_name"]
        sentiment = analyze_sentiment([text])
        label = max(sentiment, key=sentiment.get)
        save_mention(brand, source, text, label)

    all_texts = [p["text"] for p in all_posts]

    # Engine 2 — Understanding
    print(f"[Analyze] Running AI understanding on {len(all_texts)} posts...")
    ai_result = analyze_with_ai(brand, all_texts, entity_type)
    time.sleep(4)

    # Engine 3 — Actors
    print(f"[Analyze] Running actor analysis...")
    actor_result = analyze_actors(brand, all_posts)
    time.sleep(4)

    # Save score
    sentiment_counts = {
        "positive": ai_result["sentiment"]["positive_count"],
        "negative": ai_result["sentiment"]["negative_count"],
        "neutral":  ai_result["sentiment"]["neutral_count"]
    }
    score = ai_result["sentiment"]["score"]
    save_result(brand, sentiment_counts, score)

    # Engine 4 — Prediction
    print(f"[Analyze] Running prediction...")
    prediction = predict_trajectory(brand)

    result = {
        "brand": brand,
        "entity_type": entity_type,
        "mentions": len(all_posts),
        "sources": source_counts,
        "reputation_score": score,
        "sentiment": ai_result["sentiment"],
        "topics": ai_result["topics"],
        "narrative": ai_result["narrative"],
        "signals": ai_result["signals"],
        "summary": ai_result["summary"],
        "actors": actor_result,
        "prediction": prediction,
        "served_from_cache": False
    }

    save_analysis_cache(brand, result)
    return result


@app.get("/playbook")
def playbook(brand: str, entity_type: str = "brand", description: str = ""):
    print(f"[Playbook] Generating for: {brand} ({entity_type})")

    if not description:
        description = get_entity_description(brand)

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

    prediction = predict_trajectory(brand)
    time.sleep(4)

    analysis = {
        "reputation_score": score,
        "sentiment": ai_result["sentiment"],
        "narrative": ai_result["narrative"],
        "signals": ai_result["signals"],
        "summary": ai_result["summary"]
    }

    print(f"[Playbook] Running action engine...")
    action_plan = generate_playbook(
        entity=brand,
        entity_type=entity_type,
        analysis=analysis,
        actors=actor_result,
        prediction=prediction
    )

    return {
        "brand": brand,
        "entity_type": entity_type,
        "reputation_score": score,
        "risk_level": prediction.get("risk_level"),
        "crisis_probability": prediction.get("crisis_probability"),
        "playbook": action_plan
    }


@app.get("/history")
def history(brand: str):
    data = get_history(brand)

    if not data:
        return {"brand": brand, "history": [], "trend": "no_data"}

    scores = [d["score"] for d in data]
    delta = scores[0] - scores[-1] if len(scores) >= 2 else 0

    if delta >= 10:
        trend = "improving"
    elif delta <= -10:
        trend = "declining"
    else:
        trend = "stable"

    return {
        "brand": brand,
        "current_score": scores[0],
        "trend": trend,
        "score_delta": delta,
        "history": data
    }


@app.get("/alerts")
def get_alerts():
    from database import get_all_entities
    entities = get_all_entities()
    all_alerts = []

    for entity_data in entities:
        entity = entity_data["name"] if isinstance(entity_data, dict) else entity_data
        prediction = predict_trajectory(entity)
        urgency_alerts = [
            a for a in prediction.get("alerts", [])
            if a.get("urgency") in ("high", "critical")
        ]

        if urgency_alerts:
            all_alerts.append({
                "entity": entity,
                "current_score": prediction.get("current_score"),
                "risk_level": prediction.get("risk_level"),
                "crisis_probability": prediction.get("crisis_probability"),
                "alerts": urgency_alerts
            })

    return {
        "total_entities_monitored": len(entities),
        "entities_with_alerts": len(all_alerts),
        "alerts": all_alerts
    }


@app.get("/status")
def status():
    from database import get_all_entities
    entities = get_all_entities()

    entity_status = []
    for entity_data in entities:
        entity = entity_data["name"] if isinstance(entity_data, dict) else entity_data
        cached = get_analysis_cache(entity, max_age_minutes=9999)
        entity_status.append({
            "entity": entity,
            "type": entity_data.get("type", "brand") if isinstance(entity_data, dict) else "brand",
            "description": entity_data.get("description", "") if isinstance(entity_data, dict) else "",
            "cache": "fresh" if cached else "stale"
        })

    return {
        "monitor": "active",
        "schedule": "News every 2 hours | YouTube every 2 hours",
        "entities": entity_status
    }


@app.post("/add_entity")
def add_entity_api(
    name: str,
    entity_type: str = "brand",
    description: str = ""
):
    add_entity(name, entity_type, description)
    return {
        "message": f"'{name}' added to ReputationSync monitoring",
        "name": name,
        "entity_type": entity_type,
        "description": description,
        "monitoring": "active"
    }