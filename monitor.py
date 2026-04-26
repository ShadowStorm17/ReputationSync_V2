# monitor.py
# ============================================================
# ReputationSync — Background Monitoring Layer
# Engine 1 Listening automation across all sources
#
# Schedule:
#   - Full AI analysis (News + GNews + HN): every 2 hours
#   - YouTube mentions: every 2 hours
#   - 60s gap between entities to avoid API rate limits
#
# B3 FIX: YouTube quota tracking added.
# Engine 0 (Formation Detection) added to monitoring cycle.
# ============================================================

import schedule
import time
import logging
from datetime import datetime, timezone
from database import (
    get_all_entities,
    save_result,
    save_analysis_cache,
    save_mention,
    get_youtube_quota_usage,
    set_youtube_quota_usage,
    reset_youtube_quota_if_new_day
)
from sentiment import analyze_sentiment
from sources.news_source import get_news_mentions
from sources.googlenews_source import get_googlenews_mentions
from sources.hackernews_source import get_hackernews_mentions
from sources.youtube_source import get_youtube_mentions
from filter import filter_relevant
from engine_understanding import analyze_with_ai
from engine_actors import analyze_actors
from engine_prediction import predict_trajectory
from engine_control_score import calculate_control_score
from engine_trajectory import model_trajectory
from engine_formation import detect_formation

logger = logging.getLogger(__name__)

# ── YouTube Quota Constants ──────────────────────────────────────────────────
YOUTUBE_DAILY_QUOTA       = 10000
YOUTUBE_COST_PER_CALL     = 100
YOUTUBE_WARNING_THRESHOLD = 0.80


def check_youtube_quota() -> bool:
    reset_youtube_quota_if_new_day()
    current_usage    = get_youtube_quota_usage()
    remaining_units  = YOUTUBE_DAILY_QUOTA - current_usage
    usage_percentage = (current_usage / YOUTUBE_DAILY_QUOTA) * 100

    logger.info(
        f"[YouTube Quota] Usage: {current_usage:,} / {YOUTUBE_DAILY_QUOTA:,} units "
        f"({usage_percentage:.1f}%) | Remaining: {remaining_units:,} units"
    )

    if usage_percentage >= (YOUTUBE_WARNING_THRESHOLD * 100) and usage_percentage < 100:
        logger.warning(
            f"[YouTube Quota] WARNING: {usage_percentage:.1f}% of daily quota used. "
            f"{remaining_units:,} units remaining."
        )

    if current_usage >= YOUTUBE_DAILY_QUOTA:
        logger.error(
            f"[YouTube Quota] EXHAUSTED: Daily quota fully used. "
            f"YouTube Listening disabled until midnight Pacific reset."
        )
        return False

    return True


def increment_youtube_quota(calls_made: int = 1):
    current   = get_youtube_quota_usage()
    new_total = current + (calls_made * YOUTUBE_COST_PER_CALL)
    set_youtube_quota_usage(new_total)
    logger.debug(
        f"[YouTube Quota] Incremented by {calls_made * YOUTUBE_COST_PER_CALL} units. "
        f"New total: {new_total:,} / {YOUTUBE_DAILY_QUOTA:,}"
    )


def monitor_news():
    """
    Runs every 2 hours.
    Processes one entity at a time with 60s gap between each.
    Now includes Engine 0 (Formation), Engine 6 (Control),
    Engine 7 (Trajectory) in the monitoring cycle.
    """
    entities = get_all_entities()

    if not entities:
        logger.warning("[Monitor] No entities to monitor")
        return

    logger.info(f"[Monitor] Starting full analysis for {len(entities)} entities...")

    for entity_data in entities:
        if isinstance(entity_data, dict):
            entity      = entity_data["name"]
            entity_type = entity_data.get("type", "brand").lower()
            description = entity_data.get("description", "")
        else:
            entity      = entity_data
            entity_type = "brand"
            description = ""

        try:
            logger.info(f"\n[Monitor] ── Processing: {entity} ──")

            # ── Fetch sources ─────────────────────────────────────────────────
            news_posts  = get_news_mentions(entity, entity_type, description)
            gnews_posts = get_googlenews_mentions(entity, entity_type, description)
            hn_posts    = get_hackernews_mentions(entity)

            news_posts  = filter_relevant(news_posts, entity)[:30]
            gnews_posts = filter_relevant(gnews_posts, entity)[:30]
            hn_posts    = filter_relevant(hn_posts, entity)[:15]

            all_posts = news_posts + gnews_posts + hn_posts

            if not all_posts:
                logger.info(f"[Monitor] No posts found for {entity}, skipping")
                continue

            # ── Save mentions ─────────────────────────────────────────────────
            new_count = 0
            for post in all_posts:
                text      = post["text"]
                source    = post["source_name"]
                sentiment = analyze_sentiment([text])
                label     = max(sentiment, key=sentiment.get)
                try:
                    save_mention(entity, source, text, label)
                    new_count += 1
                except Exception:
                    pass

            logger.info(f"[Monitor] {entity}: {new_count} new mentions saved")

            all_texts = [p["text"] for p in all_posts]

            # ── Engine 2: Understanding ───────────────────────────────────────
            logger.info(f"[Monitor] {entity}: running AI understanding...")
            ai_result = analyze_with_ai(entity, all_texts, entity_type)
            time.sleep(15)

            # ── Engine 3: Actor Intelligence ──────────────────────────────────
            logger.info(f"[Monitor] {entity}: running actor analysis...")
            actor_result = analyze_actors(entity, all_posts)
            time.sleep(15)

            # ── Save score to history ─────────────────────────────────────────
            sentiment_counts = {
                "positive": ai_result["sentiment"]["positive_count"],
                "negative": ai_result["sentiment"]["negative_count"],
                "neutral":  ai_result["sentiment"]["neutral_count"]
            }
            score = ai_result["sentiment"]["score"]
            save_result(entity, sentiment_counts, score)

            # ── Engine 6: Control Score ───────────────────────────────────────
            logger.info(f"[Monitor] {entity}: calculating control score...")
            control_result = calculate_control_score(
                entity=entity,
                engine2_result=ai_result,
                engine3_result=actor_result,
                formation_result=None
            )
            time.sleep(5)

            # ── Engine 7: Trajectory Model ────────────────────────────────────
            logger.info(f"[Monitor] {entity}: modeling trajectory...")
            trajectory_result = model_trajectory(
                entity=entity,
                entity_type=entity_type,
                engine2_result=ai_result,
                engine3_result=actor_result,
                control_result=control_result
            )
            time.sleep(15)

            # ── Engine 4: Prediction ──────────────────────────────────────────
            logger.info(f"[Monitor] {entity}: running prediction...")
            prediction = predict_trajectory(entity, ai_result, actor_result)
            time.sleep(10)

            # ── Engine 0: Formation Detection ─────────────────────────────────
            logger.info(f"[Monitor] {entity}: running formation detection...")
            formation_result = detect_formation(
                entity=entity,
                current_posts=all_posts,
                engine2_result=ai_result
            )

            if formation_result.get("signal_detected"):
                logger.info(
                    f"[Monitor] {entity}: FORMATION SIGNAL DETECTED | "
                    f"stage: {formation_result.get('stage')} | "
                    f"confidence: {formation_result.get('confidence')}%"
                )
            else:
                logger.info(
                    f"[Monitor] {entity}: no formation signal "
                    f"({formation_result.get('reason', 'unknown')})"
                )

            # ── Build and cache full result ───────────────────────────────────
            full_result = {
                "brand":            entity,
                "entity_type":      entity_type,
                "mentions":         len(all_posts),
                "sources": {
                    "newsapi":      len(news_posts),
                    "google_news":  len(gnews_posts),
                    "hacker_news":  len(hn_posts),
                    "youtube":      0
                },
                "reputation_score": score,
                "sentiment":        ai_result["sentiment"],
                "topics":           ai_result["topics"],
                "narrative":        ai_result["narrative"],
                "signals":          ai_result["signals"],
                "summary":          ai_result["summary"],
                "actors":           actor_result,
                "control":          control_result,
                "trajectory":       trajectory_result,
                "formation":        formation_result,
                "prediction":       prediction,
                "cached":           True
            }

            save_analysis_cache(entity, full_result)
            logger.info(
                f"[Monitor] {entity}: complete ✓ "
                f"score={score} | "
                f"control={control_result.get('narrative_control_score')}/100 | "
                f"window={control_result.get('intervention_window')}"
            )

            logger.info(f"[Monitor] Waiting 60s before next entity...")
            time.sleep(60)

        except Exception as e:
            logger.error(f"[Monitor] Error processing '{entity}': {e}")
            time.sleep(60)


def monitor_youtube():
    """
    Runs every 2 hours — YouTube mentions only.
    B3 FIX: Quota tracking integrated.
    """
    entities = get_all_entities()

    if not entities:
        return

    logger.info(f"[Monitor-YouTube] Checking {len(entities)} entities...")

    if not check_youtube_quota():
        logger.warning(
            "[Monitor-YouTube] Quota exhausted — skipping all YouTube mentions."
        )
        return

    for entity_data in entities:
        if isinstance(entity_data, dict):
            entity      = entity_data["name"]
            entity_type = entity_data.get("type", "brand").lower()
            description = entity_data.get("description", "")
        else:
            entity      = entity_data
            entity_type = "brand"
            description = ""

        try:
            if not check_youtube_quota():
                logger.warning(
                    f"[Monitor-YouTube] Quota exhausted mid-run. "
                    f"Skipping remaining entities."
                )
                break

            posts = get_youtube_mentions(entity, entity_type, description)
            posts = filter_relevant(posts, entity)[:15]

            increment_youtube_quota(calls_made=1)

            new_count = 0
            for post in posts:
                text      = post["text"]
                source    = post["source_name"]
                sentiment = analyze_sentiment([text])
                label     = max(sentiment, key=sentiment.get)
                try:
                    save_mention(entity, source, text, label)
                    new_count += 1
                except Exception:
                    pass

            logger.info(f"[Monitor-YouTube] {entity}: {new_count} new mentions saved")

        except Exception as e:
            logger.error(f"[Monitor-YouTube] Error for '{entity}': {e}")


# ── Schedule ──────────────────────────────────────────────────────────────────

schedule.every(2).hours.do(monitor_news)
schedule.every(2).hours.do(monitor_youtube)


def start_monitor():
    logger.info("[Monitor] Started")
    logger.info("[Monitor] Full AI analysis: every 2 hours")
    logger.info("[Monitor] YouTube: every 2 hours")
    logger.info("[Monitor] Formation detection: every cycle")
    logger.info("[Monitor] Control score: every cycle")
    logger.info("[Monitor] Trajectory model: every cycle")

    monitor_news()

    while True:
        schedule.run_pending()
        time.sleep(10)