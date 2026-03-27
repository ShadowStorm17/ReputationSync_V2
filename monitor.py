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
#   - Tracks daily API call count in database
#   - Warns at 80% usage (8,000 / 10,000 units)
#   - Gracefully skips YouTube when quota exhausted
#   - Resets counter at midnight Pacific time (YouTube's reset)
#   - Survives server restarts via persistent DB storage
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

logger = logging.getLogger(__name__)

# ── YouTube Quota Constants ──────────────────────────────────────────────────
# YouTube Data API v3 gives 10,000 units per day.
# Each search.list call costs 100 units.
# With 5 entities × 12 runs/day = 60 calls = 6,000 units minimum.
# Warning threshold at 80% to prevent silent exhaustion.

YOUTUBE_DAILY_QUOTA = 10000  # units
YOUTUBE_COST_PER_CALL = 100  # units per search.list
YOUTUBE_WARNING_THRESHOLD = 0.80  # 80% = 8,000 units


def check_youtube_quota() -> bool:
    """
    B3 — YouTube Quota Check.

    Returns True if YouTube API calls are allowed.
    Returns False if quota is exhausted or near limit.

    Checks:
    1. Reset counter if new day (Pacific time — YouTube's reset zone)
    2. Current usage vs daily quota
    3. Log warning at 80% threshold

    This prevents silent 403 errors and gives visibility into
    when YouTube Listening will be unavailable.
    """
    # Reset counter if midnight Pacific has passed
    reset_youtube_quota_if_new_day()

    # Get current usage from database
    current_usage = get_youtube_quota_usage()
    remaining_units = YOUTUBE_DAILY_QUOTA - current_usage
    usage_percentage = (current_usage / YOUTUBE_DAILY_QUOTA) * 100

    # Log quota status for visibility
    logger.info(
        f"[YouTube Quota] Usage: {current_usage:,} / {YOUTUBE_DAILY_QUOTA:,} units "
        f"({usage_percentage:.1f}%) | Remaining: {remaining_units:,} units"
    )

    # Warning at 80% threshold
    if usage_percentage >= (YOUTUBE_WARNING_THRESHOLD * 100) and usage_percentage < 100:
        logger.warning(
            f"[YouTube Quota] ⚠️  WARNING: {usage_percentage:.1f}% of daily quota used. "
            f"{remaining_units:,} units remaining. "
            f"YouTube Listening may be unavailable soon."
        )

    # Exhausted — skip YouTube calls
    if current_usage >= YOUTUBE_DAILY_QUOTA:
        logger.error(
            f"[YouTube Quota] ❌ EXHAUSTED: Daily quota fully used. "
            f"YouTube Listening disabled until midnight Pacific reset."
        )
        return False

    return True


def increment_youtube_quota(calls_made: int = 1):
    """
    B3 — Increment quota counter after successful YouTube API calls.

    Args:
        calls_made: Number of API calls made (default 1 per entity)
    """
    current = get_youtube_quota_usage()
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
    Saves full AI analysis to cache.

    Note: YouTube is handled separately in monitor_youtube()
    to allow independent quota management.
    """
    entities = get_all_entities()

    if not entities:
        logger.warning("[Monitor] No entities to monitor")
        return

    logger.info(f"[Monitor] Starting full analysis for {len(entities)} entities...")

    for entity_data in entities:
        # Handle both dict and string format from database
        if isinstance(entity_data, dict):
            entity = entity_data["name"]
            entity_type = entity_data.get("type", "brand").lower()
            description = entity_data.get("description", "")
        else:
            entity = entity_data
            entity_type = "brand"
            description = ""

        try:
            logger.info(f"\n[Monitor] ── Processing: {entity} ──")

            # Fetch from all news sources (YouTube excluded — separate function)
            news_posts = get_news_mentions(entity, entity_type, description)
            gnews_posts = get_googlenews_mentions(entity, entity_type, description)
            hn_posts = get_hackernews_mentions(entity)

            # Apply B4 language + relevance filter
            news_posts = filter_relevant(news_posts, entity)[:30]
            gnews_posts = filter_relevant(gnews_posts, entity)[:30]
            hn_posts = filter_relevant(hn_posts, entity)[:15]

            all_posts = news_posts + gnews_posts + hn_posts

            if not all_posts:
                logger.info(f"[Monitor] No posts found for {entity}, skipping")
                continue

            # Save mentions to database
            new_count = 0
            for post in all_posts:
                text = post["text"]
                source = post["source_name"]
                sentiment = analyze_sentiment([text])
                label = max(sentiment, key=sentiment.get)
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
            time.sleep(10)  # Rate limit buffer for Groq API

            # ── Engine 3: Actor Intelligence ──────────────────────────────────
            logger.info(f"[Monitor] {entity}: running actor analysis...")
            actor_result = analyze_actors(entity, all_posts)
            time.sleep(10)

            # Save reputation score to history
            sentiment_counts = {
                "positive": ai_result["sentiment"]["positive_count"],
                "negative": ai_result["sentiment"]["negative_count"],
                "neutral":  ai_result["sentiment"]["neutral_count"]
            }
            score = ai_result["sentiment"]["score"]
            save_result(entity, sentiment_counts, score)

            # ── Engine 4: Prediction ──────────────────────────────────────────
            time.sleep(10)
            logger.info(f"[Monitor] {entity}: running prediction...")
            prediction = predict_trajectory(entity)

            # Build and cache full result
            full_result = {
                "brand": entity,
                "entity_type": entity_type,
                "mentions": len(all_posts),
                "sources": {
                    "newsapi": len(news_posts),
                    "google_news": len(gnews_posts),
                    "hacker_news": len(hn_posts),
                    "youtube": 0  # YouTube tracked separately
                },
                "reputation_score": score,
                "sentiment": ai_result["sentiment"],
                "topics": ai_result["topics"],
                "narrative": ai_result["narrative"],
                "signals": ai_result["signals"],
                "summary": ai_result["summary"],
                "actors": actor_result,
                "prediction": prediction,
                "cached": True
            }

            save_analysis_cache(entity, full_result)
            logger.info(f"[Monitor] {entity}: complete ✓ score={score}")

            # Wait between entities to avoid API rate limits
            logger.info(f"[Monitor] Waiting 60s before next entity...")
            time.sleep(60)

        except Exception as e:
            logger.error(f"[Monitor] Error processing '{entity}': {e}")
            time.sleep(60)


def monitor_youtube():
    """
    Runs every 2 hours — YouTube mentions only.

    B3 FIX: Quota tracking integrated.
    - Checks quota before each entity
    - Skips YouTube gracefully when exhausted
    - Increments counter after each successful call
    - Logs quota status for visibility
    """
    entities = get_all_entities()

    if not entities:
        return

    logger.info(f"[Monitor-YouTube] Checking {len(entities)} entities...")

    # B3 — Check quota once at start of YouTube run
    if not check_youtube_quota():
        logger.warning(
            "[Monitor-YouTube] Quota exhausted — skipping all YouTube mentions "
            "until midnight Pacific reset."
        )
        return

    for entity_data in entities:
        if isinstance(entity_data, dict):
            entity = entity_data["name"]
            entity_type = entity_data.get("type", "brand").lower()
            description = entity_data.get("description", "")
        else:
            entity = entity_data
            entity_type = "brand"
            description = ""

        try:
            # B3 — Re-check quota before each entity (in case we're near limit)
            if not check_youtube_quota():
                logger.warning(
                    f"[Monitor-YouTube] Quota exhausted mid-run. "
                    f"Skipping remaining entities."
                )
                break

            # Fetch YouTube mentions with full context (B4 fix: description included)
            posts = get_youtube_mentions(entity, entity_type, description)
            posts = filter_relevant(posts, entity)[:15]

            # B3 — Increment quota after successful API call
            increment_youtube_quota(calls_made=1)

            # Save mentions to database
            new_count = 0
            for post in posts:
                text = post["text"]
                source = post["source_name"]
                sentiment = analyze_sentiment([text])
                label = max(sentiment, key=sentiment.get)
                try:
                    save_mention(entity, source, text, label)
                    new_count += 1
                except Exception:
                    pass

            logger.info(f"[Monitor-YouTube] {entity}: {new_count} new mentions saved")

        except Exception as e:
            logger.error(f"[Monitor-YouTube] Error for '{entity}': {e}")


# ── Schedule Configuration ───────────────────────────────────────────────────
# Both monitors run every 2 hours but are staggered slightly
# to avoid simultaneous API bursts.

schedule.every(2).hours.do(monitor_news)
schedule.every(2).hours.do(monitor_youtube)


def start_monitor():
    """
    Entry point for background monitoring thread.

    Runs initial analysis immediately on startup,
    then enters scheduled loop.
    """
    logger.info("[Monitor] Started")
    logger.info("[Monitor] Full AI analysis: every 2 hours")
    logger.info("[Monitor] YouTube: every 2 hours")
    logger.info("[Monitor] YouTube quota tracking: enabled (B3)")

    # Run initial analysis on startup
    monitor_news()

    # Enter scheduled loop
    while True:
        schedule.run_pending()
        time.sleep(10)