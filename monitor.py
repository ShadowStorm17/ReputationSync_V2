import schedule
import time

from database import get_all_entities, save_result, save_analysis_cache
from sentiment import analyze_sentiment
from sources.news_source import get_news_mentions
from sources.googlenews_source import get_googlenews_mentions
from sources.hackernews_source import get_hackernews_mentions
from database import save_mention
from filter import filter_relevant
from engine_understanding import analyze_with_ai
from engine_actors import analyze_actors
from engine_prediction import predict_trajectory


def monitor_news():
    """
    Runs every 2 hours.
    Processes one entity at a time with 60s gap between each.
    Saves full AI analysis to cache.
    """

    entities = get_all_entities()

    if not entities:
        print("[Monitor] No entities to monitor")
        return

    print(f"[Monitor] Starting full analysis for {len(entities)} entities...")

    for entity_data in entities:
        # Handle both dict and string format
        if isinstance(entity_data, dict):
            entity = entity_data["name"]
            entity_type = entity_data.get("type", "brand").lower()
            description = entity_data.get("description", "")
        else:
            entity = entity_data
            entity_type = "brand"
            description = ""

        try:
            print(f"\n[Monitor] ── Processing: {entity} ──")

            # Fetch from all news sources
            news_posts = get_news_mentions(entity, entity_type, description)
            gnews_posts = get_googlenews_mentions(entity, entity_type, description)
            hn_posts = get_hackernews_mentions(entity)

            news_posts = filter_relevant(news_posts, entity)[:30]
            gnews_posts = filter_relevant(gnews_posts, entity)[:30]
            hn_posts = filter_relevant(hn_posts, entity)[:15]

            all_posts = news_posts + gnews_posts + hn_posts

            if not all_posts:
                print(f"[Monitor] No posts found for {entity}, skipping")
                continue

            # Save mentions
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

            print(f"[Monitor] {entity}: {new_count} new mentions")

            all_texts = [p["text"] for p in all_posts]

            # Engine 2
            print(f"[Monitor] {entity}: running AI understanding...")
            ai_result = analyze_with_ai(entity, all_texts, entity_type)
            time.sleep(10)

            # Engine 3
            print(f"[Monitor] {entity}: running actor analysis...")
            actor_result = analyze_actors(entity, all_posts)
            time.sleep(10)

            # Save score
            sentiment_counts = {
                "positive": ai_result["sentiment"]["positive_count"],
                "negative": ai_result["sentiment"]["negative_count"],
                "neutral":  ai_result["sentiment"]["neutral_count"]
            }
            score = ai_result["sentiment"]["score"]
            save_result(entity, sentiment_counts, score)

            # Engine 4
            time.sleep(10)
            print(f"[Monitor] {entity}: running prediction...")
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
                    "youtube": 0
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
            print(f"[Monitor] {entity}: complete ✓ score={score}")

            # Wait between entities
            print(f"[Monitor] Waiting 60s before next entity...")
            time.sleep(60)

        except Exception as e:
            print(f"[Monitor] Error processing '{entity}': {e}")
            time.sleep(60)


def monitor_youtube():
    """Runs every 2 hours — YouTube only."""

    entities = get_all_entities()

    if not entities:
        return

    print(f"[Monitor-YouTube] Checking {len(entities)} entities...")

    for entity_data in entities:
        if isinstance(entity_data, dict):
            entity = entity_data["name"]
        else:
            entity = entity_data

        try:
            from sources.youtube_source import get_youtube_mentions
            posts = get_youtube_mentions(entity)
            posts = filter_relevant(posts, entity)[:15]

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

            print(f"[Monitor-YouTube] {entity}: {new_count} new mentions")

        except Exception as e:
            print(f"[Monitor-YouTube] Error for '{entity}': {e}")


schedule.every(2).hours.do(monitor_news)
schedule.every(2).hours.do(monitor_youtube)


def start_monitor():
    print("[Monitor] Started")
    print("[Monitor] Full AI analysis: every 2 hours")
    print("[Monitor] YouTube: every 2 hours")

    monitor_news()

    while True:
        schedule.run_pending()
        time.sleep(10)