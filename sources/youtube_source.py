# sources/youtube_source.py
# ============================================================
# Engine 1 — Listening Layer: YouTube Source
# ReputationSync — Actor Authority signal acquisition via video
#
# B6 FIX: Raw query string passed directly into params dict.
# requests handles URL encoding automatically and correctly.
# Previous manual quote() caused double-encoding → 400 badRequest.
# ============================================================

import os
import requests
import logging

logger = logging.getLogger(__name__)

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


def get_youtube_mentions(entity_name: str, entity_type: str = "brand", description: str = "", max_results: int = 15) -> list:
    """
    Fetches YouTube videos for a given entity to feed Engine 1 — Listening.

    Args:
        entity_name:  The monitored entity (e.g., "Elon Musk", "Tesla")
        entity_type:  Entity classification (e.g., "person", "brand")
        description:  Context keywords to reduce Narrative Friction
        max_results:  YouTube result cap — default 15 (quota-conscious)

    Returns:
        List of post dicts with "text" and "source_name" keys,
        compatible with filter_relevant() and the Engine 2 pipeline.
    """
    if not YOUTUBE_API_KEY:
        logger.error("[YouTube] YOUTUBE_API_KEY not set — YouTube Listening disabled.")
        return []

    # Build context-aware search query
    if description:
        description_keywords = " ".join(description.strip().split()[:5])
        search_query = f"{entity_name} {description_keywords}"
    else:
        search_query = entity_name

    # B6 FIX: Raw string in params dict — requests encodes automatically
    # Do NOT wrap in quote() — that causes double-encoding → 400 badRequest
    params = {
        "part": "snippet",
        "q": search_query,
        "type": "video",
        "maxResults": max_results,
        "key": YOUTUBE_API_KEY,
        "order": "relevance",
        "relevanceLanguage": "en",
        "videoEmbeddable": "true",
    }

    logger.info(
        f"[YouTube] Fetching mentions | Entity: '{entity_name}' | "
        f"Type: '{entity_type}' | Query: '{search_query}'"
    )

    try:
        response = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params=params,
            timeout=15
        )

        if response.status_code == 400:
            logger.error(
                f"[YouTube] 400 badRequest for '{entity_name}'. "
                f"Query: '{search_query}'. "
                f"Response: {response.text[:300]}"
            )
            return []

        if response.status_code == 403:
            logger.warning(
                f"[YouTube] 403 Forbidden for '{entity_name}'. "
                f"Daily quota likely exhausted."
            )
            return []

        response.raise_for_status()
        data = response.json()

    except requests.exceptions.Timeout:
        logger.warning(f"[YouTube] Timeout for '{entity_name}' — skipping.")
        return []

    except requests.exceptions.ConnectionError:
        logger.warning(f"[YouTube] Connection error for '{entity_name}'.")
        return []

    except requests.exceptions.RequestException as e:
        logger.error(f"[YouTube] Request error for '{entity_name}': {e}")
        return []

    # Parse results into pipeline-compatible format
    items = data.get("items", [])

    if not items:
        logger.info(f"[YouTube] Zero results for '{entity_name}'.")
        return []

    posts = []

    for item in items:
        try:
            snippet = item.get("snippet", {})
            video_id = item.get("id", {}).get("videoId")

            if not video_id:
                continue

            title = snippet.get("title", "").strip()
            channel = snippet.get("channelTitle", "Unknown Channel").strip()
            published_at = snippet.get("publishedAt", "")
            description_text = snippet.get("description", "").strip()

            if not title:
                continue

            # Unified text field for filter.py and Engine 2 compatibility
            combined_text = f"{title}. {description_text}".strip(". ")

            posts.append({
                "text": combined_text,
                "source_name": f"YouTube — {channel}",
                "title": title,
                "channel": channel,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "published_at": published_at,
                "description": description_text,
            })

        except Exception as e:
            logger.warning(f"[YouTube] Failed to parse item for '{entity_name}': {e}")
            continue

    logger.info(f"[YouTube] Retrieved {len(posts)} videos for '{entity_name}'")

    return posts