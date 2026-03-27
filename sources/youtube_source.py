import os
import requests
import logging

logger = logging.getLogger(__name__)


def fetch_youtube_videos(entity_name: str, entity_type: str, description: str, max_results: int = 15) -> list:
    """
    Fetches YouTube videos for a given entity to feed Engine 1 — Listening.

    Constructs a context-aware search query combining entity name and
    description keywords to reduce Narrative Friction caused by
    ambiguous entity names (partial B2 mitigation at source level).

    Args:
        entity_name:  The monitored entity (e.g., "Elon Musk", "Tesla")
        entity_type:  Entity classification (e.g., "person", "brand")
        description:  Context keywords to sharpen Actor Authority signal
                      (e.g., "Tesla SpaceX Twitter founder CEO")
        max_results:  YouTube result cap — default 15 per entity (quota-conscious)

    Returns:
        List of video dicts compatible with Engine 1 → Engine 2 pipeline schema.
        Returns empty list on any failure — never raises, never breaks the monitor.

    B6 Fix Applied:
        Raw string passed directly into params dict.
        requests library handles percent-encoding automatically and correctly.
        No manual quote() call — that was causing double-encoding and 400 errors.
    """

    api_key = os.getenv("YOUTUBE_API_KEY")

    if not api_key:
        logger.error(
            "[YouTube] YOUTUBE_API_KEY not set in environment. "
            "Engine 1 YouTube Listening layer is disabled."
        )
        return []

    # ── Build Search Query ────────────────────────────────────────────────────
    # Combine entity name with description context to sharpen search signal.
    # Description words anchor ambiguous names to the correct entity.
    # Example: "Apple" alone returns fruit news.
    #          "Apple iPhone Mac software" returns the correct brand narrative.
    #
    # We limit description to first 5 words to avoid over-constraining results
    # while still providing enough context to reduce Narrative Friction.

    if description:
        description_keywords = " ".join(description.strip().split()[:5])
        search_query = f"{entity_name} {description_keywords}"
    else:
        search_query = entity_name

    # ── API Parameters ────────────────────────────────────────────────────────
    # B6 FIX: search_query is passed RAW into params dict.
    # requests encodes this correctly and automatically.
    # Do NOT wrap search_query in quote() here — that causes double-encoding.

    params = {
        "part": "snippet",
        "q": search_query,        # Raw string — requests handles encoding
        "type": "video",
        "maxResults": max_results,
        "key": api_key,
        "order": "relevance",
        "relevanceLanguage": "en",  # B4 synergy — reduces non-English at source
        "videoEmbeddable": "true",
    }

    logger.info(
        f"[YouTube] Fetching mentions | Entity: '{entity_name}' | "
        f"Type: '{entity_type}' | Query: '{search_query}'"
    )

    # ── API Request ───────────────────────────────────────────────────────────

    try:
        response = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params=params,
            timeout=15
        )

        # ── Explicit Status Handling ──────────────────────────────────────────
        # Granular error logging gives visibility into quota and encoding issues.
        # This replaces the silent failure that previously masked B6.

        if response.status_code == 400:
            logger.error(
                f"[YouTube] 400 badRequest for '{entity_name}'. "
                f"Constructed query: '{search_query}'. "
                f"API response: {response.text[:300]}"
            )
            return []

        if response.status_code == 403:
            logger.warning(
                f"[YouTube] 403 Forbidden for '{entity_name}'. "
                f"Daily quota likely exhausted. "
                f"See B3 — YouTube quota tracking not yet implemented."
            )
            return []

        if response.status_code == 404:
            logger.error(
                f"[YouTube] 404 for '{entity_name}'. "
                f"Endpoint may have changed. Check YouTube Data API v3 docs."
            )
            return []

        response.raise_for_status()
        data = response.json()

    except requests.exceptions.Timeout:
        logger.warning(
            f"[YouTube] Request timed out for '{entity_name}'. "
            f"Skipping this cycle — monitor will retry in 2 hours."
        )
        return []

    except requests.exceptions.ConnectionError:
        logger.warning(
            f"[YouTube] Connection error for '{entity_name}'. "
            f"Network issue or YouTube API unreachable."
        )
        return []

    except requests.exceptions.RequestException as e:
        logger.error(f"[YouTube] Unexpected request error for '{entity_name}': {e}")
        return []

    # ── Parse Response ────────────────────────────────────────────────────────

    items = data.get("items", [])

    if not items:
        logger.info(
            f"[YouTube] Zero results returned for '{entity_name}'. "
            f"Query: '{search_query}'"
        )
        return []

    videos = []

    for item in items:
        try:
            snippet = item.get("snippet", {})
            video_id = item.get("id", {}).get("videoId")

            if not video_id:
                # Item is a channel or playlist result — skip
                continue

            title = snippet.get("title", "").strip()
            channel = snippet.get("channelTitle", "Unknown Channel").strip()
            published_at = snippet.get("publishedAt", "")
            description_text = snippet.get("description", "").strip()

            if not title:
                continue

            videos.append({
                "title": title,
                "channel": channel,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "published_at": published_at,
                "description": description_text,
                # Combined text field for Engine 2 — Understanding analysis
                # Title carries primary narrative signal; description adds depth
                "text": f"{title}. {description_text}".strip(". "),
                "source": f"YouTube — {channel}",
                "entity": entity_name,
            })

        except Exception as e:
            logger.warning(
                f"[YouTube] Failed to parse video item for '{entity_name}': {e}. "
                f"Skipping item."
            )
            continue

    logger.info(
        f"[YouTube] Retrieved {len(videos)} videos for '{entity_name}' | "
        f"Query: '{search_query}'"
    )

    return videos