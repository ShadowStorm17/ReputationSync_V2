# filter.py
# ============================================================
# Engine 1 — Listening Layer: Relevance + Language Filter
# ReputationSync — Narrative Friction reduction at ingestion
#
# Purpose: Ensures only English, brand-relevant content
# reaches Engine 2 — Understanding for AI analysis.
#
# Non-English content pollutes narrative scoring by introducing
# foreign sentiment signals that the AI misreads as English.
# This filter is the quality gate between raw ingestion and
# AI analysis.
#
# B4 Fix: Added "text" key normalization for all source types.
# YouTube returns title+description, not a unified "text" field.
# This caused filter_relevant() to silently pass all YouTube
# content through unfiltered — KeyError swallowed by isinstance.
# ============================================================

import re
import logging

logger = logging.getLogger(__name__)

# ── Non-English Word Signals ──────────────────────────────────────────────────
# High-frequency function words unique to each language.
# These appear constantly in native text but rarely in English.
# Threshold: if >8% of words in a post match these, reject it.

NON_ENGLISH_SIGNALS = {
    # German
    "und", "der", "die", "das", "ein", "eine", "ist", "wird",
    "mit", "fur", "auf", "bei", "von", "aus", "nach", "uber",
    "nicht", "auch", "dass", "sich", "werden", "oder", "aber",
    "noch", "mehr", "hier", "finden", "thema", "lesen", "beim",
    # French
    "les", "des", "une", "pour", "sur", "avec", "dans", "par",
    "qui", "que", "pas", "plus", "comme", "mais", "tout", "elle",
    "nous", "vous", "ils", "leur", "cette", "sont", "etait",
    # Spanish
    "los", "las", "una", "por", "con", "del", "sus",
    "como", "este", "esta", "pero", "muy", "hay", "ser",
    "tiene", "estan", "entre", "cuando", "tambien", "porque",
    # Italian
    "della", "dello", "degli", "alle", "agli", "nel", "nella",
    "sono", "questo", "questa", "anche", "pero", "quando",
    # Portuguese
    "para", "uma", "com", "nao", "por", "seu", "sua",
    "isso", "esse", "pelo", "pela", "tem",
    # Dutch
    "een", "het", "van", "zijn", "deze", "naar",
    "heeft", "kunnen", "hebben",
}

# ── Boilerplate Signals ───────────────────────────────────────────────────────
# Full phrases that definitively identify non-English aggregator
# or publisher boilerplate. One match = immediate reject.

BOILERPLATE_SIGNALS = [
    "hier finden sie",
    "lesen sie mehr",
    "mehr informationen",
    "alle rechte vorbehalten",
    "datenschutz",
    "impressum",
    "newsletter abonnieren",
    "zu diesem thema",
    "informationen thema",
    "finden informationen",
]


def extract_text(post: dict | str) -> str:
    """
    Extracts a unified text string from any post format.

    Different sources return different dict schemas:
    - NewsAPI / GNews:    {"text": "...", "source_name": "..."}
    - HackerNews:         {"text": "...", "source_name": "..."}
    - YouTube (old):      {"title": "...", "description": "..."}  ← No "text" key
    - YouTube (new):      {"text": "...", "title": "...", ...}    ← Has "text" key

    B4 Fix: This function normalizes all formats to a single string.
    Previously, YouTube posts with no "text" key caused silent
    filter bypass — all YouTube content passed through unfiltered.
    """
    if isinstance(post, str):
        return post

    if isinstance(post, dict):
        # Primary: use unified "text" field if present (new YouTube format)
        if post.get("text"):
            return post["text"]

        # Fallback: reconstruct from title + description (old YouTube format)
        title = post.get("title", "")
        description = post.get("description", "")
        if title or description:
            return f"{title}. {description}".strip(". ")

        # Last resort: any string value in the dict
        for key in ("content", "snippet", "body", "summary"):
            if post.get(key):
                return post[key]

    return ""


def is_english(text: str) -> bool:
    """
    Returns True if text is likely English.

    Detection method: word-level frequency analysis.
    Checks what percentage of words are known non-English
    function words. If above threshold — reject.

    Thresholds:
    - >8% non-English words → reject (catches mixed content)
    - 3+ non-English words in short text (<40 words) → reject
      (catches short foreign snippets that beat the ratio)

    Returns True (assume English) for very short texts
    where ratio detection is unreliable.
    """
    if not text or len(text) < 10:
        return True

    text_lower = text.lower()

    # Immediate reject: known boilerplate phrases
    for phrase in BOILERPLATE_SIGNALS:
        if phrase in text_lower:
            return False

    # Extract clean words only (3+ letters, no numbers)
    words = re.findall(r'\b[a-z]{3,}\b', text_lower)

    if len(words) < 3:
        return True  # Too short to judge reliably

    # Count non-English signal word matches
    non_english_count = sum(1 for w in words if w in NON_ENGLISH_SIGNALS)
    non_english_ratio = non_english_count / len(words)

    # Ratio threshold — catches long foreign articles
    if non_english_ratio > 0.08:
        return False

    # Absolute count threshold — catches short foreign snippets
    if non_english_count >= 3 and len(words) < 40:
        return False

    return True


def filter_relevant(posts: list, brand: str) -> list:
    """
    Quality gate between Engine 1 raw ingestion and Engine 2 AI analysis.

    Filters posts to only those that:
    1. Contain the brand name (relevance check)
    2. Are written in English (language check)

    Accepts any post format (dict or string) via extract_text().
    Logs rejection counts for pipeline visibility.

    Args:
        posts:  Raw list from any source (NewsAPI, YouTube, GNews, HN)
        brand:  Entity name to check relevance against

    Returns:
        Filtered list in original dict/string format — structure preserved.
        Only the selection changes, not the data shape.
    """
    if not posts:
        return []

    brand_lower = brand.lower()
    filtered = []
    rejected_relevance = 0
    rejected_language = 0

    for post in posts:
        # Extract unified text regardless of source schema
        text = extract_text(post)

        if not text:
            rejected_relevance += 1
            continue

        text_lower = text.lower()

        # ── Relevance Check ───────────────────────────────────────────────────
        # Post must mention the brand to be relevant to this entity's narrative
        if brand_lower not in text_lower:
            rejected_relevance += 1
            continue

        # ── Language Check ────────────────────────────────────────────────────
        # Non-English content pollutes Engine 2 sentiment and topic analysis
        if not is_english(text):
            rejected_language += 1
            logger.info(
                f"[Filter] REJECTED non-English | "
                f"Brand: '{brand}' | "
                f"Preview: '{text[:80]}'"
            )
            continue

        filtered.append(post)

    # Always log filter summary for pipeline visibility
    logger.info(
        f"[Filter] '{brand}' | "
        f"Input: {len(posts)} | "
        f"Passed: {len(filtered)} | "
        f"Rejected relevance: {rejected_relevance} | "
        f"Rejected language: {rejected_language}"
    )

    return filtered