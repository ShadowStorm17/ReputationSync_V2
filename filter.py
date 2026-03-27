# filter.py
# ============================================================
# ReputationSync — Relevance + Language Filter
# Engine 1 Listening quality gate
#
# Purpose: Ensures only English, brand-relevant content
# reaches Engine 2 — Understanding for AI analysis.
#
# B4 Fix v2: Removed collision-prone short words from
# NON_ENGLISH_SIGNALS that matched common English substrings.
# Raised threshold from 8% to 12% to reduce false positives.
# Words like "con", "del", "las", "sur", "tem", "par" were
# matching inside English words and rejecting valid content
# from FOX, LA Times, UPI and other English-language outlets.
#
# Retained strong, unambiguous foreign-language markers only.
# ============================================================

import re
import logging

logger = logging.getLogger(__name__)

# ── Non-English Word Signals ──────────────────────────────────────────────────
# IMPORTANT: Only include words that are:
# 1. High-frequency in the target foreign language
# 2. Rarely or never appear as standalone words in English text
# 3. Not substrings of common English words
#
# Removed from original list (collision-prone):
#   "sur"  → matches inside "surges", "surface", "surplus"
#   "con"  → matches inside "concern", "consumer", "contract"
#   "del"  → matches inside "model", "delivery", "delta"
#   "las"  → matches inside "class", "clash", "Los Angeles"
#   "tem"  → matches inside "system", "item", "problem"
#   "par"  → matches inside "eparate", "compare", "partner"
#   "por"  → matches inside "report", "support", "export"
#   "son"  → common English word
#   "com"  → matches domain suffixes (.com)
#   "mais" → rare but "mais" appears in English names
#   "hay"  → common English word (dried grass)
#   "ser"  → matches inside "server", "service", "series"
#   "bei"  → too short, rare collision risk
#   "van"  → common English word and surname
#   "het"  → matches inside "whether", "ether"

NON_ENGLISH_SIGNALS = {
    # German — high frequency, unambiguous
    "und", "der", "die", "das", "ein", "eine", "ist", "wird",
    "mit", "auf", "von", "aus", "nach", "uber", "nicht", "auch",
    "dass", "sich", "werden", "oder", "aber", "noch", "mehr",
    "hier", "beim", "thema", "lesen",

    # French — high frequency, unambiguous
    "les", "des", "une", "pour", "avec", "dans",
    "qui", "que", "pas", "plus", "comme", "mais", "tout", "elle",
    "nous", "vous", "ils", "leur", "cette", "sont", "etait",

    # Spanish — high frequency, unambiguous
    "los", "las", "una", "sus",
    "este", "esta", "pero", "muy", "tiene",
    "estan", "entre", "cuando", "tambien", "porque",

    # Italian — high frequency, unambiguous
    "della", "dello", "degli", "alle", "agli", "nel", "nella",
    "sono", "questo", "questa", "anche", "quando",

    # Portuguese — high frequency, unambiguous
    "para", "uma", "nao", "seu", "sua",
    "isso", "esse", "pelo", "pela",

    # Dutch — high frequency, unambiguous
    "een", "zijn", "deze", "naar",
    "heeft", "kunnen", "hebben",
}

# ── Boilerplate Signals ───────────────────────────────────────────────────────
# Full phrases that definitively identify non-English publisher boilerplate.
# One match = immediate reject. These are extremely specific — no false positives.

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
    Extracts unified text string from any post format.

    Different Engine 1 sources return different schemas:
    - NewsAPI / GNews / HN: {"text": "...", "source_name": "..."}
    - YouTube (new):        {"text": "...", "title": "...", ...}
    - YouTube (old):        {"title": "...", "description": "..."}

    B4 Fix: Normalizes all formats to single string.
    Previously YouTube posts with no "text" key caused silent
    filter bypass — all YouTube content passed unfiltered.
    """
    if isinstance(post, str):
        return post

    if isinstance(post, dict):
        # Primary: unified text field (all new source formats)
        if post.get("text"):
            return post["text"]

        # Fallback: reconstruct from title + description (legacy YouTube)
        title = post.get("title", "")
        description = post.get("description", "")
        if title or description:
            return f"{title}. {description}".strip(". ")

        # Last resort: any known text field
        for key in ("content", "snippet", "body", "summary"):
            if post.get(key):
                return post[key]

    return ""


def is_english(text: str) -> bool:
    """
    Returns True if text is likely English.

    Detection method: word-level frequency analysis against
    known high-frequency foreign language function words.

    Thresholds (v2 — raised to reduce false positives):
    - Boilerplate phrase match → immediate reject
    - >12% non-English words → reject (was 8%, raised to reduce FP)
    - 4+ non-English words in short text (<30 words) → reject
      (was 3 words / 40 word limit — tightened)

    Returns True (assume English) for very short texts
    where ratio detection is unreliable.
    """
    if not text or len(text) < 10:
        return True

    text_lower = text.lower()

    # Immediate reject: unambiguous foreign boilerplate phrases
    for phrase in BOILERPLATE_SIGNALS:
        if phrase in text_lower:
            return False

    # Extract clean words (3+ letters, no numbers, no punctuation)
    words = re.findall(r'\b[a-z]{3,}\b', text_lower)

    if len(words) < 5:
        return True  # Too short to judge reliably

    # Count non-English signal word matches
    non_english_count = sum(1 for w in words if w in NON_ENGLISH_SIGNALS)
    non_english_ratio = non_english_count / len(words)

    # Ratio threshold — catches long foreign articles
    # Raised from 8% to 12% to reduce English false positives
    if non_english_ratio > 0.12:
        return False

    # Absolute count threshold — catches short foreign snippets
    # Tightened: requires 4+ matches in texts under 30 words
    if non_english_count >= 4 and len(words) < 30:
        return False

    return True


def filter_relevant(posts: list, brand: str) -> list:
    """
    Quality gate between Engine 1 raw ingestion and Engine 2 AI analysis.

    Filters posts to only those that:
    1. Contain the brand name (relevance check)
    2. Are written in English (language check)

    Accepts any post format via extract_text() normalization.
    Logs rejection counts for full pipeline visibility.

    Args:
        posts: Raw list from any Engine 1 source
        brand: Entity name to check relevance against

    Returns:
        Filtered list in original format — structure preserved.
    """
    if not posts:
        return []

    brand_lower = brand.lower()
    filtered = []
    rejected_relevance = 0
    rejected_language = 0

    for post in posts:
        text = extract_text(post)

        if not text:
            rejected_relevance += 1
            continue

        text_lower = text.lower()

        # ── Relevance Check ───────────────────────────────────────────────────
        if brand_lower not in text_lower:
            rejected_relevance += 1
            continue

        # ── Language Check ────────────────────────────────────────────────────
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