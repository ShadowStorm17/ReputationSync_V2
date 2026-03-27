import re

# Common non-English words that indicate foreign language content
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

# Boilerplate phrases that indicate non-English aggregator pages
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


def is_english(text: str) -> bool:
    """
    Returns True if text is likely English.
    Uses word-level frequency detection.
    """
    if not text or len(text) < 10:
        return True

    text_lower = text.lower()

    # Immediate reject on known boilerplate phrases
    for phrase in BOILERPLATE_SIGNALS:
        if phrase in text_lower:
            return False

    # Extract words only (no numbers, no punctuation)
    words = re.findall(r'\b[a-z]{3,}\b', text_lower)

    if len(words) < 3:
        return True

    # Count non-English signal words
    non_english_count = sum(1 for w in words if w in NON_ENGLISH_SIGNALS)
    non_english_ratio = non_english_count / len(words)

    # Reject if more than 8% non-English words
    if non_english_ratio > 0.08:
        return False

    # Reject if 3+ non-English words in short text
    if non_english_count >= 3 and len(words) < 40:
        return False

    return True


def filter_relevant(posts: list, brand: str) -> list:
    """
    Filters posts to only those:
    1. Mentioning the brand
    2. Written in English
    """
    brand_lower = brand.lower()
    filtered = []
    rejected_language = 0
    rejected_relevance = 0

    for p in posts:
        text = p["text"] if isinstance(p, dict) else p
        text_lower = text.lower()

        # Must mention the brand
        if brand_lower not in text_lower:
            rejected_relevance += 1
            continue

        # Must be English
        if not is_english(text):
            rejected_language += 1
            continue

        filtered.append(p)

    if rejected_language > 0:
        print(f"[Filter] Rejected {rejected_language} non-English "
              f"and {rejected_relevance} irrelevant posts for '{brand}'")

    return filtered