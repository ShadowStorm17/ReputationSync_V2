def filter_relevant(posts, brand):
    brand_lower = brand.lower()

    # Common non-English words that indicate foreign language articles
    non_english_signals = [
        "que", "las", "los", "une", "les", "des", "sur", "pour",
        "mit", "und", "der", "die", "das", "ein", "ist", "wird",
        "per", "con", "del", "della", "sono", "este", "esta",
        "nous", "vous", "ils", "elle", "comme", "mais", "avec"
    ]

    filtered = []

    for p in posts:
        text = p["text"] if isinstance(p, dict) else p
        text_lower = text.lower()

        # Must mention the brand
        if brand_lower not in text_lower:
            continue

        # Skip if too many non-English words
        words = text_lower.split()
        non_english_count = sum(1 for w in words if w in non_english_signals)
        if non_english_count >= 2:
            continue

        filtered.append(p)

    return filtered