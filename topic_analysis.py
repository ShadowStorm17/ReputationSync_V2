from collections import Counter
import re

STOP_WORDS = {
    # English common words
    "the", "and", "for", "that", "this", "with", "from", "have", "been",
    "will", "are", "was", "were", "has", "had", "but", "not", "what",
    "all", "can", "its", "into", "over", "after", "also", "than", "then",
    "when", "more", "they", "their", "there", "about", "which", "would",
    "could", "should", "just", "said", "says", "new", "one", "two", "may",
    "now", "how", "him", "his", "her", "she", "they", "our", "your", "who",
    "did", "does", "get", "got", "out", "use", "used", "via", "per", "ago",
    "yet", "still", "even", "much", "many", "most", "other", "some", "any",
    "each", "both", "only", "very", "well", "back", "down", "here", "where",
    "report", "reports", "says", "said", "according", "amid", "while",
    "make", "take", "come", "look", "know", "think", "need", "want",
    "read", "show", "find", "give", "tell", "keep", "seem", "feel",
    "high", "long", "large", "small", "good", "great", "right", "left",
    "first", "last", "next", "like", "time", "year", "week", "month",
    "today", "million", "billion", "percent", "number", "despite",
    # Boilerplate news/web phrases
    "click", "read", "more", "information", "informationen", "subscribe",
    "newsletter", "cookie", "privacy", "policy", "terms", "rights",
    "reserved", "breaking", "latest", "update", "updated", "sign",
    "login", "register", "follow", "share", "comment", "comments",
    "posted", "published", "editor", "staff", "contact", "homepage",
    # German stop words (in case non-English slips through)
    "nicht", "auch", "dass", "sich", "werden", "eine", "einem", "einer",
    "einen", "oder", "aber", "noch", "nach", "mehr", "hier", "finden",
    "thema", "lesen", "metall", "konflikt", "gegen", "tesla", "werksleiter",
    "informationen", "ihrer", "unsere", "diese", "dieser", "dieses",
    "beim", "sein", "ihre", "wird", "haben", "sind", "wurde", "beim"
}


def extract_topics(posts):
    """
    Extracts meaningful topics from a list of text posts.
    Returns top 10 topics as list of [topic, count] pairs.
    """

    if not posts:
        return []

    word_counts = Counter()
    phrase_counts = Counter()

    for post in posts:
        # Clean the text
        text = post.lower()
        text = re.sub(r"[^a-z0-9\s]", " ", text)
        words = text.split()

        # Filter to meaningful words only
        meaningful = [
            w for w in words
            if len(w) > 3
            and w not in STOP_WORDS
            and not w.isdigit()
        ]

        # Count individual keywords
        word_counts.update(meaningful)

        # Count two-word phrases
        for i in range(len(meaningful) - 1):
            phrase = meaningful[i] + " " + meaningful[i + 1]
            phrase_counts[phrase] += 1

    # Combine: prefer phrases over single words when count is significant
    topics = Counter()

    for phrase, count in phrase_counts.items():
        if count >= 2:
            topics[phrase] = count * 2

    for word, count in word_counts.most_common(30):
        if word not in " ".join(topics.keys()):
            topics[word] = count

    # Return top 10 as clean list
    results = []
    for topic, count in topics.most_common(10):
        results.append({
            "topic": topic,
            "count": count
        })

    return results