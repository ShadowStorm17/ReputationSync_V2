import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NEWS_API_KEY")

# Brands with common non-business meanings
AMBIGUOUS_BRANDS = {
    "virgin", "apple", "amazon", "oracle",
    "shell", "dove", "tide", "sprint", "robin",
    "amazon", "beast", "signal", "notion"
}


def build_query(entity: str, entity_type: str = "brand", description: str = "") -> str:
    """
    Builds the best possible search query for NewsAPI.

    Priority:
    1. If description provided — use entity + key description words
    2. If ambiguous brand — add business context
    3. If long name — shorten to first 3 words
    4. Otherwise — use entity name as is
    """

    # Priority 1 — use description if provided
    if description and description.strip():
        # Take first 4 words of description as context
        desc_words = description.strip().split()[:4]
        context = " ".join(desc_words)
        query = f"{entity} {context}"
        print(f"[NewsAPI] Query with description: '{query}'")
        return query

    # Priority 2 — handle very long names
    if len(entity) > 30:
        shortened = " ".join(entity.split()[:3])
        print(f"[NewsAPI] Query shortened: '{entity}' → '{shortened}'")
        return shortened

    # Priority 3 — disambiguate ambiguous brand names
    words = entity.lower().split()
    if entity_type == "brand" and any(w in AMBIGUOUS_BRANDS for w in words):
        query = f"{entity} company OR CEO OR business OR revenue"
        print(f"[NewsAPI] Query disambiguated: '{entity}' → '{query}'")
        return query

    # Priority 4 — entity type specific context
    if entity_type == "film":
        query = f"{entity} film OR movie OR box office OR review"
        print(f"[NewsAPI] Film query: '{query}'")
        return query

    if entity_type == "politician":
        query = f"{entity} policy OR election OR government OR vote"
        print(f"[NewsAPI] Politician query: '{query}'")
        return query

    if entity_type == "person":
        query = f"{entity}"
        return query

    return entity


def get_news_mentions(entity: str, entity_type: str = "brand", description: str = "") -> list:

    if not API_KEY:
        print("[NewsAPI] No API key found")
        return []

    query = build_query(entity, entity_type, description)

    url = (
        f"https://newsapi.org/v2/everything"
        f"?q={query}"
        f"&language=en"
        f"&sortBy=publishedAt"
        f"&apiKey={API_KEY}"
    )

    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        if data.get("status") != "ok":
            print(f"[NewsAPI] Error for '{entity}': {data.get('message', 'unknown')}")
            return []

        posts = []

        if "articles" in data:
            for article in data["articles"]:
                title = article.get("title", "")
                description_text = article.get("description", "")
                source_name = article.get("source", {}).get("name", "Unknown")
                url_link = article.get("url", "")

                if not title or title == "[Removed]":
                    continue

                text = title
                if description_text and description_text != title:
                    text = title + ". " + description_text

                if text.strip():
                    posts.append({
                        "text": text,
                        "source_name": source_name,
                        "source_type": "news",
                        "url": url_link
                    })

        print(f"[NewsAPI] '{entity}': {len(posts)} results")
        return posts

    except Exception as e:
        print(f"[NewsAPI] Request failed for '{entity}': {e}")
        return []