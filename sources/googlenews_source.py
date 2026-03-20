import requests
import urllib.parse
from xml.etree import ElementTree
import re


def build_queries(entity: str, entity_type: str = "brand", description: str = "") -> list:
    """
    Builds multiple search queries for balanced coverage.
    Uses description if provided for disambiguation.
    """

    base = entity

    # If description provided use it for context
    if description and description.strip():
        desc_words = description.strip().split()[:3]
        context = " ".join(desc_words)
        return [
            f"{base} {context}",
            f"{base} {context} controversy OR criticism OR problem",
            f"{base} {context} news OR announcement OR latest"
        ]

    if entity_type == "film":
        return [
            f"{base} film",
            f"{base} box office OR review OR collection",
            f"{base} controversy OR criticism OR rating"
        ]

    elif entity_type == "politician":
        return [
            f"{base}",
            f"{base} policy OR vote OR election OR government",
            f"{base} scandal OR criticism OR approval OR opposition"
        ]

    elif entity_type == "person":
        return [
            f"{base}",
            f"{base} controversy OR scandal OR criticism OR statement",
            f"{base} interview OR announcement OR news"
        ]

    elif entity_type in ("brand", "company"):
        return [
            f"{base} news",
            f"{base} controversy OR criticism OR lawsuit OR scandal OR problem",
            f"{base} stock OR revenue OR CEO OR layoffs OR earnings OR crisis"
        ]

    else:
        return [
            f"{base} news",
            f"{base} controversy OR criticism OR problem",
            f"{base} latest OR announcement"
        ]


def get_googlenews_mentions(entity: str, entity_type: str = "brand", description: str = "") -> list:

    queries = build_queries(entity, entity_type, description)
    all_posts = []
    seen_titles = set()

    for query in queries:
        posts = _fetch_rss(query)
        for post in posts:
            title = post["text"][:80]
            if title not in seen_titles:
                seen_titles.add(title)
                all_posts.append(post)

    print(f"[GoogleNews] '{entity}': {len(all_posts)} results ({len(queries)} queries)")
    return all_posts


def _fetch_rss(query: str) -> list:

    encoded = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            return []

        root = ElementTree.fromstring(response.content)
        posts = []

        for item in root.findall(".//item"):
            title = item.findtext("title", "").strip()
            description = item.findtext("description", "").strip()
            link = item.findtext("link", "").strip()
            source_tag = item.find("source")
            source_name = source_tag.text.strip() if source_tag is not None else "Google News"

            description = re.sub(r"<[^>]+>", "", description).strip()

            text = title
            if description and description != title:
                text = title + ". " + description

            if text.strip():
                posts.append({
                    "text": text,
                    "source_name": source_name,
                    "source_type": "news",
                    "url": link
                })

        return posts

    except Exception as e:
        print(f"[GoogleNews] RSS error for '{query}': {e}")
        return []