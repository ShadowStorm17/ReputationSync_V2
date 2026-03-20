import requests


def get_hackernews_mentions(entity):
    """
    Fetches mentions from Hacker News search API.
    Completely free, no API key needed.
    Best for tech brands: Apple, Tesla, Nvidia, OpenAI, etc.
    """

    try:
        url = f"https://hn.algolia.com/api/v1/search?query={entity}&tags=story&hitsPerPage=20"
        response = requests.get(url, timeout=10)
        data = response.json()

        posts = []

        for hit in data.get("hits", []):
            title = hit.get("title", "").strip()
            url_link = hit.get("url", "")
            points = hit.get("points", 0)
            comments = hit.get("num_comments", 0)

            if not title:
                continue

            # Add engagement context to text for better sentiment analysis
            text = title
            if points > 100:
                text += f". (High engagement: {points} points, {comments} comments)"

            posts.append({
                "text": text,
                "source_name": "Hacker News",
                "source_type": "forum",
                "url": url_link,
                "points": points
            })

        print(f"[HackerNews] '{entity}': {len(posts)} results")
        return posts

    except Exception as e:
        print(f"[HackerNews] Error for '{entity}': {e}")
        return []