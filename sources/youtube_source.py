import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")


def get_youtube_mentions(entity):

    if not API_KEY:
        print("[YouTube] No API key found")
        return []

    url = (
        f"https://www.googleapis.com/youtube/v3/search"
        f"?part=snippet&q={entity}&type=video"
        f"&maxResults=10&key={API_KEY}"
    )

    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        if "error" in data:
            code = data["error"].get("code")
            reason = data["error"].get("errors", [{}])[0].get("reason", "unknown")
            print(f"[YouTube] API error for '{entity}': code={code} reason={reason}")
            return []

        posts = []

        if "items" in data:
            for item in data["items"]:
                title = item["snippet"].get("title", "")
                description = item["snippet"].get("description", "")
                channel = item["snippet"].get("channelTitle", "Unknown")
                video_id = item.get("id", {}).get("videoId", "")
                url_link = f"https://youtube.com/watch?v={video_id}" if video_id else ""

                text = title
                if description:
                    text = title + ". " + description

                if text.strip():
                    posts.append({
                        "text": text,
                        "source_name": channel,
                        "source_type": "youtube",
                        "url": url_link
                    })

        print(f"[YouTube] '{entity}': {len(posts)} results")
        return posts

    except Exception as e:
        print(f"[YouTube] Request failed for '{entity}': {e}")
        return []