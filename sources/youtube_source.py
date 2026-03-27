import os
import requests
from urllib.parse import quote  # Import added for URL encoding

def fetch_youtube_videos(entity_name, entity_type, description, max_results=15):
    """
    Fetches YouTube videos related to the entity.
    Fixes Bug B6: Ensures multi-word entities (e.g., 'Elon Musk') are URL encoded.
    """
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("Warning: YOUTUBE_API_KEY not found.")
        return []

    # Construct the search query. 
    # We combine the entity name with context keywords if available to reduce ambiguity.
    search_query = f"{entity_name}"
    if description:
        search_query += f" {description}"
    
    # FIX B6: URL Encode the query string to handle spaces and special characters
    # 'safe' parameter ensures standard alphanumerics aren't unnecessarily encoded, 
    # but spaces become %20.
    encoded_query = quote(search_query, safe='')

    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": encoded_query,  # Use the encoded query here
        "type": "video",
        "maxResults": max_results,
        "key": api_key,
        "order": "relevance",
        "videoEmbeddable": "true"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (400, 500, etc.)
        data = response.json()
        
        videos = []
        if "items" in data:
            for item in data["items"]:
                videos.append({
                    "title": item["snippet"]["title"],
                    "channel": item["snippet"]["channelTitle"],
                    "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                    "published_at": item["snippet"]["publishedAt"],
                    "description": item["snippet"]["description"]
                })
        return videos

    except requests.exceptions.RequestException as e:
        print(f"Error fetching YouTube data for {entity_name}: {e}")
        return []