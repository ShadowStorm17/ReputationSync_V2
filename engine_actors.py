import os
import json
import re
from collections import defaultdict
from dotenv import load_dotenv
from ai_client import generate

load_dotenv()


def clean_json(raw: str) -> str:
    """
    Cleans common JSON formatting issues from Groq responses.
    """
    # Remove code block markers
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    # Remove trailing commas before closing brackets
    raw = re.sub(r",\s*}", "}", raw)
    raw = re.sub(r",\s*]", "]", raw)

    # Fix single quotes used instead of double quotes
    # Only replace when clearly used as JSON string delimiters
    # This is conservative to avoid breaking content with apostrophes
    raw = re.sub(r":\s*'([^']*)'", r': "\1"', raw)

    return raw


def analyze_actors(entity: str, posts: list) -> dict:

    if not posts:
        return _empty_result()

    source_counts = defaultdict(int)
    source_types = {}

    for post in posts:
        if not isinstance(post, dict):
            continue
        name = post.get("source_name") or "Unknown"
        source_type = post.get("source_type", "news")
        source_counts[name] += 1
        source_types[name] = source_type

    if not source_counts:
        return _empty_result()

    # Find actual primary driver by mention count
    actual_primary = max(source_counts, key=source_counts.get)
    actual_primary_count = source_counts[actual_primary]

    actor_lines = []
    for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        actor_lines.append(
            f"{source} ({source_types.get(source, 'news')}): {count} mentions"
        )

    actor_summary = "\n".join(actor_lines)

    sample = posts[:30]
    sample_lines = "\n".join(
        f"[{p.get('source_name', '?')}]: {p.get('text', '')[:120]}"
        for p in sample
        if isinstance(p, dict)
    )

    prompt = f"""You are a reputation intelligence analyst.
Analyze who is driving the narrative about "{entity}".

SOURCES AND MENTION COUNTS (sorted by volume):
{actor_summary}

IMPORTANT:
- The source with the most mentions is "{actual_primary}" with {actual_primary_count} mentions
- Primary driver MUST be the source with highest mention count
- "what_they_say" must describe their SPECIFIC angle and stance
  Do NOT use generic phrases like "providing coverage" or "reporting on developments"
  Instead describe exactly what angle they take and why

SAMPLE MENTIONS WITH SOURCES:
{sample_lines}

Return ONLY a valid JSON object with exactly this structure, no explanation:
{{
  "top_actors": [
    {{
      "name": "<source name>",
      "type": "<news_outlet or youtube_channel or blog or forum or social>",
      "mention_count": <number>,
      "sentiment_toward_entity": <number -1 to 1>,
      "influence": "<low or medium or high or very_high>",
      "narrative_role": "<critic or defender or neutral_reporter or amplifier>",
      "what_they_say": "<specific description of their angle — not generic>"
    }}
  ],
  "primary_driver": "<one sentence starting with {actual_primary} which has the most mentions>",
  "narrative_breakdown": {{
    "critics": ["<source names consistently negative>"],
    "defenders": ["<source names consistently positive>"],
    "neutral": ["<source names balanced>"]
  }},
  "coordination_signals": ["<signs of coordinated messaging or empty list>"],
  "actor_summary": "<2 sentence overview of actor landscape>"
}}"""

    try:
        raw = generate(prompt)
        raw = clean_json(raw)

        result = json.loads(raw)

        # Override with factual data
        result["primary_driver_source"] = actual_primary
        result["primary_driver_count"] = actual_primary_count
        result["entity"] = entity
        result["total_sources"] = len(source_counts)
        return result

    except json.JSONDecodeError as e:
        print(f"[Actors] JSON parse error for '{entity}': {e}")
        print(f"[Actors] Attempting fallback parse...")

        # Fallback — try to extract what we can
        try:
            # Find the JSON object in the response
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                result = json.loads(match.group())
                result["primary_driver_source"] = actual_primary
                result["primary_driver_count"] = actual_primary_count
                result["entity"] = entity
                result["total_sources"] = len(source_counts)
                print(f"[Actors] Fallback parse succeeded for '{entity}'")
                return result
        except Exception:
            pass

        # If all parsing fails return structured fallback
        return _fallback_with_data(
            entity, actual_primary, actual_primary_count, source_counts
        )

    except Exception as e:
        print(f"[Actors] Error for '{entity}': {e}")
        return _empty_result()


def _fallback_with_data(
    entity: str,
    primary: str,
    primary_count: int,
    source_counts: dict
) -> dict:
    """
    Returns a basic actor result using raw data
    when AI parsing fails completely.
    """
    top_sources = sorted(source_counts.items(), key=lambda x: -x[1])[:5]

    top_actors = [
        {
            "name": name,
            "type": "news_outlet",
            "mention_count": count,
            "sentiment_toward_entity": 0,
            "influence": "medium",
            "narrative_role": "neutral_reporter",
            "what_they_say": f"Covers {entity} with {count} mentions"
        }
        for name, count in top_sources
    ]

    return {
        "top_actors": top_actors,
        "primary_driver": f"{primary} is driving the narrative with {primary_count} mentions",
        "primary_driver_source": primary,
        "primary_driver_count": primary_count,
        "narrative_breakdown": {
            "critics": [],
            "defenders": [],
            "neutral": [name for name, _ in top_sources]
        },
        "coordination_signals": [],
        "actor_summary": f"Narrative driven by {primary} ({primary_count} mentions). AI parsing failed — basic data shown.",
        "entity": entity,
        "total_sources": len(source_counts)
    }


def _empty_result() -> dict:
    return {
        "top_actors": [],
        "primary_driver": "Insufficient data",
        "primary_driver_source": "",
        "primary_driver_count": 0,
        "narrative_breakdown": {
            "critics": [],
            "defenders": [],
            "neutral": []
        },
        "coordination_signals": [],
        "actor_summary": "No actor data available."
    }