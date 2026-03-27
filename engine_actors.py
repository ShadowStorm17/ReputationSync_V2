# engine_actors.py
# ============================================================
# ReputationSync — Engine 3: Actor Intelligence
# Identifies who drives the narrative and their influence
#
# Fixes applied:
#
# Fix 1 — Retry logic:
#   If first Groq parse fails, retries once with a simpler
#   focused prompt before falling back to basic data.
#   Reduces "AI parsing failed" fallback rate significantly.
#
# Fix 2 — Better JSON cleaning:
#   More aggressive cleanup of Groq formatting quirks.
#   Handles escaped quotes, newlines in strings, truncated JSON.
#
# Fix 3 — Smarter fallback actor typing:
#   When AI fails, uses source name heuristics to correctly type actors.
#   "Hacker News" → forum (not news_outlet)
#   "YouTube —" prefix → youtube
#   "Reddit" → forum
#   Prevents Engine 5 from sending interview requests to forums.
#
# Fix 4 — Sentiment from mention text:
#   When AI fails, derives basic sentiment from TextBlob scores
#   stored in the post data rather than returning 0 for everything.
#   Gives Engine 4 and Engine 5 more accurate actor landscape.
# ============================================================

import os
import json
import re
import logging
from collections import defaultdict
from dotenv import load_dotenv
from ai_client import generate

load_dotenv()
logger = logging.getLogger(__name__)


# ── Actor Type Heuristics ─────────────────────────────────────────────────────
# Fix 3: Used in fallback when AI parse fails.
# Prevents Hacker News from being typed as "news_outlet"
# which causes Engine 5 to suggest press interviews with a forum.

FORUM_SOURCES    = {"hacker news", "reddit", "product hunt", "ycombinator"}
YOUTUBE_PREFIXES = {"youtube", "youtube —"}
SOCIAL_SOURCES   = {"twitter", "x.com", "linkedin", "instagram", "facebook"}


def infer_actor_type(source_name: str) -> str:
    """
    Infers actor type from source name when AI classification is unavailable.
    Returns: forum / youtube / social / news
    """
    name_lower = source_name.lower()

    if any(forum in name_lower for forum in FORUM_SOURCES):
        return "forum"

    if any(name_lower.startswith(yt) for yt in YOUTUBE_PREFIXES):
        return "youtube"

    if any(social in name_lower for social in SOCIAL_SOURCES):
        return "social"

    return "news"


def infer_influence(mention_count: int, actor_type: str) -> str:
    """
    Infers influence level from mention count when AI is unavailable.
    Forums get slightly higher weight due to community amplification.
    """
    if actor_type == "forum":
        if mention_count >= 10:
            return "very_high"
        elif mention_count >= 5:
            return "high"
        else:
            return "medium"
    else:
        if mention_count >= 10:
            return "high"
        elif mention_count >= 4:
            return "medium"
        else:
            return "low"


# ── Fix 2: Enhanced JSON Cleaner ──────────────────────────────────────────────

def clean_json(raw: str) -> str:
    """
    Aggressively cleans Groq JSON formatting issues.
    Handles: code blocks, trailing commas, single quotes,
             escaped quotes in strings, truncated responses.
    """
    # Remove markdown code blocks
    if "```" in raw:
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]

    raw = raw.strip()

    # Remove trailing commas before closing brackets
    raw = re.sub(r",\s*}", "}", raw)
    raw = re.sub(r",\s*]", "]", raw)

    # Fix single quotes used as JSON string delimiters
    raw = re.sub(r":\s*'([^']*)'", r': "\1"', raw)

    # Fix unescaped newlines inside strings
    raw = re.sub(r'(?<!\\)\n(?=[^"]*")', ' ', raw)

    # Remove control characters that break JSON parsing
    raw = re.sub(r'[\x00-\x1f\x7f]', ' ', raw)

    # If JSON is truncated (no closing brace) — attempt to close it
    open_braces  = raw.count('{')
    close_braces = raw.count('}')
    if open_braces > close_braces:
        raw += '}' * (open_braces - close_braces)

    open_brackets  = raw.count('[')
    close_brackets = raw.count(']')
    if open_brackets > close_brackets:
        raw += ']' * (open_brackets - close_brackets)

    return raw


# ── Fix 4: Sentiment from Post Text ──────────────────────────────────────────

def derive_sentiment_from_posts(posts: list, source_name: str) -> float:
    """
    Derives basic sentiment score for a source from its mention data.
    Uses the sentiment label already assigned by sentiment.py.
    Returns float -1.0 to +1.0.

    Used in fallback when AI parse fails — prevents returning 0
    for every actor which gives Engine 5 a flat actor landscape.
    """
    source_posts = [
        p for p in posts
        if isinstance(p, dict) and p.get("source_name") == source_name
    ]

    if not source_posts:
        return 0.0

    sentiment_map = {
        "positive": 1.0,
        "negative": -1.0,
        "neutral":  0.0
    }

    scores = []
    for post in source_posts:
        label = post.get("sentiment", "neutral")
        if isinstance(label, str):
            scores.append(sentiment_map.get(label.lower(), 0.0))

    if not scores:
        return 0.0

    avg = sum(scores) / len(scores)
    return round(avg, 2)


def infer_narrative_role(sentiment: float, actor_type: str) -> str:
    """
    Infers narrative role from derived sentiment score.
    """
    if sentiment <= -0.4:
        return "critic"
    elif sentiment >= 0.4:
        return "defender"
    elif actor_type == "forum":
        return "amplifier"
    else:
        return "neutral_reporter"


# ── Main Entry Point ──────────────────────────────────────────────────────────

def analyze_actors(entity: str, posts: list) -> dict:
    """
    Engine 3 — Identifies who drives the narrative and their influence.

    Process:
    1. Count mentions per source from post data
    2. Attempt AI analysis with full prompt (primary path)
    3. If AI parse fails, retry with simplified prompt (Fix 1)
    4. If retry fails, use smart fallback with type heuristics (Fix 3)
       and derived sentiment (Fix 4)

    Returns actor dict compatible with Engine 4 and Engine 5.
    """
    if not posts:
        return _empty_result()

    # ── Build Source Intelligence from Post Data ───────────────────────────────
    source_counts   = defaultdict(int)
    source_types    = {}
    source_posts    = defaultdict(list)

    for post in posts:
        if not isinstance(post, dict):
            continue
        name = post.get("source_name") or "Unknown"
        source_counts[name] += 1
        source_types[name] = post.get("source_type", "news")
        source_posts[name].append(post)

    if not source_counts:
        return _empty_result()

    # Factual primary driver — always highest mention count
    actual_primary       = max(source_counts, key=source_counts.get)
    actual_primary_count = source_counts[actual_primary]

    # Build actor summary lines for prompt
    actor_lines = []
    for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        actor_lines.append(
            f"{source} ({source_types.get(source, 'news')}): {count} mentions"
        )
    actor_summary = "\n".join(actor_lines)

    # Sample mentions for context
    sample = posts[:30]
    sample_lines = "\n".join(
        f"[{p.get('source_name', '?')}]: {p.get('text', '')[:120]}"
        for p in sample
        if isinstance(p, dict)
    )

    # ── Primary Prompt ────────────────────────────────────────────────────────
    prompt = _build_primary_prompt(
        entity, actual_primary, actual_primary_count,
        actor_summary, sample_lines
    )

    # ── Attempt 1: Full AI Analysis ───────────────────────────────────────────
    result = _attempt_parse(prompt, entity, attempt=1)

    if result:
        result = _attach_factual_data(result, actual_primary, actual_primary_count,
                                       source_counts, entity)
        logger.info(f"[Actors] AI parse succeeded for '{entity}' (attempt 1)")
        return result

    # ── Attempt 2: Retry with Simplified Prompt (Fix 1) ──────────────────────
    logger.warning(f"[Actors] Attempt 1 failed for '{entity}' — retrying with simplified prompt")

    simple_prompt = _build_simple_prompt(
        entity, actual_primary, actual_primary_count, actor_lines[:8]
    )
    result = _attempt_parse(simple_prompt, entity, attempt=2)

    if result:
        result = _attach_factual_data(result, actual_primary, actual_primary_count,
                                       source_counts, entity)
        logger.info(f"[Actors] AI parse succeeded for '{entity}' (attempt 2 — simplified)")
        return result

    # ── Attempt 3: Smart Fallback (Fix 3 + Fix 4) ────────────────────────────
    logger.warning(
        f"[Actors] Both AI attempts failed for '{entity}' — "
        f"using smart fallback with type heuristics and derived sentiment"
    )
    return _smart_fallback(entity, actual_primary, actual_primary_count,
                           source_counts, posts)


# ── Prompt Builders ───────────────────────────────────────────────────────────

def _build_primary_prompt(entity, primary, primary_count,
                           actor_summary, sample_lines) -> str:
    return f"""You are a reputation intelligence analyst.
Analyze who is driving the narrative about "{entity}".

SOURCES AND MENTION COUNTS (sorted by volume):
{actor_summary}

IMPORTANT:
- Primary driver MUST be "{primary}" with {primary_count} mentions
- "type" field MUST be one of: news / forum / youtube / social / blog
- Hacker News type = forum (NOT news_outlet)
- YouTube sources type = youtube
- Reddit type = forum
- "what_they_say" must describe their SPECIFIC angle, not generic coverage
- "sentiment_toward_entity" must reflect actual tone, not default to 0

SAMPLE MENTIONS:
{sample_lines}

Return ONLY valid JSON, no explanation, no markdown:
{{
  "top_actors": [
    {{
      "name": "<source name>",
      "type": "<news or forum or youtube or social or blog>",
      "mention_count": <number>,
      "sentiment_toward_entity": <float -1.0 to 1.0>,
      "influence": "<low or medium or high or very_high>",
      "narrative_role": "<critic or defender or neutral_reporter or amplifier>",
      "what_they_say": "<specific angle description — not generic>"
    }}
  ],
  "primary_driver": "<one sentence naming {primary} as primary driver>",
  "narrative_breakdown": {{
    "critics":   ["<sources consistently negative>"],
    "defenders": ["<sources consistently positive>"],
    "neutral":   ["<sources balanced>"]
  }},
  "coordination_signals": [],
  "actor_summary": "<2 sentence overview of actor landscape>"
}}"""


def _build_simple_prompt(entity, primary, primary_count, actor_lines) -> str:
    """
    Fix 1 — Simplified retry prompt.
    Shorter, stricter, less room for Groq to produce malformed JSON.
    """
    sources_text = "\n".join(actor_lines)

    return f"""Analyze actors for "{entity}". Return ONLY valid JSON.

SOURCES:
{sources_text}

Primary driver: {primary} ({primary_count} mentions)

JSON structure (return this exactly):
{{
  "top_actors": [
    {{
      "name": "<name>",
      "type": "<news or forum or youtube or social>",
      "mention_count": <number>,
      "sentiment_toward_entity": <-1.0 to 1.0>,
      "influence": "<low or medium or high or very_high>",
      "narrative_role": "<critic or defender or neutral_reporter or amplifier>",
      "what_they_say": "<their specific angle on {entity}>"
    }}
  ],
  "primary_driver": "{primary} drives the narrative with {primary_count} mentions",
  "narrative_breakdown": {{
    "critics": [],
    "defenders": [],
    "neutral": ["{primary}"]
  }},
  "coordination_signals": [],
  "actor_summary": "Narrative driven by {primary}."
}}"""


# ── Parse Attempt ─────────────────────────────────────────────────────────────

def _attempt_parse(prompt: str, entity: str, attempt: int) -> dict | None:
    """
    Calls Groq and attempts to parse JSON response.
    Returns parsed dict on success, None on failure.
    """
    try:
        raw = generate(prompt)
        raw = clean_json(raw)
        result = json.loads(raw)

        # Validate minimum required structure
        if not result.get("top_actors"):
            logger.warning(f"[Actors] Attempt {attempt} returned empty top_actors for '{entity}'")
            return None

        return result

    except json.JSONDecodeError as e:
        logger.warning(f"[Actors] JSON parse error attempt {attempt} for '{entity}': {e}")

        # Try regex extraction as last resort within this attempt
        try:
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                result = json.loads(match.group())
                if result.get("top_actors"):
                    logger.info(f"[Actors] Regex extraction succeeded for '{entity}'")
                    return result
        except Exception:
            pass

        return None

    except Exception as e:
        logger.error(f"[Actors] Unexpected error attempt {attempt} for '{entity}': {e}")
        return None


# ── Data Attachment ───────────────────────────────────────────────────────────

def _attach_factual_data(result, primary, primary_count, source_counts, entity) -> dict:
    """
    Overrides AI-generated factual fields with ground-truth data.
    Ensures primary driver always reflects actual mention count.
    """
    result["primary_driver_source"] = primary
    result["primary_driver_count"]  = primary_count
    result["entity"]                = entity
    result["total_sources"]         = len(source_counts)
    return result


# ── Smart Fallback ────────────────────────────────────────────────────────────

def _smart_fallback(
    entity: str,
    primary: str,
    primary_count: int,
    source_counts: dict,
    posts: list
) -> dict:
    """
    Fix 3 + Fix 4 — Smart fallback when all AI attempts fail.

    Fix 3: Uses source name heuristics for correct actor typing.
           Hacker News → forum, YouTube — * → youtube, etc.
           Prevents Engine 5 from sending interview requests to forums.

    Fix 4: Derives sentiment from post sentiment labels already assigned
           by sentiment.py. Returns meaningful scores instead of 0 everywhere.
    """
    top_sources = sorted(source_counts.items(), key=lambda x: -x[1])[:6]

    top_actors = []
    critics    = []
    defenders  = []
    neutral    = []

    for name, count in top_sources:
        # Fix 3: Infer type from source name
        actor_type = infer_actor_type(name)

        # Fix 4: Derive sentiment from post data
        sentiment = derive_sentiment_from_posts(posts, name)

        # Infer influence and role from derived data
        influence = infer_influence(count, actor_type)
        role      = infer_narrative_role(sentiment, actor_type)

        # Build specific what_they_say from role
        if role == "critic":
            what_they_say = f"Consistently covers {entity} with critical tone across {count} mentions"
        elif role == "defender":
            what_they_say = f"Presents positive coverage of {entity} across {count} mentions"
        elif role == "amplifier":
            what_they_say = f"Amplifies {entity} content across community with {count} mentions"
        else:
            what_they_say = f"Provides balanced coverage of {entity} across {count} mentions"

        top_actors.append({
            "name":                   name,
            "type":                   actor_type,
            "mention_count":          count,
            "sentiment_toward_entity": sentiment,
            "influence":              influence,
            "narrative_role":         role,
            "what_they_say":          what_they_say
        })

        # Populate narrative breakdown
        if role == "critic":
            critics.append(name)
        elif role == "defender":
            defenders.append(name)
        else:
            neutral.append(name)

    logger.info(
        f"[Actors] Smart fallback for '{entity}' | "
        f"Sources: {len(top_sources)} | "
        f"Critics: {len(critics)} | Defenders: {len(defenders)}"
    )

    return {
        "top_actors":       top_actors,
        "primary_driver":   f"{primary} drives the narrative about {entity} with {primary_count} mentions",
        "primary_driver_source": primary,
        "primary_driver_count":  primary_count,
        "narrative_breakdown": {
            "critics":   critics,
            "defenders": defenders,
            "neutral":   neutral
        },
        "coordination_signals": [],
        "actor_summary": (
            f"Narrative driven by {primary} ({primary_count} mentions). "
            f"Smart fallback active — type and sentiment derived from post data. "
            f"{len(critics)} critic(s), {len(defenders)} defender(s) identified."
        ),
        "entity":        entity,
        "total_sources": len(source_counts)
    }


# ── Empty Result ──────────────────────────────────────────────────────────────

def _empty_result() -> dict:
    return {
        "top_actors": [],
        "primary_driver": "Insufficient data",
        "primary_driver_source": "",
        "primary_driver_count":  0,
        "narrative_breakdown": {
            "critics":   [],
            "defenders": [],
            "neutral":   []
        },
        "coordination_signals": [],
        "actor_summary": "No actor data available.",
        "entity":        "",
        "total_sources": 0
    }