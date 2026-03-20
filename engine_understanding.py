import os
import json
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ai_client import generate

load_dotenv()

ENTITY_INSTRUCTIONS = {
    "brand": """
Focus on:
- Product and service sentiment
- Corporate reputation and controversies
- Financial performance coverage
- Customer and employee sentiment
- Regulatory and legal issues
- Competitive positioning
""",
    "company": """
Focus on:
- Business performance and financials
- Leadership and corporate governance
- Employee and culture coverage
- Industry positioning
- Legal and regulatory issues
- Innovation and product coverage
""",
    "person": """
Focus on:
- Personal brand and public image
- Professional achievements and failures
- Controversies and personal conduct
- Media portrayal and narrative
- Public statements and their reception
- Industry influence and credibility
""",
    "politician": """
Focus on:
- Policy positions and voting record coverage
- Public approval and disapproval
- Scandal and controversy coverage
- Party and coalition dynamics
- Electoral and campaign coverage
- Government performance narratives
""",
    "celebrity": """
Focus on:
- Public image and fan sentiment
- Professional work reception
- Personal life and controversy coverage
- Brand deals and endorsements
- Social media narrative
- Media portrayal
""",
    "film": """
Focus on:
- Box office performance and collections
- Critical and audience reviews
- Cast and director reception
- Controversy around themes or content
- Comparison to other films
- Award season coverage
- Streaming and distribution news
""",
    "founder": """
Focus on:
- Company performance tied to founder
- Leadership style and decisions coverage
- Innovation and vision narrative
- Controversies and personal conduct
- Investor and employee sentiment
- Industry influence
"""
}


def get_recent_scores(entity: str, limit: int = 3) -> list:
    """Gets the most recent scores for smoothing."""
    try:
        conn = sqlite3.connect("reputation.db")
        cursor = conn.cursor()
        cursor.execute("""
        SELECT score FROM reputation_history
        WHERE brand = ?
        ORDER BY created_at DESC
        LIMIT ?
        """, (entity, limit))
        rows = cursor.fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def is_genuine_crisis(result: dict) -> bool:
    """
    Detects if this is a genuine crisis that should
    bypass score smoothing cap.

    Returns True if:
    - Narrative type is crisis or scandal
    - AND there are specific crisis indicators
    - AND raw score dropped significantly
    """
    narrative_type = result.get("narrative", {}).get("narrative_type", "neutral")
    crisis_indicators = result.get("signals", {}).get("crisis_indicators", [])
    raw_score = result.get("sentiment", {}).get("score", 50)

    is_crisis_narrative = narrative_type in ("crisis", "scandal")
    has_specific_indicators = len(crisis_indicators) >= 2
    is_low_score = raw_score < 30

    return is_crisis_narrative and has_specific_indicators and is_low_score


def smooth_score(new_score: int, recent_scores: list, bypass_cap: bool = False) -> int:
    """
    Blends new score with recent history to prevent wild swings.

    Normal mode:
    - Weights: new 50%, last 30%, before 20%
    - Cap: maximum 15 point change per run

    Crisis mode (bypass_cap=True):
    - Still blends for smoothness
    - Removes the 15 point cap
    - Allows full crisis drop to show through
    """

    if not recent_scores:
        return new_score

    if len(recent_scores) == 1:
        smoothed = round((new_score * 0.6) + (recent_scores[0] * 0.4))
    else:
        smoothed = round(
            new_score * 0.5 +
            recent_scores[0] * 0.3 +
            recent_scores[1] * 0.2
        )

    # Apply cap only in normal mode
    if not bypass_cap and recent_scores:
        max_change = 15
        if smoothed < recent_scores[0] - max_change:
            smoothed = recent_scores[0] - max_change
            print(f"[Smoothing] Drop capped at {max_change} points → {smoothed}")
        elif smoothed > recent_scores[0] + max_change:
            smoothed = recent_scores[0] + max_change
            print(f"[Smoothing] Rise capped at {max_change} points → {smoothed}")

    return max(0, min(100, smoothed))


def get_label(score: int) -> str:
    """Returns sentiment label for a given score."""
    if score >= 80:
        return "very_positive"
    elif score >= 65:
        return "positive"
    elif score >= 45:
        return "neutral"
    elif score >= 25:
        return "negative"
    else:
        return "very_negative"


def analyze_with_ai(entity: str, posts: list, entity_type: str = "brand") -> dict:

    if not posts:
        return _empty_result(entity)

    # Normalize entity type to lowercase
    entity_type = entity_type.lower().strip()

    # Map common variations to our defined types
    type_map = {
        "movie": "film",
        "movies": "film",
        "films": "film",
        "brand": "brand",
        "company": "company",
        "person": "person",
        "people": "person",
        "politician": "politician",
        "politics": "politician",
        "celebrity": "celebrity",
        "celeb": "celebrity",
        "founder": "founder",
        "startup": "brand"
    }
    entity_type = type_map.get(entity_type, entity_type)

    sample = posts[:50]
    joined = "\n".join(f"- {p}" for p in sample)

    instructions = ENTITY_INSTRUCTIONS.get(
        entity_type,
        ENTITY_INSTRUCTIONS["brand"]
    )

    prompt = f"""You are a reputation intelligence analyst. Your job is to give an accurate, balanced and critical assessment.

Analyze these mentions of "{entity}" (entity type: {entity_type}).

ANALYSIS FOCUS FOR THIS ENTITY TYPE:
{instructions}

IMPORTANT RULES:
- Be critical and balanced. Do not inflate positive sentiment.
- If mentions contain ANY negative coverage include it in negative_count.
- Negative count must reflect ALL critical, controversial or unfavorable mentions.
- Do not round up to positive. If sentiment is mixed say so honestly.
- Crisis indicators must be specific — quote actual issues found in the mentions.
- Topics must be specific to this entity type — not generic phrases.
- "what_they_say" for each actor must describe their SPECIFIC angle,
  not generic phrases like "providing coverage".

MENTIONS:
{joined}

Return ONLY a valid JSON object with exactly this structure, no explanation:
{{
  "sentiment": {{
    "score": <number 0-100, where 0=very negative, 50=neutral, 100=very positive>,
    "label": <"very_negative" or "negative" or "neutral" or "positive" or "very_positive">,
    "positive_count": <number of clearly positive mentions>,
    "negative_count": <number of critical, negative or controversial mentions>,
    "neutral_count": <number of factual balanced mentions>,
    "reason": "<one honest sentence explaining the overall sentiment>"
  }},
  "topics": [
    {{
      "topic": "<specific topic name relevant to {entity_type}>",
      "sentiment": <number -1 to 1>,
      "count": <number of mentions about this topic>
    }}
  ],
  "narrative": {{
    "current_story": "<1-2 sentences describing the dominant narrative forming>",
    "narrative_type": <"neutral" or "positive" or "negative" or "controversy" or "crisis" or "growth" or "scandal">,
    "momentum": <"improving" or "stable" or "declining">
  }},
  "signals": {{
    "crisis_indicators": ["<specific warning sign pulled from actual mentions>"],
    "positive_signals": ["<specific positive development from actual mentions>"]
  }},
  "summary": "<2-3 sentence honest executive summary — include both positives and negatives>"
}}"""

    try:
        raw = generate(prompt)

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)

        # Get raw AI score
        raw_score = result["sentiment"]["score"]

        # Detect genuine crisis before smoothing
        crisis_detected = is_genuine_crisis({
            "narrative": result.get("narrative", {}),
            "signals": result.get("signals", {}),
            "sentiment": {"score": raw_score}
        })

        if crisis_detected:
            print(f"[Smoothing] CRISIS detected for '{entity}' — bypassing cap")

        # Apply score smoothing
        recent_scores = get_recent_scores(entity)
        smoothed = smooth_score(raw_score, recent_scores, bypass_cap=crisis_detected)

        if smoothed != raw_score:
            print(f"[Smoothing] {entity}: raw={raw_score} → smoothed={smoothed}")

        # Update score and label
        result["sentiment"]["score"] = smoothed
        result["sentiment"]["raw_score"] = raw_score
        result["sentiment"]["label"] = get_label(smoothed)
        result["sentiment"]["crisis_bypass"] = crisis_detected

        result["entity"] = entity
        result["entity_type"] = entity_type
        result["mention_count"] = len(posts)
        return result

    except json.JSONDecodeError as e:
        print(f"[Understanding] JSON parse error for '{entity}': {e}")
        return _empty_result(entity)

    except Exception as e:
        print(f"[Understanding] Error for '{entity}': {e}")
        return _empty_result(entity)


def _empty_result(entity: str) -> dict:
    return {
        "entity": entity,
        "sentiment": {
            "score": 50,
            "label": "neutral",
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "reason": "No mentions found"
        },
        "topics": [],
        "narrative": {
            "current_story": "No mentions found.",
            "narrative_type": "neutral",
            "momentum": "stable"
        },
        "signals": {
            "crisis_indicators": [],
            "positive_signals": []
        },
        "summary": "No mentions found for this entity.",
        "mention_count": 0
    }