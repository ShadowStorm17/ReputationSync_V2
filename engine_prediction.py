import os
import json
import sqlite3
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ai_client import generate

load_dotenv()


def get_risk_level(crisis_probability: int) -> str:
    """Consistent risk level based on crisis probability."""
    if crisis_probability >= 71:
        return "critical"
    elif crisis_probability >= 41:
        return "high"
    elif crisis_probability >= 16:
        return "medium"
    else:
        return "low"


def get_crisis_probability_from_score(score: int) -> int:
    """Estimates crisis probability from score alone when history is thin."""
    if score < 20:
        return 75
    elif score < 30:
        return 60
    elif score < 40:
        return 40
    elif score < 50:
        return 25
    elif score < 65:
        return 10
    else:
        return 5


def predict_trajectory(entity: str) -> dict:

    history = _get_score_history(entity, days=30)
    current_score = history[0]["score"] if history else 50

    if len(history) < 2:
        crisis_prob = get_crisis_probability_from_score(current_score)
        risk_level = get_risk_level(crisis_prob)

        if current_score < 30:
            trajectory = "critical"
        elif current_score < 45:
            trajectory = "declining"
        else:
            trajectory = "stable"

        return {
            "crisis_probability": crisis_prob,
            "trajectory": trajectory,
            "trend_summary": f"Initial reading — score is {current_score}/100. More data needed for full trend analysis.",
            "forecast_7_days": "Monitoring started — check back after a few analysis runs for trajectory forecast.",
            "risk_level": risk_level,
            "alerts": [],
            "recommendation": "Continue monitoring to build trend data.",
            "estimated_score_in_7_days": current_score,
            "score_delta": 0,
            "current_score": current_score,
            "data_points": len(history)
        }

    scores = [h["score"] for h in history]
    oldest_score = scores[-1]
    score_delta = current_score - oldest_score

    if len(scores) >= 3:
        velocity = (scores[0] - scores[2]) / 2
    else:
        velocity = score_delta

    biggest_drop = 0
    for i in range(len(scores) - 1):
        drop = scores[i + 1] - scores[i]
        if drop < biggest_drop:
            biggest_drop = drop

    history_lines = "\n".join(
        f"{h['time']}: score={h['score']}"
        for h in history
    )

    prompt = f"""You are a reputation risk analyst.
Analyze the score trend for "{entity}" and predict what happens next.

SCORE HISTORY (most recent first):
{history_lines}

KEY METRICS:
- Current score: {current_score}/100
- Score {len(history)} readings ago: {oldest_score}/100
- Total change: {score_delta:+d} points
- Velocity: {velocity:+.1f} points per reading
- Biggest single drop: {biggest_drop} points

IMPORTANT RULES:
- crisis_probability and risk_level MUST be consistent:
  0-15% = low, 16-40% = medium, 41-70% = high, 71%+ = critical
- Be realistic — do not over-inflate crisis probability
- If score is stable and high, crisis probability should be low

Return ONLY a valid JSON object, no explanation:
{{
  "crisis_probability": <number 0-100>,
  "trajectory": <"improving" or "stable" or "declining" or "volatile" or "critical">,
  "trend_summary": "<one sentence describing the trend>",
  "forecast_7_days": "<what you expect in the next 7 days>",
  "risk_level": <"low" or "medium" or "high" or "critical">,
  "alerts": [
    {{
      "type": "<sudden_drop or sustained_decline or volatility or positive_momentum>",
      "description": "<what triggered this alert>",
      "urgency": "<low or medium or high or critical>"
    }}
  ],
  "recommendation": "<one sentence on what needs to happen>",
  "estimated_score_in_7_days": <predicted score 0-100>
}}"""

    try:
        raw = generate(prompt)

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        # Clean JSON
        import re
        raw = re.sub(r",\s*}", "}", raw)
        raw = re.sub(r",\s*]", "]", raw)

        result = json.loads(raw)

        # Force consistency between crisis_probability and risk_level
        crisis_prob = result.get("crisis_probability", 0)
        result["risk_level"] = get_risk_level(crisis_prob)

        result["current_score"] = current_score
        result["score_delta"] = score_delta
        result["data_points"] = len(history)
        return result

    except json.JSONDecodeError as e:
        print(f"[Prediction] JSON parse error for '{entity}': {e}")
        return _fallback_result(current_score, score_delta)

    except Exception as e:
        print(f"[Prediction] Error for '{entity}': {e}")
        return _fallback_result(current_score, score_delta)


def _get_score_history(entity: str, days: int = 30) -> list:
    conn = sqlite3.connect("reputation.db")
    cursor = conn.cursor()

    since = (datetime.utcnow() - timedelta(days=days)).isoformat()

    cursor.execute("""
    SELECT score, created_at
    FROM reputation_history
    WHERE brand = ? AND created_at > ?
    ORDER BY created_at DESC
    LIMIT 50
    """, (entity, since))

    rows = cursor.fetchall()
    conn.close()

    return [
        {"score": r[0], "time": r[1]}
        for r in rows
    ]


def _fallback_result(current_score: int, score_delta: int) -> dict:

    if score_delta <= -10:
        trajectory = "declining"
        crisis_probability = 55
    elif score_delta >= 10:
        trajectory = "improving"
        crisis_probability = 8
    else:
        trajectory = "stable"
        crisis_probability = 15

    # Always derive risk_level from crisis_probability
    risk_level = get_risk_level(crisis_probability)

    return {
        "crisis_probability": crisis_probability,
        "trajectory": trajectory,
        "trend_summary": f"Score has changed {score_delta:+d} points recently.",
        "forecast_7_days": "Insufficient AI analysis available.",
        "risk_level": risk_level,
        "alerts": [],
        "recommendation": "Continue monitoring.",
        "estimated_score_in_7_days": current_score,
        "current_score": current_score,
        "score_delta": score_delta,
        "data_points": 0
    }