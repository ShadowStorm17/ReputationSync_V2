# engine_prediction.py
# ============================================================
# ReputationSync — Engine 4: Prediction
# Crisis probability, trajectory, alerts, 7-day forecast
#
# B5 FIX: Smarter first-reading prediction.
# Previously returned "check back later" with no alerts
# when only 1 data point existed.
#
# Now uses Engine 2 (narrative type, momentum, crisis indicators,
# positive signals) and Engine 3 (defenders, critics, actor count)
# to generate a meaningful, alert-rich first reading immediately.
#
# Function signature updated:
#   predict_trajectory(entity, ai_result=None, actor_result=None)
#
# Backward compatible — all existing calls without ai_result/
# actor_result still work exactly as before.
# ============================================================

import os
import json
import sqlite3
import re
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from ai_client import generate

load_dotenv()


# ── Risk Level ────────────────────────────────────────────────────────────────

def get_risk_level(crisis_probability: int) -> str:
    """
    Consistent risk level derived from crisis probability.
    Single source of truth — never set risk_level manually.
    0-15% = low, 16-40% = medium, 41-70% = high, 71%+ = critical
    """
    if crisis_probability >= 71:
        return "critical"
    elif crisis_probability >= 41:
        return "high"
    elif crisis_probability >= 16:
        return "medium"
    else:
        return "low"


def get_crisis_probability_from_score(score: int) -> int:
    """
    Estimates crisis probability from score alone.
    Used as baseline — B5 adds narrative/actor adjustments on top.
    """
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


# ── Main Entry Point ──────────────────────────────────────────────────────────

def predict_trajectory(
    entity: str,
    ai_result: dict = None,
    actor_result: dict = None
) -> dict:
    """
    Generates crisis probability, trajectory, alerts and 7-day forecast.

    B5 FIX: Now accepts optional ai_result and actor_result.
    When provided on first reading (data_points <= 1), uses Engine 2
    narrative signals and Engine 3 actor landscape for prediction
    instead of returning "check back later".

    When data_points >= 2, uses existing Groq AI trend analysis
    (unchanged from original — best path for multi-point history).

    Args:
        entity:       Entity name
        ai_result:    Engine 2 output — optional (narrative, signals, sentiment)
        actor_result: Engine 3 output — optional (actors, defenders, critics)

    Returns:
        Full prediction dict with crisis_probability, risk_level,
        trajectory, alerts, forecast_7_days, trend_summary.
    """
    history = _get_score_history(entity, days=30)
    current_score = history[0]["score"] if history else 50

    # ── B5: First Reading Path ────────────────────────────────────────────────
    # Use Engine 2 + Engine 3 intelligence when history is thin
    if len(history) < 2:
        if ai_result and actor_result:
            # Rich first reading — full signal intelligence available
            return _predict_first_reading_smart(
                entity, current_score, ai_result, actor_result
            )
        else:
            # Thin first reading — score-only fallback (legacy behavior)
            return _predict_first_reading_score_only(entity, current_score, history)

    # ── Multi-Datapoint Path: Groq AI Trend Analysis ──────────────────────────
    # Original logic preserved — Groq generates rich trend prediction
    return _predict_from_history(entity, history, current_score)


# ── B5: Smart First Reading ───────────────────────────────────────────────────

def _predict_first_reading_smart(
    entity: str,
    current_score: int,
    ai_result: dict,
    actor_result: dict
) -> dict:
    """
    B5 — Smart first reading using Engine 2 and Engine 3 intelligence.

    Crisis probability formula:
    Base (from score) + Narrative type bonus + Indicator bonus
    + Momentum adjustment + Defender penalty

    All components are clamped to 0-100 final range.
    """

    # ── Extract Engine 2 Signals ──────────────────────────────────────────────
    narrative     = ai_result.get("narrative", {})
    signals       = ai_result.get("signals", {})
    sentiment     = ai_result.get("sentiment", {})

    narrative_type     = narrative.get("narrative_type", "neutral").lower()
    momentum           = narrative.get("momentum", "stable").lower()
    current_story      = narrative.get("current_story", "")
    crisis_indicators  = signals.get("crisis_indicators", [])
    positive_signals   = signals.get("positive_signals", [])

    # ── Extract Engine 3 Actor Landscape ─────────────────────────────────────
    narrative_breakdown = actor_result.get("narrative_breakdown", {})
    defenders           = narrative_breakdown.get("defenders", [])
    critics             = narrative_breakdown.get("critics", [])
    top_actors          = actor_result.get("top_actors", [])

    # ── Crisis Probability Calculation ────────────────────────────────────────

    # 1. Base probability from score
    base = get_crisis_probability_from_score(current_score)

    # 2. Narrative type adjustment
    # Crisis/scandal = high base. Growth/positive = reduces probability.
    narrative_bonus = {
        "crisis":        25,
        "scandal":       30,
        "controversy":   15,
        "neutral":        0,
        "growth":       -15,
        "positive":     -20,
    }.get(narrative_type, 0)

    # 3. Crisis indicator bonus (each confirmed indicator = +5%, max +20%)
    indicator_bonus = min(len(crisis_indicators) * 5, 20)

    # 4. Positive signal reduction (each positive signal = -3%, max -10%)
    positive_reduction = min(len(positive_signals) * 3, 10)

    # 5. Momentum adjustment
    momentum_adjustment = {
        "declining":  12,
        "stable":      0,
        "improving": -12,
    }.get(momentum, 0)

    # 6. Actor landscape adjustment
    # No defenders = narrative unchallenged = higher risk
    # Presence of critics from multiple outlets = coordinated pressure
    defender_penalty = 0 if len(defenders) > 0 else 8
    critic_bonus = min(len(critics) * 3, 12)

    # 7. Final calculation
    crisis_probability = (
        base
        + narrative_bonus
        + indicator_bonus
        - positive_reduction
        + momentum_adjustment
        + defender_penalty
        + critic_bonus
    )
    crisis_probability = max(0, min(100, crisis_probability))

    # ── Trajectory ────────────────────────────────────────────────────────────
    # First reading trajectory = current momentum from Engine 2
    trajectory_map = {
        "declining":  "declining",
        "stable":     "stable",
        "improving":  "improving",
    }
    trajectory = trajectory_map.get(momentum, "stable")

    # Override to critical if crisis probability is very high
    if crisis_probability >= 71:
        trajectory = "critical"

    # ── Alerts ────────────────────────────────────────────────────────────────
    alerts = []

    # Alert: Active crisis or scandal narrative
    if narrative_type in ["crisis", "scandal"]:
        alerts.append({
            "type": "crisis_narrative_active",
            "description": (
                f"{narrative_type.capitalize()} narrative confirmed with "
                f"{momentum} momentum. {current_story[:100]}"
            ),
            "urgency": "critical" if crisis_probability >= 71 else "high"
        })

    # Alert: Multiple crisis indicators confirmed
    if len(crisis_indicators) >= 2:
        alerts.append({
            "type": "multiple_crisis_indicators",
            "description": (
                f"{len(crisis_indicators)} crisis indicators active: "
                f"{'; '.join(crisis_indicators[:2])}"
            ),
            "urgency": "high"
        })

    # Alert: No defenders in Actor Authority landscape
    if len(defenders) == 0 and len(critics) > 0:
        alerts.append({
            "type": "no_defenders_identified",
            "description": (
                f"No defending actors identified. "
                f"{len(critics)} critics ({', '.join(critics[:3])}) "
                f"driving narrative unchallenged."
            ),
            "urgency": "medium"
        })

    # Alert: Low reputation score threshold
    if current_score < 30:
        alerts.append({
            "type": "critical_score",
            "description": (
                f"Reputation score {current_score}/100 is in critical range. "
                f"Immediate narrative intervention required."
            ),
            "urgency": "critical"
        })
    elif current_score < 45:
        alerts.append({
            "type": "low_score",
            "description": (
                f"Reputation score {current_score}/100 is below healthy threshold."
            ),
            "urgency": "medium"
        })

    # ── 7-Day Forecast ────────────────────────────────────────────────────────
    forecast = _generate_first_reading_forecast(
        narrative_type, momentum, crisis_indicators,
        critics, defenders, current_score
    )

    # ── Trend Summary ─────────────────────────────────────────────────────────
    defender_text = "No defenders identified." if not defenders else f"{len(defenders)} defender(s) present."
    indicator_text = f"{len(crisis_indicators)} crisis indicator(s) confirmed." if crisis_indicators else "No crisis indicators."

    trend_summary = (
        f"First reading — score {current_score}/100. "
        f"{narrative_type.capitalize()} narrative, {momentum} momentum. "
        f"{indicator_text} {defender_text}"
    )

    # ── Recommendation ────────────────────────────────────────────────────────
    if crisis_probability >= 71:
        recommendation = "Immediate crisis response required — activate Engine 5 playbook now."
    elif crisis_probability >= 41:
        recommendation = "High risk detected — review action playbook and engage key Actor Authority targets."
    elif crisis_probability >= 16:
        recommendation = "Monitor closely — narrative trajectory may worsen without intervention."
    else:
        recommendation = "Continue monitoring — narrative is stable."

    return {
        "crisis_probability":       crisis_probability,
        "trajectory":               trajectory,
        "trend_summary":            trend_summary,
        "forecast_7_days":          forecast,
        "risk_level":               get_risk_level(crisis_probability),
        "alerts":                   alerts,
        "recommendation":           recommendation,
        "estimated_score_in_7_days": _estimate_future_score(current_score, momentum),
        "score_delta":              0,
        "current_score":            current_score,
        "data_points":              1
    }


def _predict_first_reading_score_only(
    entity: str,
    current_score: int,
    history: list
) -> dict:
    """
    Thin first reading — no Engine 2/3 data available.
    Used when predict_trajectory() is called without ai_result/actor_result.
    Better than original but still limited to score-based signals.
    """
    crisis_prob = get_crisis_probability_from_score(current_score)
    risk_level  = get_risk_level(crisis_prob)

    if current_score < 30:
        trajectory = "critical"
    elif current_score < 45:
        trajectory = "declining"
    else:
        trajectory = "stable"

    alerts = []
    if current_score < 30:
        alerts.append({
            "type": "critical_score",
            "description": f"Score {current_score}/100 is critically low on first reading.",
            "urgency": "critical"
        })
    elif current_score < 45:
        alerts.append({
            "type": "low_score",
            "description": f"Score {current_score}/100 is below healthy threshold.",
            "urgency": "medium"
        })

    return {
        "crisis_probability":        crisis_prob,
        "trajectory":                trajectory,
        "trend_summary":             f"First reading — score {current_score}/100. Building trend data.",
        "forecast_7_days":           "Insufficient trend data. Run analysis again in 2 hours for trajectory forecast.",
        "risk_level":                risk_level,
        "alerts":                    alerts,
        "recommendation":            "Continue monitoring to build trend data.",
        "estimated_score_in_7_days": current_score,
        "score_delta":               0,
        "current_score":             current_score,
        "data_points":               len(history)
    }


# ── Multi-Datapoint: Groq AI Trend Analysis ───────────────────────────────────

def _predict_from_history(entity: str, history: list, current_score: int) -> dict:
    """
    Original Groq AI prediction logic — unchanged.
    Called when data_points >= 2. Best path for established entities.
    """
    scores       = [h["score"] for h in history]
    oldest_score = scores[-1]
    score_delta  = current_score - oldest_score

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

        raw = re.sub(r",\s*}", "}", raw)
        raw = re.sub(r",\s*]", "]", raw)

        result = json.loads(raw)

        # Force consistency between crisis_probability and risk_level
        crisis_prob      = result.get("crisis_probability", 0)
        result["risk_level"]               = get_risk_level(crisis_prob)
        result["current_score"]            = current_score
        result["score_delta"]              = score_delta
        result["data_points"]              = len(history)
        return result

    except json.JSONDecodeError as e:
        print(f"[Prediction] JSON parse error for '{entity}': {e}")
        return _fallback_result(current_score, score_delta, len(history))

    except Exception as e:
        print(f"[Prediction] Error for '{entity}': {e}")
        return _fallback_result(current_score, score_delta, len(history))


# ── Helper Functions ──────────────────────────────────────────────────────────

def _generate_first_reading_forecast(
    narrative_type: str,
    momentum: str,
    indicators: list,
    critics: list,
    defenders: list,
    score: int
) -> str:
    """
    Generates specific 7-day forecast text for first reading.
    Based on narrative type, momentum and actor landscape.
    """
    if narrative_type in ["crisis", "scandal"]:
        if len(indicators) >= 3:
            return (
                f"{len(indicators)} active crisis indicators suggest narrative will persist "
                f"for 7+ days without intervention. "
                f"{'No defenders present — critics control the story.' if not defenders else 'Defenders present but outnumbered.'}"
            )
        elif len(indicators) >= 1:
            return (
                f"Crisis narrative active with {len(indicators)} confirmed indicator(s). "
                f"Trajectory likely to worsen if Actor Authority targets are not engaged."
            )
        else:
            return "Crisis narrative detected. Monitor for escalation in next 48 hours."

    if momentum == "declining":
        return (
            f"Negative momentum with score at {score}/100. "
            f"Expect continued decline without proactive narrative intervention."
        )

    if momentum == "improving":
        return (
            f"Positive momentum detected at {score}/100. "
            f"Score likely to stabilize or improve over next 7 days."
        )

    return (
        f"Narrative stable at {score}/100. "
        f"No immediate escalation expected. Continue monitoring for volume spikes."
    )


def _estimate_future_score(current_score: int, momentum: str) -> int:
    """
    Simple 7-day score projection based on momentum direction.
    """
    if momentum == "declining":
        return max(0, current_score - 8)
    elif momentum == "improving":
        return min(100, current_score + 8)
    else:
        return current_score


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


def _fallback_result(
    current_score: int,
    score_delta: int,
    data_points: int = 0
) -> dict:
    """
    Fallback when Groq AI parse fails on multi-datapoint path.
    """
    if score_delta <= -10:
        trajectory        = "declining"
        crisis_probability = 55
    elif score_delta >= 10:
        trajectory        = "improving"
        crisis_probability = 8
    else:
        trajectory        = "stable"
        crisis_probability = 15

    return {
        "crisis_probability":        crisis_probability,
        "trajectory":                trajectory,
        "trend_summary":             f"Score has changed {score_delta:+d} points recently.",
        "forecast_7_days":           "Insufficient AI analysis available.",
        "risk_level":                get_risk_level(crisis_probability),
        "alerts":                    [],
        "recommendation":            "Continue monitoring.",
        "estimated_score_in_7_days": current_score,
        "current_score":             current_score,
        "score_delta":               score_delta,
        "data_points":               data_points
    }