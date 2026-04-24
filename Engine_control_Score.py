# engine_control_score.py
# ============================================================
# ReputationSync — Engine 6: Narrative Control Score
# Calculates how much control the brand has over the narrative.
# Logic-based (no API calls) — uses Engine 2 + Engine 3 output.
#
# Factors:
#   Origin     — internal vs external narrative source
#   Velocity   — momentum direction from Engine 2
#   Rigidity   — primary actor type from Engine 3
#   Defenders  — third-party support from Engine 3
#
# Fixed to match actual Engine 3 output structure:
#   - primary_driver_type read from top_actors[0]["type"]
#   - defenders read from narrative_breakdown["defenders"]
# ============================================================

import logging
import database
from datetime import datetime

logger = logging.getLogger(__name__)


def calculate_control_score(entity: str, engine2_result: dict,
                             engine3_result: dict,
                             formation_result: dict = None) -> dict:
    """
    Calculates Narrative Control Score (0-100).

    Inputs:
      entity          — entity name (for DB save)
      engine2_result  — output from engine_understanding.py
      engine3_result  — output from engine_actors.py
      formation_result — output from engine_formation.py (optional)

    Returns dict with:
      narrative_control_score   — 0 to 100
      control_breakdown         — per-factor scores
      intervention_window       — open / narrowing / closed
      control_interpretation    — plain English guidance
      primary_driver_type       — what type of actor is driving narrative
    """

    # ── Factor 1: Origin (0-25 points) ──────────────────────────────────────
    # Internal = brand controls the facts
    # External = brand is reacting
    # Formation engine sets this — defaults to external if not available
    origin_type = "external"
    if formation_result and isinstance(formation_result, dict):
        origin_type = formation_result.get("origin_type", "external")

    origin_map = {
        "internal": 20,
        "hybrid":   12,
        "external":  5
    }
    origin_score = origin_map.get(origin_type, 5)

    # ── Factor 2: Spread Velocity (0-25 points) ──────────────────────────────
    # Engine 2 returns momentum as: improving / stable / declining
    momentum = "stable"
    if engine2_result and isinstance(engine2_result, dict):
        momentum = engine2_result.get("momentum", "stable")

    velocity_map = {
        "improving": 20,
        "stable":    15,
        "declining":  8
    }
    velocity_score = velocity_map.get(momentum, 12)

    # ── Factor 3: Actor Rigidity (0-25 points) ───────────────────────────────
    # Read primary actor type from top_actors[0]["type"]
    # Engine 3 stores type per actor, not at top level
    primary_driver_type = _get_primary_driver_type(engine3_result)

    rigidity_map = {
        "news":    18,   # Can approach with facts and exclusives
        "youtube": 14,   # Can pitch counter-narrative content
        "forum":    8,   # Harder to influence directly
        "social":   5    # Lowest control — viral dynamics
    }
    rigidity_score = rigidity_map.get(primary_driver_type, 10)

    # ── Factor 4: Defender Presence (0-25 points) ────────────────────────────
    # Engine 3 stores defenders inside narrative_breakdown.defenders
    defenders = _get_defenders(engine3_result)
    defender_count = len(defenders)

    if defender_count >= 3:
        defender_score = 22
    elif defender_count == 2:
        defender_score = 16
    elif defender_count == 1:
        defender_score = 10
    else:
        defender_score = 3

    # ── Total + Window ────────────────────────────────────────────────────────
    total = origin_score + velocity_score + rigidity_score + defender_score
    total = max(0, min(100, total))  # Clamp to 0-100

    if total >= 70:
        window = "open"
    elif total >= 45:
        window = "narrowing"
    else:
        window = "closed"

    interpretation = _interpret_control(total)

    result = {
        "narrative_control_score": total,
        "control_breakdown": {
            "origin":            origin_score,
            "velocity":          velocity_score,
            "actor_rigidity":    rigidity_score,
            "defender_presence": defender_score
        },
        "intervention_window":    window,
        "control_interpretation": interpretation,
        "primary_driver_type":    primary_driver_type,
        "origin_type":            origin_type,
        "defender_count":         defender_count,
        "calculated_at":          datetime.utcnow().isoformat()
    }

    # ── Save to DB ────────────────────────────────────────────────────────────
    '''try:
        database.save_control_score(
            entity=entity,
            control_score=total,
            origin_score=origin_score,
            velocity_score=velocity_score,
            rigidity_score=rigidity_score,
            defender_score=defender_score,
            intervention_window=window
        )
    except Exception as e:
        logger.error(f"[Control] DB save failed for '{entity}': {e}")'''

    logger.info(
        f"[Control] '{entity}' scored {total}/100 | "
        f"window: {window} | driver: {primary_driver_type} | "
        f"momentum: {momentum} | defenders: {defender_count}"
    )

    return result


# ── Helper: Extract Primary Driver Type ───────────────────────────────────────

def _get_primary_driver_type(engine3_result: dict) -> str:
    """
    Reads actor type from top_actors[0] in Engine 3 output.
    Engine 3 does not expose primary_driver_type at the top level.
    Falls back to 'news' if structure is missing or malformed.
    """
    if not engine3_result or not isinstance(engine3_result, dict):
        return "news"

    top_actors = engine3_result.get("top_actors", [])
    if not top_actors or not isinstance(top_actors, list):
        return "news"

    first_actor = top_actors[0]
    if not isinstance(first_actor, dict):
        return "news"

    actor_type = first_actor.get("type", "news")

    # Normalise variations that might come back from AI
    normalise_map = {
        "news_outlet":  "news",
        "newspaper":    "news",
        "blog":         "news",
        "video":        "youtube",
        "community":    "forum",
        "social_media": "social"
    }
    return normalise_map.get(actor_type, actor_type)


# ── Helper: Extract Defenders ─────────────────────────────────────────────────

def _get_defenders(engine3_result: dict) -> list:
    """
    Reads defenders from narrative_breakdown.defenders in Engine 3 output.
    Engine 3 stores defenders as a list of source name strings.
    Returns empty list if missing.
    """
    if not engine3_result or not isinstance(engine3_result, dict):
        return []

    breakdown = engine3_result.get("narrative_breakdown", {})
    if not isinstance(breakdown, dict):
        return []

    defenders = breakdown.get("defenders", [])
    if not isinstance(defenders, list):
        return []

    return defenders


# ── Interpretation ────────────────────────────────────────────────────────────

def _interpret_control(score: int) -> str:
    if score >= 75:
        return (
            "Strong control. Narrative is shapeable. "
            "Proactive moves will land well."
        )
    elif score >= 55:
        return (
            "Moderate control. Window is open but closing. "
            "Act within 24-48 hours."
        )
    elif score >= 35:
        return (
            "Weak control. Narrative partially set. "
            "Focus on third-party voices, not direct response."
        )
    else:
        return (
            "Minimal control. Narrative is consolidated against you. "
            "Containment, not redirection."
        )