# engine_trajectory.py
# ============================================================
# ReputationSync — Engine 7: Narrative Trajectory Model
# Models where the narrative is heading across three scenarios.
# Uses one Groq call per entity per cycle.
#
# Output:
#   scenario_paths — best / most_likely / worst case
#   escalation_triggers — specific named events
#   amplification_pathways — actors that could spread narrative
#   momentum_velocity — accelerating / steady / decelerating
# ============================================================

import json
import re
import logging
from ai_client import generate

logger = logging.getLogger(__name__)


# ── Main Entry Point ──────────────────────────────────────────────────────────

def model_trajectory(entity: str, entity_type: str,
                     engine2_result: dict, engine3_result: dict,
                     control_result: dict = None) -> dict:
    """
    Models narrative trajectory across three scenario paths.

    Inputs:
      entity         — entity name
      entity_type    — brand / person / politician etc
      engine2_result — output from engine_understanding.py
      engine3_result — output from engine_actors.py
      control_result — output from engine_control_score.py (optional)

    Returns:
      Full trajectory dict with scenario paths and escalation triggers.
    """

    # ── Extract key signals ───────────────────────────────────────────────────
    narrative     = engine2_result.get("narrative", {})
    signals       = engine2_result.get("signals", {})
    sentiment     = engine2_result.get("sentiment", {})

    score          = sentiment.get("score", 50)
    narrative_type = narrative.get("narrative_type", "neutral")
    momentum       = narrative.get("momentum", "stable")
    current_story  = narrative.get("current_story", "")

    crisis_indicators = signals.get("crisis_indicators", [])
    positive_signals  = signals.get("positive_signals", [])

    primary_driver    = engine3_result.get("primary_driver_source", "Unknown")
    primary_count     = engine3_result.get("primary_driver_count", 0)
    top_actors        = engine3_result.get("top_actors", [])
    defenders         = engine3_result.get("narrative_breakdown", {}).get("defenders", [])
    critics           = engine3_result.get("narrative_breakdown", {}).get("critics", [])

    control_score     = control_result.get("narrative_control_score", 50) if control_result else 50
    window            = control_result.get("intervention_window", "narrowing") if control_result else "narrowing"

    # ── Build actor summary for prompt ───────────────────────────────────────
    actor_lines = []
    for actor in top_actors[:5]:
        if isinstance(actor, dict):
            actor_lines.append(
                f"{actor.get('name', '?')} "
                f"({actor.get('type', '?')}, "
                f"{actor.get('mention_count', 0)} mentions, "
                f"role: {actor.get('narrative_role', '?')})"
            )
    actor_summary = "\n".join(actor_lines) if actor_lines else "No actor data"

    # ── Build prompt ──────────────────────────────────────────────────────────
    prompt = f"""You are a senior narrative strategist with 20 years of PR crisis experience.

Model the narrative trajectory for "{entity}" ({entity_type}).

CURRENT STATE:
- Reputation score: {score}/100
- Narrative type: {narrative_type}
- Momentum: {momentum}
- Current story: {current_story}
- Crisis indicators: {', '.join(crisis_indicators) if crisis_indicators else 'None'}
- Positive signals: {', '.join(positive_signals) if positive_signals else 'None'}
- Narrative control score: {control_score}/100
- Intervention window: {window}

ACTOR LANDSCAPE:
{actor_summary}
- Primary driver: {primary_driver} ({primary_count} mentions)
- Critics: {', '.join(critics) if critics else 'None'}
- Defenders: {', '.join(defenders) if defenders else 'None'}

MODEL THREE SCENARIO PATHS for the next 30 days.
Be specific. Reference actual actors and indicators named above.
Do not use generic statements like "if media picks this up".
Name the specific outlet, actor or event.

Return ONLY valid JSON, no explanation, no markdown:
{{
  "scenario_paths": {{
    "best_case": {{
      "conditions": "<what must happen for this to resolve positively — be specific>",
      "endpoint": "<narrative state at day 30 if best case occurs>",
      "probability": <number 0-100>,
      "score_at_day_30": <predicted score 0-100>
    }},
    "most_likely": {{
      "conditions": "<what happens if current trajectory continues unchanged>",
      "endpoint": "<narrative state at day 30 on current path>",
      "probability": <number 0-100>,
      "score_at_day_30": <predicted score 0-100>
    }},
    "worst_case": {{
      "conditions": "<specific events that would escalate this — name actors and outlets>",
      "endpoint": "<narrative state at day 30 if worst case occurs>",
      "probability": <number 0-100>,
      "score_at_day_30": <predicted score 0-100>
    }}
  }},
  "escalation_triggers": [
    "<specific named event 1 that moves worst case from possible to probable>",
    "<specific named event 2>",
    "<specific named event 3>"
  ],
  "amplification_pathways": [
    "<actor or platform 1 that could spread this narrative faster>",
    "<actor or platform 2>"
  ],
  "momentum_velocity": "<accelerating or steady or decelerating>",
  "momentum_reasoning": "<one sentence explaining why>",
  "narrative_window": "<one sentence on how much time exists to intervene>"
}}"""

    # ── Call Groq ─────────────────────────────────────────────────────────────
    try:
        raw = generate(prompt)
        raw = _clean_json(raw)
        result = json.loads(raw)

        # Validate minimum structure
        if not result.get("scenario_paths"):
            logger.warning(f"[Trajectory] Missing scenario_paths for '{entity}' — using fallback")
            return _fallback_trajectory(entity, score, momentum, narrative_type)

        # Attach metadata
        result["entity"]         = entity
        result["based_on_score"] = score
        result["narrative_type"] = narrative_type

        logger.info(
            f"[Trajectory] '{entity}' modeled | "
            f"most_likely: {result['scenario_paths']['most_likely'].get('probability', '?')}% | "
            f"worst_case: {result['scenario_paths']['worst_case'].get('probability', '?')}% | "
            f"velocity: {result.get('momentum_velocity', '?')}"
        )

        return result

    except json.JSONDecodeError as e:
        logger.warning(f"[Trajectory] JSON parse error for '{entity}': {e}")
        return _fallback_trajectory(entity, score, momentum, narrative_type)

    except Exception as e:
        logger.error(f"[Trajectory] Error for '{entity}': {e}")
        return _fallback_trajectory(entity, score, momentum, narrative_type)


# ── JSON Cleaner ──────────────────────────────────────────────────────────────

def _clean_json(raw: str) -> str:
    if "```" in raw:
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]

    raw = raw.strip()
    raw = re.sub(r",\s*}", "}", raw)
    raw = re.sub(r",\s*]", "]", raw)
    raw = re.sub(r'[\x00-\x1f\x7f]', ' ', raw)

    open_braces  = raw.count('{')
    close_braces = raw.count('}')
    if open_braces > close_braces:
        raw += '}' * (open_braces - close_braces)

    return raw


# ── Fallback ──────────────────────────────────────────────────────────────────

def _fallback_trajectory(entity: str, score: int,
                          momentum: str, narrative_type: str) -> dict:
    """
    Returns a logic-based fallback when Groq parse fails.
    Never crashes — always returns valid structure.
    """

    # Estimate scenario scores based on momentum
    if momentum == "improving":
        best_score    = min(100, score + 20)
        likely_score  = min(100, score + 8)
        worst_score   = max(0, score - 5)
        velocity      = "accelerating"
        best_prob     = 35
        likely_prob   = 50
        worst_prob    = 15
    elif momentum == "declining":
        best_score    = min(100, score + 5)
        likely_score  = max(0, score - 8)
        worst_score   = max(0, score - 20)
        velocity      = "decelerating"
        best_prob     = 15
        likely_prob   = 50
        worst_prob    = 35
    else:
        best_score    = min(100, score + 10)
        likely_score  = score
        worst_score   = max(0, score - 10)
        velocity      = "steady"
        best_prob     = 25
        likely_prob   = 55
        worst_prob    = 20

    return {
        "entity":         entity,
        "based_on_score": score,
        "narrative_type": narrative_type,
        "scenario_paths": {
            "best_case": {
                "conditions":      "Positive signals amplified and crisis indicators resolved.",
                "endpoint":        f"Narrative stabilizes positively at day 30.",
                "probability":     best_prob,
                "score_at_day_30": best_score
            },
            "most_likely": {
                "conditions":      f"Current {momentum} trajectory continues unchanged.",
                "endpoint":        f"Narrative remains {narrative_type} at day 30.",
                "probability":     likely_prob,
                "score_at_day_30": likely_score
            },
            "worst_case": {
                "conditions":      "Crisis indicators escalate and primary actors amplify narrative.",
                "endpoint":        f"Narrative deteriorates significantly by day 30.",
                "probability":     worst_prob,
                "score_at_day_30": worst_score
            }
        },
        "escalation_triggers": [
            "Primary actor increases coverage volume significantly",
            "Additional outlets pick up existing narrative threads",
            "Crisis indicators confirmed by second independent source"
        ],
        "amplification_pathways": [
            "Primary news outlet coverage",
            "Forum discussion amplification"
        ],
        "momentum_velocity":  velocity,
        "momentum_reasoning": f"Based on {momentum} momentum at score {score}/100.",
        "narrative_window":   "Intervention window based on current momentum direction.",
        "fallback":           True
    }