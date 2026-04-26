# engine_intelligence.py
# ============================================================
# ReputationSync — Strategic Intelligence Brief
# Interprets all engine outputs like a senior PR strategist.
#
# This is NOT a data summarizer.
# This thinks through what is actually happening, who is
# driving it, whether it matters, and what to do about it.
#
# Output becomes the "intelligence_brief" field in /analyze.
# This is what a PR professional reads first.
# ============================================================

import json
import re
import logging
from ai_client import generate

logger = logging.getLogger(__name__)


# ── Main Entry Point ──────────────────────────────────────────────────────────

def generate_intelligence_brief(
    entity: str,
    entity_type: str,
    engine2_result: dict,
    engine3_result: dict,
    engine4_result: dict,
    control_result: dict,
    trajectory_result: dict,
    formation_result: dict = None
) -> dict:
    """
    Generates a strategic intelligence brief from all engine outputs.
    Thinks like a senior PR strategist, not a data processor.

    Returns structured brief with:
      - narrative_status: what is actually happening
      - signal_quality: what matters vs noise
      - actor_diagnosis: who is shaping this and how
      - control_diagnosis: who owns the narrative right now
      - intervention_window: open/narrow/closed with reasoning
      - response_options: 2-3 specific paths with tradeoffs
      - trajectory_plain: simple direction + triggers
      - priority_action: the one thing to do first
    """

    # ── Build context block ───────────────────────────────────────────────────
    context = _build_context(
        entity, entity_type, engine2_result, engine3_result,
        engine4_result, control_result, trajectory_result,
        formation_result
    )

    prompt = f"""You are a senior PR strategist and narrative intelligence analyst with 20 years of experience managing reputation crises for major brands, politicians, and public figures.

You are NOT a data summarization engine.
You interpret signals, identify what is really happening, and advise on what to do next.

Your output must make a PR professional say "this actually helps me decide what to do."

INTELLIGENCE DATA:
{context}

THINKING FRAMEWORK — work through this before writing output:
1. Is this noise / early signal / forming narrative / established narrative?
2. Who is actually driving this? (real influence, not just mention counts)
3. Where did this originate? (mainstream vs niche vs insider)
4. Why is momentum increasing or decreasing?
5. Is there still an intervention window? Or is framing already locked?

Now generate the strategic intelligence brief.

HARD RULES:
- Do NOT hallucinate events not in the data above
- Do NOT overstate crisis without strong signals
- Do NOT rely purely on sentiment scores
- Do NOT produce generic PR advice ("improve communication" is not acceptable)
- Every recommendation must name specific actors or channels from the data
- Speak plainly — a founder and a PR intern must both understand this

Return ONLY valid JSON, no explanation, no markdown:
{{
  "narrative_status": {{
    "classification": "<noise or early_signal or forming_narrative or established_narrative>",
    "what_is_happening": "<2-3 sentences explaining what story is forming or has formed — plain language, no jargon>",
    "why_it_matters": "<1-2 sentences on the real business or reputation impact if this continues>",
    "stage": "<early or forming or set>"
  }},
  "signal_quality": {{
    "signals_that_matter": [
      "<signal 1 from the data that is a real indicator — explain why it matters>",
      "<signal 2>"
    ],
    "signals_that_are_noise": [
      "<signal that looks concerning but is not — explain why it is noise>"
    ],
    "overall_signal_strength": "<weak or moderate or strong>"
  }},
  "actor_diagnosis": {{
    "who_is_shaping_this": "<name the specific actor driving perception — not just listing, explain their mechanism of influence>",
    "amplifiers": ["<actor amplifying the narrative>"],
    "originators": ["<actor who started or is originating the narrative>"],
    "credible_voices_involved": "<yes or no — and who they are if yes>",
    "defender_situation": "<are credible defenders present or is the narrative unchallenged>"
  }},
  "control_diagnosis": {{
    "who_controls_narrative": "<brand or external or contested>",
    "control_slipping": "<yes or no>",
    "why": "<specific reason control is held or lost — reference actual actors and signals>"
  }},
  "intervention_window": {{
    "status": "<open or narrow or closed>",
    "reasoning": "<specific explanation of why — not generic>",
    "hours_remaining": "<estimated hours before window closes or N/A if open>"
  }},
  "response_options": [
    {{
      "option": "A",
      "label": "<short name for this approach>",
      "what_to_do": "<specific action — names channels, actors, format>",
      "why_it_works": "<mechanism — why this specifically works for this situation>",
      "tradeoff": "<honest risk if this goes wrong>"
    }},
    {{
      "option": "B",
      "label": "<short name>",
      "what_to_do": "<specific action>",
      "why_it_works": "<mechanism>",
      "tradeoff": "<honest risk>"
    }},
    {{
      "option": "C",
      "label": "<short name>",
      "what_to_do": "<specific action>",
      "why_it_works": "<mechanism>",
      "tradeoff": "<honest risk>"
    }}
  ],
  "trajectory_plain": {{
    "direction": "<improving or stable or worsening>",
    "escalation_trigger": "<the one specific thing that would make this significantly worse>",
    "reversal_trigger": "<the one specific thing that could reverse negative momentum>"
  }},
  "priority_action": "<one sentence — the single most important thing to do right now, specific and actionable>"
}}"""

    try:
        raw = generate(prompt)
        raw = _clean_json(raw)
        result = json.loads(raw)

        logger.info(
            f"[Intelligence] Brief generated for '{entity}' | "
            f"classification: {result.get('narrative_status', {}).get('classification', '?')} | "
            f"window: {result.get('intervention_window', {}).get('status', '?')} | "
            f"signal strength: {result.get('signal_quality', {}).get('overall_signal_strength', '?')}"
        )

        return result

    except json.JSONDecodeError as e:
        logger.warning(f"[Intelligence] JSON parse error for '{entity}': {e}")
        try:
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception:
            pass
        logger.error(f"[Intelligence] Failed for '{entity}' — returning fallback")
        return _fallback_brief(entity, engine2_result, control_result)

    except Exception as e:
        logger.error(f"[Intelligence] Unexpected error for '{entity}': {e}")
        return _fallback_brief(entity, engine2_result, control_result)


# ── Context Builder ───────────────────────────────────────────────────────────

def _build_context(
    entity, entity_type, engine2_result, engine3_result,
    engine4_result, control_result, trajectory_result,
    formation_result
) -> str:
    """Builds a clean context block for the prompt."""

    # Engine 2 signals
    narrative      = engine2_result.get("narrative", {})
    signals        = engine2_result.get("signals", {})
    sentiment      = engine2_result.get("sentiment", {})
    summary        = engine2_result.get("summary", "")

    score          = sentiment.get("score", 50)
    narrative_type = narrative.get("narrative_type", "unknown")
    momentum       = narrative.get("momentum", "stable")
    current_story  = narrative.get("current_story", "")
    crisis_inds    = signals.get("crisis_indicators", [])
    pos_signals    = signals.get("positive_signals", [])

    # Engine 3 actors
    top_actors  = engine3_result.get("top_actors", [])[:5]
    critics     = engine3_result.get("narrative_breakdown", {}).get("critics", [])
    defenders   = engine3_result.get("narrative_breakdown", {}).get("defenders", [])
    primary_drv = engine3_result.get("primary_driver_source", "Unknown")
    primary_cnt = engine3_result.get("primary_driver_count", 0)

    actor_lines = []
    for a in top_actors:
        actor_lines.append(
            f"  {a.get('name')} | type: {a.get('type')} | "
            f"role: {a.get('narrative_role')} | "
            f"mentions: {a.get('mention_count')} | "
            f"says: {a.get('what_they_say', '')[:100]}"
        )

    # Engine 4 prediction
    crisis_prob = engine4_result.get("crisis_probability", 0)
    risk_level  = engine4_result.get("risk_level", "low")
    trajectory  = engine4_result.get("trajectory", "stable")
    alerts      = engine4_result.get("alerts", [])

    alert_lines = [
        f"  [{a.get('urgency','').upper()}] {a.get('description','')}"
        for a in alerts
    ]

    # Control score
    control_score  = control_result.get("narrative_control_score", 50)
    window         = control_result.get("intervention_window", "narrowing")
    control_interp = control_result.get("control_interpretation", "")

    # Trajectory scenarios
    scenario_paths = trajectory_result.get("scenario_paths", {}) if trajectory_result else {}
    most_likely    = scenario_paths.get("most_likely", {})
    escalation     = trajectory_result.get("escalation_triggers", []) if trajectory_result else []
    velocity       = trajectory_result.get("momentum_velocity", "steady") if trajectory_result else "steady"

    # Formation signal
    formation_block = "No formation signal detected."
    if formation_result and formation_result.get("signal_detected"):
        formation_block = (
            f"EARLY SIGNAL DETECTED:\n"
            f"  Hypothesis: {formation_result.get('hypothesis', '')}\n"
            f"  Confidence: {formation_result.get('confidence', 0)}%\n"
            f"  Stage: {formation_result.get('stage', '')}\n"
            f"  Time to surface: {formation_result.get('time_to_surface', '?')} hours\n"
            f"  Pre-action: {formation_result.get('recommended_pre_action', '')}"
        )

    return f"""ENTITY: {entity} ({entity_type})
REPUTATION SCORE: {score}/100
RISK LEVEL: {risk_level.upper()}
CRISIS PROBABILITY: {crisis_prob}%
NARRATIVE TYPE: {narrative_type}
MOMENTUM: {momentum}
CURRENT STORY: {current_story}

CONFIRMED CRISIS INDICATORS:
{chr(10).join("  - " + s for s in crisis_inds) or "  None"}

CONFIRMED POSITIVE SIGNALS:
{chr(10).join("  - " + s for s in pos_signals) or "  None"}

ACTOR LANDSCAPE:
{chr(10).join(actor_lines) or "  No actor data"}
PRIMARY DRIVER: {primary_drv} ({primary_cnt} mentions)
CRITICS: {", ".join(critics) if critics else "None"}
DEFENDERS: {", ".join(defenders) if defenders else "None — narrative unchallenged"}

ACTIVE ALERTS:
{chr(10).join(alert_lines) or "  None"}

NARRATIVE CONTROL SCORE: {control_score}/100
INTERVENTION WINDOW: {window.upper()}
CONTROL INTERPRETATION: {control_interp}

TRAJECTORY:
  Most likely: {most_likely.get('conditions', 'Unknown')} 
  Day-30 score: {most_likely.get('score_at_day_30', '?')}/100
  Momentum velocity: {velocity}
  Escalation triggers: {", ".join(escalation[:2]) if escalation else "None identified"}

FORMATION DETECTION:
{formation_block}

ENGINE SUMMARY: {summary}"""


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

def _fallback_brief(entity: str, engine2_result: dict,
                    control_result: dict) -> dict:
    """Returns minimal valid structure if Groq fails."""
    momentum = engine2_result.get("narrative", {}).get("momentum", "stable")
    window   = control_result.get("intervention_window", "narrowing") if control_result else "narrowing"

    return {
        "narrative_status": {
            "classification": "unknown",
            "what_is_happening": "Analysis unavailable — retry in next cycle.",
            "why_it_matters": "Unable to assess at this time.",
            "stage": "unknown"
        },
        "signal_quality": {
            "signals_that_matter":   [],
            "signals_that_are_noise": [],
            "overall_signal_strength": "unknown"
        },
        "actor_diagnosis": {
            "who_is_shaping_this":      "Unknown",
            "amplifiers":               [],
            "originators":              [],
            "credible_voices_involved": "unknown",
            "defender_situation":       "unknown"
        },
        "control_diagnosis": {
            "who_controls_narrative": "unknown",
            "control_slipping":       "unknown",
            "why":                    "Analysis unavailable"
        },
        "intervention_window": {
            "status":          window,
            "reasoning":       "Based on control score calculation",
            "hours_remaining": "Unknown"
        },
        "response_options": [],
        "trajectory_plain": {
            "direction":          momentum,
            "escalation_trigger": "Unknown",
            "reversal_trigger":   "Unknown"
        },
        "priority_action": "Run full analysis cycle to generate strategic brief."
    }