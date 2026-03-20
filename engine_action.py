import os
import json
from dotenv import load_dotenv
from ai_client import generate

load_dotenv()


def generate_playbook(entity, entity_type, analysis, actors, prediction):

    if not analysis or not prediction:
        return _empty_result()

    score = analysis.get("reputation_score", 50)
    narrative = analysis.get("narrative", {})
    signals = analysis.get("signals", {})
    sentiment = analysis.get("sentiment", {})
    top_actors = actors.get("top_actors", [])[:5]
    critics = actors.get("narrative_breakdown", {}).get("critics", [])
    defenders = actors.get("narrative_breakdown", {}).get("defenders", [])
    crisis_prob = prediction.get("crisis_probability", 0)
    trajectory = prediction.get("trajectory", "stable")
    risk_level = prediction.get("risk_level", "low")
    alerts = prediction.get("alerts", [])
    forecast = prediction.get("forecast_7_days", "")
    recommendation = prediction.get("recommendation", "")

    situation = f"""
ENTITY: {entity} ({entity_type})
REPUTATION SCORE: {score}/100
RISK LEVEL: {risk_level}
CRISIS PROBABILITY: {crisis_prob}%
TRAJECTORY: {trajectory}

CURRENT NARRATIVE:
{narrative.get("current_story", "")}
Narrative type: {narrative.get("narrative_type", "")}
Momentum: {narrative.get("momentum", "")}

SENTIMENT:
Label: {sentiment.get("label", "")}
Reason: {sentiment.get("reason", "")}

CRISIS INDICATORS:
{chr(10).join("- " + s for s in signals.get("crisis_indicators", []))}

POSITIVE SIGNALS:
{chr(10).join("- " + s for s in signals.get("positive_signals", []))}

TOP ACTORS:
{chr(10).join(f"- {a.get('name')} ({a.get('narrative_role')}): {a.get('what_they_say')}" for a in top_actors)}

CRITICS: {", ".join(critics) if critics else "None"}
DEFENDERS: {", ".join(defenders) if defenders else "None"}

ALERTS:
{chr(10).join(f"- [{a.get('urgency').upper()}] {a.get('description')}" for a in alerts)}

7-DAY FORECAST: {forecast}
RECOMMENDATION: {recommendation}
"""

    prompt = f"""You are a world-class reputation strategist.
You have full intelligence on the current reputation situation.

{situation}

Generate a CONCRETE, SPECIFIC, ACTIONABLE playbook to improve this entity's reputation.

Return ONLY a valid JSON object with exactly this structure, no explanation:
{{
  "situation_assessment": "<2-3 honest sentences on where things stand>",
  "strategic_goal": "<what the narrative should look like in 30 days>",
  "immediate_actions": [
    {{
      "priority": <1-5>,
      "action": "<specific action>",
      "why": "<why this works>",
      "how": "<exact steps>",
      "timeline": "<24 hours / 3 days / 1 week>",
      "expected_impact": "<what this changes>"
    }}
  ],
  "narrative_strategy": {{
    "counter_narrative": "<the story to tell instead>",
    "key_messages": ["<message 1>", "<message 2>", "<message 3>"],
    "what_to_avoid": ["<thing that makes it worse>"],
    "reframe": "<how to reframe the negative narrative>"
  }},
  "actor_engagement": [
    {{
      "actor": "<specific outlet>",
      "goal": "<what you want>",
      "approach": "<how to engage>",
      "message": "<what to say>",
      "timing": "<when>"
    }}
  ],
  "content_plan": [
    {{
      "type": "<press release / blog / social / interview>",
      "title": "<specific title>",
      "angle": "<the hook>",
      "platform": "<where to publish>",
      "timing": "<when>"
    }}
  ],
  "what_not_to_do": ["<action to avoid and why>"],
  "30_day_plan": [
    {{"week": 1, "focus": "<priority>", "actions": ["<action>", "<action>"]}},
    {{"week": 2, "focus": "<priority>", "actions": ["<action>", "<action>"]}},
    {{"week": 3, "focus": "<priority>", "actions": ["<action>", "<action>"]}},
    {{"week": 4, "focus": "<priority>", "actions": ["<action>", "<action>"]}}
  ],
  "score_recovery_forecast": {{
    "current_score": {score},
    "realistic_30_day_target": <realistic score>,
    "optimistic_30_day_target": <best case>,
    "if_no_action": <score if nothing done>
  }}
}}"""

    try:
        raw = generate(prompt)

        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        result["entity"] = entity
        result["generated_at"] = __import__("datetime").datetime.utcnow().isoformat()
        result["based_on_score"] = score
        result["based_on_risk"] = risk_level
        return result

    except json.JSONDecodeError as e:
        print(f"[Action] JSON parse error for '{entity}': {e}")
        return _empty_result()

    except Exception as e:
        print(f"[Action] Error for '{entity}': {e}")
        return _empty_result()


def _empty_result() -> dict:
    return {
        "situation_assessment": "Insufficient data to generate playbook.",
        "strategic_goal": "",
        "immediate_actions": [],
        "narrative_strategy": {
            "counter_narrative": "",
            "key_messages": [],
            "what_to_avoid": [],
            "reframe": ""
        },
        "actor_engagement": [],
        "content_plan": [],
        "what_not_to_do": [],
        "30_day_plan": [],
        "score_recovery_forecast": {
            "current_score": 50,
            "realistic_30_day_target": 50,
            "optimistic_30_day_target": 60,
            "if_no_action": 45
        }
    }