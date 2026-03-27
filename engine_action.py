# engine_action.py
# ============================================================
# ReputationSync — Engine 5: Action (THE MOAT)
# Prescriptive playbook — the only tool that prescribes
#
# Architecture: Two focused Groq calls merged into one playbook.
# Call 1: Situation + Immediate Actions + Actor Engagement
# Call 2: 30-day Plan + Content + Spokesperson + Forecast
#
# Fix 1 — Score forecast realism (numeric bounds + trajectory floor)
# Fix 2 — Forum engagement logic (type-aware approach rules)
# Fix 3 — Hallucination guard (no invented names or initiatives)
# Fix 4 — Week-specific success metrics (each week measurably different)
# Fix 5 — what_not_to_do tied to confirmed crisis indicators
# Fix 6 — Key messages specific enough to be spoken directly to press
# ============================================================

import os
import json
import re
import logging
from datetime import datetime
from dotenv import load_dotenv
from ai_client import generate

load_dotenv()
logger = logging.getLogger(__name__)


# ── Entity-Type Context ───────────────────────────────────────────────────────

ENTITY_TYPE_CONTEXT = {
    "brand": (
        "Focus on consumer trust, product quality, and corporate responsibility. "
        "Actions must target consumer-facing channels, retail partnerships, "
        "and product narrative. Personal statements from executives carry less "
        "weight than product proof points."
    ),
    "company": (
        "Focus on investor confidence, operational transparency, and stakeholder trust. "
        "Actions must address earnings narrative, regulatory response, and B2B relationships. "
        "Board-level communication carries significant weight."
    ),
    "person": (
        "Focus on personal brand integrity and professional credibility. "
        "Actions must balance personal authenticity with professional reputation. "
        "Direct personal communication (interviews, statements) carries more "
        "weight than corporate PR."
    ),
    "politician": (
        "Focus on voter approval, policy narrative, and party alignment. "
        "Every action has electoral consequences — weigh carefully. "
        "Constituent communication, town halls, and policy wins outweigh media spin."
    ),
    "celebrity": (
        "Focus on fan loyalty, media relationships, and cultural relevance. "
        "Social media presence, exclusive interviews, and project announcements "
        "drive narrative shifts. Authenticity beats polish for celebrity reputation recovery."
    ),
    "film": (
        "Focus on audience sentiment, critical reception, and box office trajectory. "
        "Talent visibility, review response strategy, and audience engagement are "
        "primary levers. Social proof from critics and influencers moves the needle fastest."
    ),
    "founder": (
        "Focus on company performance narrative tied to personal credibility. "
        "Address both personal brand and company reputation simultaneously — "
        "they are inseparable. Investor, employee, and customer sentiment all "
        "require separate communication tracks."
    ),
}


# ── Crisis Tier Instructions ──────────────────────────────────────────────────

CRISIS_TIER_INSTRUCTIONS = {
    "critical": (
        "CRISIS TIER: CRITICAL — Emergency response. "
        "Generate 6 immediate actions minimum. Every action has 24-48 hour window. "
        "Damage containment before narrative repair. Target critics first. "
        "Include specific what NOT to say. Front-load weeks 1 and 2 of 30-day plan heavily."
    ),
    "high": (
        "CRISIS TIER: HIGH — Proactive containment. "
        "Generate 5 immediate actions minimum. Balance containment with positive "
        "narrative building. Target both critics and convertible neutral actors. "
        "Include media Q&A preparation. Week 1 carries most of the work."
    ),
    "medium": (
        "CRISIS TIER: MEDIUM — Narrative shaping. "
        "Generate 4 immediate actions. Build positive narrative momentum. "
        "Convert neutral actors to defenders. Amplify existing positive signals."
    ),
    "low": (
        "CRISIS TIER: LOW — Maintenance mode. "
        "Generate 3 immediate actions focused on momentum building. "
        "Reinforce positive signals. Amplify existing defenders."
    ),
}


# ── Actor Type Engagement Rules ───────────────────────────────────────────────

ACTOR_TYPE_ENGAGEMENT_RULES = """
ACTOR ENGAGEMENT RULES BY TYPE — FOLLOW STRICTLY:
- Forums (Hacker News, Reddit, Product Hunt):
    DO: AMA post, direct comment response from entity, submit positive
        content for community upvoting, engage in discussion threads,
        respond to top critical comments personally.
    DO NOT: Suggest exclusive interview, press briefing, or embargo
        with a forum. Forums are communities not editors.

- News outlets (CNBC, Bloomberg, WSJ, Reuters, NYT, Guardian, BBC):
    DO: Exclusive interview, embargo briefing, op-ed submission,
        direct quote response, press conference invitation,
        background briefing for context-setting.
    DO NOT: Suggest AMA or community post for a news outlet.

- YouTube channels:
    DO: Exclusive video interview, documentary cooperation,
        behind-the-scenes access, response video statement.
    DO NOT: Treat YouTube channels as forums or newspapers.

- Social media (Twitter/X, LinkedIn, Instagram):
    DO: Direct post, thread, pinned statement, live video response.
    DO NOT: Suggest social media as a press briefing venue.
"""


# ── JSON Cleaner ──────────────────────────────────────────────────────────────

def clean_json(raw: str) -> str:
    if "```" in raw:
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()
    raw = re.sub(r",\s*}", "}", raw)
    raw = re.sub(r",\s*]", "]", raw)
    return raw


# ── Situation Block Builder ───────────────────────────────────────────────────

def build_situation_block(
    entity, entity_type, score, narrative, signals,
    sentiment, summary, top_actors, critics, defenders,
    neutral, primary_driver, crisis_prob, trajectory,
    risk_level, alerts, forecast, recommendation,
    estimated_score
) -> str:
    actor_block = ""
    for a in top_actors:
        actor_type = a.get("type", "unknown")
        actor_block += (
            f"\n  - {a.get('name')} | Type: {actor_type} | "
            f"Role: {a.get('narrative_role')} | "
            f"Influence: {a.get('influence')} | "
            f"Sentiment: {a.get('sentiment_toward_entity', 0):+.1f} | "
            f"Says: {a.get('what_they_say', '')[:120]}"
        )

    type_context = ENTITY_TYPE_CONTEXT.get(
        entity_type.lower(), ENTITY_TYPE_CONTEXT["brand"]
    )

    return f"""
ENTITY: {entity} ({entity_type})
SCORE: {score}/100 | RISK: {risk_level.upper()} | CRISIS PROBABILITY: {crisis_prob}%
TRAJECTORY: {trajectory} | SCORE IN 7 DAYS IF NO ACTION: {estimated_score}/100

ENTITY TYPE CONTEXT: {type_context}

NARRATIVE: {narrative.get("current_story", "")}
Type: {narrative.get("narrative_type", "")} | Momentum: {narrative.get("momentum", "")}

SUMMARY: {summary}

SENTIMENT: {sentiment.get("label", "")} — {sentiment.get("reason", "")}

CONFIRMED CRISIS INDICATORS (real media — do not add to or invent from this list):
{chr(10).join("- " + s for s in signals.get("crisis_indicators", [])) or "None"}

CONFIRMED POSITIVE SIGNALS (real media — do not add to or invent from this list):
{chr(10).join("- " + s for s in signals.get("positive_signals", [])) or "None"}

ACTOR AUTHORITY LANDSCAPE (Type field determines engagement method):
{actor_block or "No actor data"}

CRITICS (driving negative narrative): {", ".join(critics) if critics else "None"}
DEFENDERS (supporting entity): {", ".join(defenders) if defenders else "NONE — narrative unchallenged"}
NEUTRAL (convertible): {", ".join(neutral) if neutral else "None"}
PRIMARY NARRATIVE DRIVER: {primary_driver or "Unknown"}

ACTIVE ALERTS:
{chr(10).join(f"- [{a.get('urgency','').upper()}] {a.get('description','')}" for a in alerts) or "None"}

7-DAY FORECAST: {forecast}
RECOMMENDATION: {recommendation}
"""


# ── Call 1: Immediate Actions + Actor Engagement ──────────────────────────────

def call_immediate_actions(
    situation: str,
    entity: str,
    entity_type: str,
    score: int,
    risk_level: str,
    crisis_prob: int
) -> dict:
    tier = CRISIS_TIER_INSTRUCTIONS.get(
        risk_level.lower(), CRISIS_TIER_INSTRUCTIONS["medium"]
    )

    prompt = f"""You are the world's most elite reputation crisis strategist.
A client needs an emergency action plan right now.

{tier}

{ACTOR_TYPE_ENGAGEMENT_RULES}

INTELLIGENCE BRIEFING:
{situation}

RULES — FOLLOW EXACTLY:
1. Every action must name specific outlets, platforms, or people from the intelligence above
2. NO generic advice — "engage with media" is not acceptable
3. Every immediate action MUST have if_taken, if_ignored, and score_impact fields
4. if_taken = specific narrative outcome if action is executed within timeline
5. if_ignored = specific consequence referencing the actual actor or indicator affected
6. score_impact = realistic score change (e.g. "+3 to +6 points")
7. Actor engagement approach must match actor TYPE using engagement rules above
8. Forums (Hacker News, Reddit) NEVER receive interview or press briefing suggestions
9. repercussion_if_ignored must name the specific actor and their likely next move
10. HALLUCINATION GUARD: Never invent product names, projects, initiatives, or events
    not in the CONFIRMED signals above. Work only with what is in the briefing.
11. KEY MESSAGES must be complete sentences a spokesperson can say directly to camera.
    Not topics. Not themes. Actual quotable statements grounded in confirmed facts.
    Bad example: "We are addressing the lawsuit."
    Good example: "We are cooperating fully with the SEC investigation and have
    provided all requested documents — we expect this matter to be resolved in court."

Return ONLY valid JSON — no explanation, no markdown:
{{
  "situation_assessment": "<3-4 sentences referencing real crisis indicators and named actors>",
  "strategic_goal": "<specific headline you want written about {entity} in 30 days>",
  "immediate_actions": [
    {{
      "priority": 1,
      "action": "<specific named action — who, what, where, referencing real actor>",
      "why": "<why this works for {entity}'s specific confirmed situation>",
      "how": "<exact steps — platform, format, who delivers, key message to lead with>",
      "timeline": "<24 hours or 48 hours or 3 days or 1 week>",
      "expected_impact": "<specific narrative change — not generic>",
      "if_taken": "<exactly what improves — name the actor or indicator affected>",
      "if_ignored": "<exactly what gets worse — name the specific actor and their likely action>",
      "score_impact": "<realistic range e.g. +3 to +6 points>"
    }}
  ],
  "narrative_strategy": {{
    "counter_narrative": "<specific story using only CONFIRMED positive signals — no invented claims>",
    "key_messages": [
      "<complete quotable sentence — spokesperson reads this directly to press, references confirmed fact>",
      "<complete quotable sentence — directly addresses a named confirmed crisis indicator honestly>",
      "<complete quotable sentence — forward-looking, grounded in confirmed signals only>"
    ],
    "what_to_avoid": [
      "<specific communication trap tied to a confirmed crisis indicator — not generic advice>"
    ],
    "reframe": "<how to reframe the specific confirmed negative narrative — use real language>",
    "tone_guidance": "<how {entity} should sound given crisis level and entity type>"
  }},
  "actor_engagement": [
    {{
      "actor": "<real actor name from intelligence above>",
      "actor_type": "<forum or news or youtube or social>",
      "current_stance": "<their exact role and sentiment from the data>",
      "goal": "<specific measurable outcome you want from this actor>",
      "approach": "<engagement method matching actor TYPE from engagement rules>",
      "message": "<specific angle or content — grounded in confirmed signals only>",
      "timing": "<when and specifically why this timing>",
      "repercussion_if_ignored": "<what this actor specifically does next if not engaged>"
    }}
  ]
}}"""

    try:
        raw = generate(prompt)
        raw = clean_json(raw)
        return json.loads(raw)
    except Exception as e:
        logger.error(f"[Action] Call 1 failed for '{entity}': {e}")
        return {}


# ── Call 2: Recovery Plan + Spokesperson + Forecast ──────────────────────────

def call_recovery_plan(
    situation: str,
    entity: str,
    entity_type: str,
    score: int,
    risk_level: str,
    crisis_prob: int,
    trajectory: str
) -> dict:
    max_realistic  = min(100, score + 20)
    max_optimistic = min(100, score + 30)

    if trajectory in ["declining", "critical"]:
        if_no_action = max(0, score - 12)
    elif trajectory == "stable":
        if_no_action = max(0, score - 5)
    else:
        if_no_action = max(0, score - 2)

    prompt = f"""You are the world's most elite reputation crisis strategist.
You are building the 30-day recovery plan for a client in a {risk_level} risk situation.

{ACTOR_TYPE_ENGAGEMENT_RULES}

INTELLIGENCE BRIEFING:
{situation}

RULES — FOLLOW EXACTLY:
1. Content plan titles must be actual publishable headlines specific to {entity} — not placeholders
2. what_not_to_do must reference CONFIRMED crisis indicators — not generic PR rules
3. Spokesperson talking points must be complete press-ready sentences — not topics or themes
4. Media Q&A questions must be ones journalists would ask based on CONFIRMED crisis indicators
5. 30-day plan weeks 1 and 2 must be heavily front-loaded with specific actions
6. SUCCESS METRICS MUST BE DIFFERENT FOR EACH WEEK AND SPECIFIC:
   - Week 1 metric: measures CONTAINMENT (e.g. "No new crisis indicator surfaces in coverage")
   - Week 2 metric: measures NARRATIVE SHIFT (e.g. "At least 2 neutral outlets publish positive angle")
   - Week 3 metric: measures ACTOR CONVERSION (e.g. "Primary forum critic sentiment moves from negative to neutral")
   - Week 4 metric: measures SCORE MOVEMENT (e.g. "Reputation score reaches X/100 based on next analysis run")
   Do NOT use the same metric for multiple weeks.
   Do NOT use vague metrics like "increase in positive coverage."
7. Score forecast strictly bounded:
   - current_score: {score}
   - realistic_30_day_target: {max_realistic} exactly
   - optimistic_30_day_target: {max_optimistic} exactly
   - if_no_action: {if_no_action} or lower given {trajectory} trajectory
8. what_not_to_do actions must be specific to {entity}'s CONFIRMED situation:
   Reference actual crisis indicators by name. Explain the specific mechanism
   by which the mistake amplifies the confirmed problem.
9. HALLUCINATION GUARD: Never invent product names, projects, initiatives,
   or events not in the CONFIRMED signals above.

Return ONLY valid JSON — no explanation, no markdown:
{{
  "content_plan": [
    {{
      "type": "<press release or blog post or social thread or video or interview or op-ed>",
      "title": "<actual publishable headline specific to {entity}'s real confirmed situation>",
      "angle": "<specific hook tied to a CONFIRMED positive signal>",
      "platform": "<exact platform or publication name>",
      "timing": "<when and specifically why>",
      "expected_reach": "<who this reaches and why it matters for the narrative>",
      "narrative_friction_reduction": "<which specific confirmed Narrative Friction this removes>"
    }}
  ],
  "what_not_to_do": [
    {{
      "action": "<specific named action tied to a confirmed crisis indicator>",
      "reason": "<exact mechanism by which this amplifies {entity}'s confirmed problem>",
      "risk": "<specific consequence — name the actor or outlet likely to escalate>"
    }}
  ],
  "spokesperson_guidance": {{
    "recommended_spokesperson": "<who should speak and why — specific to {entity}'s situation>",
    "talking_points": [
      "<press-ready sentence 1 — spokesperson reads this verbatim, references confirmed fact>",
      "<press-ready sentence 2 — addresses confirmed crisis indicator directly and honestly>",
      "<press-ready sentence 3 — forward-looking, grounded in confirmed positive signals only>"
    ],
    "what_not_to_say": [
      "<specific phrase that would amplify a named confirmed crisis indicator>"
    ],
    "media_qa_prep": [
      {{
        "likely_question": "<question journalist asks based on confirmed crisis indicator 1>",
        "recommended_answer": "<honest specific answer — does not deny confirmed facts>"
      }},
      {{
        "likely_question": "<question based on confirmed crisis indicator 2>",
        "recommended_answer": "<honest specific answer>"
      }},
      {{
        "likely_question": "<question based on Actor Authority landscape — critic outlet angle>",
        "recommended_answer": "<honest specific answer>"
      }}
    ]
  }},
  "30_day_plan": [
    {{
      "week": 1,
      "focus": "Crisis containment — stop the bleeding",
      "actions": ["<specific action>", "<specific action>", "<specific action>"],
      "success_metric": "<containment metric — specific and measurable, not generic>"
    }},
    {{
      "week": 2,
      "focus": "Narrative launch — introduce the counter-story",
      "actions": ["<specific action>", "<specific action>", "<specific action>"],
      "success_metric": "<narrative shift metric — different from week 1, specific>"
    }},
    {{
      "week": 3,
      "focus": "Actor conversion — move neutral outlets to defender",
      "actions": ["<specific action>", "<specific action>"],
      "success_metric": "<actor conversion metric — names a specific outlet or forum>"
    }},
    {{
      "week": 4,
      "focus": "Consolidation — lock in score recovery",
      "actions": ["<specific action>", "<specific action>"],
      "success_metric": "<score movement metric — references target score number>"
    }}
  ],
  "score_recovery_forecast": {{
    "current_score": {score},
    "realistic_30_day_target": {max_realistic},
    "optimistic_30_day_target": {max_optimistic},
    "if_no_action": {if_no_action},
    "key_milestones": [
      "<specific confirmed action that must happen for score to reach {max_realistic}>",
      "<second milestone tied to confirmed actors or signals>",
      "<third milestone>"
    ]
  }}
}}"""

    try:
        raw = generate(prompt)
        raw = clean_json(raw)
        return json.loads(raw)
    except Exception as e:
        logger.error(f"[Action] Call 2 failed for '{entity}': {e}")
        return {}


# ── Main Entry Point ──────────────────────────────────────────────────────────

def generate_playbook(
    entity: str,
    entity_type: str,
    analysis: dict,
    actors: dict,
    prediction: dict
) -> dict:
    """
    Engine 5 — Generates full prescriptive action playbook.
    Two focused Groq calls merged into one complete playbook.
    All six quality fixes applied.
    """
    if not analysis or not prediction:
        return _empty_result()

    # ── Extract Intelligence ──────────────────────────────────────────────────
    score           = analysis.get("reputation_score", 50)
    narrative       = analysis.get("narrative", {})
    signals         = analysis.get("signals", {})
    sentiment       = analysis.get("sentiment", {})
    summary         = analysis.get("summary", "")

    top_actors      = actors.get("top_actors", [])[:6]
    critics         = actors.get("narrative_breakdown", {}).get("critics", [])
    defenders       = actors.get("narrative_breakdown", {}).get("defenders", [])
    neutral         = actors.get("narrative_breakdown", {}).get("neutral", [])
    primary_driver  = actors.get("primary_driver_source", "")

    crisis_prob     = prediction.get("crisis_probability", 0)
    trajectory      = prediction.get("trajectory", "stable")
    risk_level      = prediction.get("risk_level", "low")
    alerts          = prediction.get("alerts", [])
    forecast        = prediction.get("forecast_7_days", "")
    recommendation  = prediction.get("recommendation", "")
    estimated_score = prediction.get("estimated_score_in_7_days", score)

    # ── Build Shared Situation Block ──────────────────────────────────────────
    situation = build_situation_block(
        entity, entity_type, score, narrative, signals,
        sentiment, summary, top_actors, critics, defenders,
        neutral, primary_driver, crisis_prob, trajectory,
        risk_level, alerts, forecast, recommendation,
        estimated_score
    )

    # ── Call 1: Immediate Actions + Actor Engagement ──────────────────────────
    logger.info(
        f"[Action] Call 1 — Immediate actions for '{entity}' | "
        f"Risk: {risk_level} | Score: {score}"
    )
    part1 = call_immediate_actions(
        situation, entity, entity_type, score, risk_level, crisis_prob
    )

    # ── Call 2: Recovery Plan + Spokesperson + Forecast ───────────────────────
    logger.info(f"[Action] Call 2 — Recovery plan for '{entity}'")
    part2 = call_recovery_plan(
        situation, entity, entity_type, score, risk_level, crisis_prob, trajectory
    )

    # ── Merge ─────────────────────────────────────────────────────────────────
    playbook = {
        "situation_assessment":  part1.get("situation_assessment", ""),
        "strategic_goal":        part1.get("strategic_goal", ""),
        "immediate_actions":     part1.get("immediate_actions", []),
        "narrative_strategy":    part1.get("narrative_strategy", {}),
        "actor_engagement":      part1.get("actor_engagement", []),
        "content_plan":          part2.get("content_plan", []),
        "what_not_to_do":        part2.get("what_not_to_do", []),
        "spokesperson_guidance": part2.get("spokesperson_guidance", {}),
        "30_day_plan":           part2.get("30_day_plan", []),
        "score_recovery_forecast": part2.get("score_recovery_forecast", {
            "current_score":            score,
            "realistic_30_day_target":  min(100, score + 20),
            "optimistic_30_day_target": min(100, score + 30),
            "if_no_action":             max(0, score - 8),
            "key_milestones":           []
        }),
        "entity":             entity,
        "entity_type":        entity_type,
        "generated_at":       datetime.utcnow().isoformat(),
        "based_on_score":     score,
        "based_on_risk":      risk_level,
        "crisis_probability": crisis_prob,
    }

    logger.info(
        f"[Action] Playbook complete for '{entity}' | "
        f"Actions: {len(playbook['immediate_actions'])} | "
        f"Actor targets: {len(playbook['actor_engagement'])} | "
        f"Risk: {risk_level} | Score: {score}"
    )

    return playbook


# ── Empty Result Fallback ─────────────────────────────────────────────────────

def _empty_result(score: int = 50, risk_level: str = "unknown") -> dict:
    return {
        "situation_assessment":   "Insufficient data. Run /analyze first.",
        "strategic_goal":         "",
        "immediate_actions":      [],
        "narrative_strategy": {
            "counter_narrative":  "",
            "key_messages":       [],
            "what_to_avoid":      [],
            "reframe":            "",
            "tone_guidance":      ""
        },
        "actor_engagement":       [],
        "content_plan":           [],
        "what_not_to_do":         [],
        "spokesperson_guidance": {
            "recommended_spokesperson": "",
            "talking_points":           [],
            "what_not_to_say":          [],
            "media_qa_prep":            []
        },
        "30_day_plan":            [],
        "score_recovery_forecast": {
            "current_score":            score,
            "realistic_30_day_target":  score,
            "optimistic_30_day_target": min(100, score + 10),
            "if_no_action":             max(0, score - 5),
            "key_milestones":           []
        },
        "entity":             "",
        "generated_at":       datetime.utcnow().isoformat(),
        "based_on_score":     score,
        "based_on_risk":      risk_level,
        "crisis_probability": 0
    }