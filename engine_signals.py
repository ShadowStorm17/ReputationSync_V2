# engine_signals.py
# ============================================================
# ReputationSync — Signal Extraction Engine (Phase 2)
# Implements hardened constraints for narrative intelligence.
#
# Each signal must produce:
#   claim        — what is being asserted (specific sentence)
#   angle        — how it is framed
#   implication  — why it matters if it spreads
#   change_type  — new_claim / new_angle / resurfaced_claim / no_change
#   what_changed — explicit description of the shift
#   why_now      — reason for emergence if inferable
#
# Signals that cannot produce a clear claim are discarded as noise.
# Classification is based on framing change, not timestamps.
# ============================================================

import json
import re
import logging
from ai_client import generate
import database

logger = logging.getLogger(__name__)


# ── Main Entry Point ──────────────────────────────────────────────────────────

def extract_signals(entity: str, current_posts: list,
                    previous_state: dict = None) -> dict:
    """
    Extracts and classifies signals from current monitoring cycle.

    Inputs:
      entity         — entity name
      current_posts  — posts from current monitoring cycle
      previous_state — previous narrative state from DB (or None)

    Returns:
      signals          — list of classified signal objects
      narrative_shift  — explicit shift assessment
      signal_summary   — counts by classification
      confidence       — overall confidence with basis
    """

    if not current_posts or len(current_posts) < 3:
        return _empty_result(reason="insufficient_posts")

    # ── Step 1: Extract raw claims from posts ─────────────────────────────────
    raw_claims = _extract_raw_claims(entity, current_posts)

    if not raw_claims:
        return _empty_result(reason="no_claims_extracted")

    # ── Step 2: Classify each claim ───────────────────────────────────────────
    classified_signals = _classify_signals(
        entity, raw_claims, previous_state
    )

    # ── Step 3: Detect narrative shift ────────────────────────────────────────
    narrative_shift = _detect_narrative_shift(
        entity, classified_signals, previous_state
    )

    # ── Step 4: Filter noise ──────────────────────────────────────────────────
    # Only signals with clear claims and non-zero change survive
    actionable_signals = [
        s for s in classified_signals
        if s.get("claim") and s.get("change_type") != "no_change"
    ]
    noise_count = len(classified_signals) - len(actionable_signals)

    # ── Step 5: Confidence assessment ─────────────────────────────────────────
    confidence = _assess_confidence(
        actionable_signals, previous_state, len(current_posts)
    )

    # ── Step 6: Save actionable signals to DB ─────────────────────────────────
    for signal in actionable_signals:
        try:
            database.save_mention_signal(
                entity=entity,
                signal_text=signal.get("claim", "")[:500],
                claim=signal.get("claim", ""),
                narrative_angle=signal.get("angle", ""),
                classification=_map_to_db_classification(
                    signal.get("change_type", "new_claim")
                ),
                confidence=confidence.get("score", 0.5)
            )
        except Exception as e:
            logger.error(f"[Signals] DB save error for '{entity}': {e}")

    logger.info(
        f"[Signals] '{entity}': {len(actionable_signals)} actionable | "
        f"{noise_count} noise | "
        f"shift: {narrative_shift.get('shift_type', 'none')} | "
        f"confidence: {confidence.get('level', 'unknown')}"
    )

    return {
        "signals":         actionable_signals,
        "narrative_shift": narrative_shift,
        "signal_summary": {
            "total_posts":        len(current_posts),
            "claims_extracted":   len(classified_signals),
            "actionable_signals": len(actionable_signals),
            "discarded_as_noise": noise_count,
            "new_claim_count":    sum(
                1 for s in actionable_signals
                if s.get("change_type") == "new_claim"
            ),
            "resurfaced_count":   sum(
                1 for s in actionable_signals
                if s.get("change_type") == "resurfaced_claim"
            ),
            "angle_shift_count":  sum(
                1 for s in actionable_signals
                if s.get("change_type") == "new_angle"
            )
        },
        "confidence": confidence
    }


# ── Step 1: Extract Raw Claims ────────────────────────────────────────────────

def _extract_raw_claims(entity: str, posts: list) -> list:
    """
    Uses Groq to extract discrete claims from post content.
    Each claim must reduce to: claim + angle + implication.
    Signals that cannot produce a clear claim are discarded.
    """

    sample_lines = []
    for p in posts[:25]:
        if isinstance(p, dict) and p.get("text"):
            sample_lines.append(
                f"[{p.get('source_name', '?')} | "
                f"{p.get('source_type', 'unknown')}]: "
                f"{p.get('text', '')[:200]}"
            )
    sample_block = "\n".join(sample_lines)

    prompt = f"""You are a signal extraction specialist.

Your job: extract discrete claims being made about "{entity}" from media content.

A CLAIM is a specific assertion being made.
Not a topic. Not a theme. A specific assertable statement.

VALID CLAIM EXAMPLES:
  "Tesla autopilot caused a fatal accident in Texas"
  "Nike uses child labor in Vietnamese factories"
  "Uber concealed a data breach from regulators"

INVALID — DISCARD THESE:
  "Sentiment is mixed"
  "There are concerns about the company"
  "Performance issues reported"

CONTENT TO ANALYZE:
{sample_block}

For each distinct claim extract:
  claim       — the specific assertion (one clear sentence, max 20 words)
  angle       — how it is framed (e.g. "safety failure", "regulatory evasion", "financial misconduct")
  implication — why this matters for {entity} reputation if it spreads (one sentence)
  source      — which outlet is making this claim
  source_type — forum / news / youtube / social

RULES:
  - Maximum 6 claims total
  - If you cannot write a clear one-sentence claim → discard that signal as noise
  - Do not combine multiple claims into one
  - Do not invent claims not present in the content above
  - Each claim must be falsifiable (it can be proven true or false)

Return ONLY valid JSON:
{{
  "claims": [
    {{
      "claim": "<specific one-sentence assertion>",
      "angle": "<framing label — 2-4 words>",
      "implication": "<reputation consequence if this spreads>",
      "source": "<outlet name>",
      "source_type": "<forum or news or youtube or social>"
    }}
  ]
}}"""

    try:
        raw = generate(prompt)
        raw = _clean_json(raw)
        result = json.loads(raw)
        claims = result.get("claims", [])

        # Hard filter: must have claim and angle
        valid = [
            c for c in claims
            if c.get("claim") and len(c.get("claim", "")) > 10
            and c.get("angle")
        ]

        logger.info(
            f"[Signals] '{entity}': {len(valid)} valid claims "
            f"from {len(posts)} posts "
            f"({len(claims) - len(valid)} discarded as noise)"
        )
        return valid

    except json.JSONDecodeError as e:
        logger.warning(f"[Signals] JSON parse error for '{entity}': {e}")
        try:
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                result = json.loads(match.group())
                return result.get("claims", [])
        except Exception:
            pass
        return []

    except Exception as e:
        logger.error(f"[Signals] Claim extraction failed for '{entity}': {e}")
        return []


# ── Step 2: Classify Signals ──────────────────────────────────────────────────

def _classify_signals(entity: str, claims: list,
                      previous_state: dict) -> list:
    """
    Classifies each claim against previous narrative state.

    change_type:
      new_claim        — claim never seen before
      new_angle        — same topic, different framing
      resurfaced_claim — seen before, was absent, now back
      no_change        — present and unchanged (filtered as noise)

    what_changed: explicit description of shift
    why_now: reason for emergence if inferable
    """

    if not previous_state:
        # First cycle — everything is new by definition
        classified = []
        for claim in claims:
            classified.append({
                **claim,
                "change_type":      "new_claim",
                "what_changed":     (
                    f"New claim detected: "
                    f"'{claim.get('claim', '')[:100]}'"
                ),
                "why_now":          (
                    "First monitoring cycle — "
                    "no baseline to compare against"
                ),
                "confidence":       "low",
                "confidence_basis": (
                    "No previous state exists. "
                    "Classification will improve after second cycle."
                )
            })
        return classified

    prev_dominant  = previous_state.get("dominant_narrative", "")
    prev_keywords  = previous_state.get("framing_keywords", [])
    prev_emerging  = previous_state.get("emerging_narratives", [])
    prev_all_text  = " ".join([
        prev_dominant,
        " ".join(
            n.get("framing", "") if isinstance(n, dict) else str(n)
            for n in prev_emerging
        )
    ]).lower()

    classified = []
    for claim in claims:
        claim_text = claim.get("claim", "").lower()
        angle      = claim.get("angle", "").lower()

        # Check DB for historical presence
        db_class = database.classify_mention_signal(
            entity, claim.get("claim", "")
        )

        if db_class == "REACTIVATED":
            change_type  = "resurfaced_claim"
            what_changed = _describe_reactivation(claim, prev_all_text)
            why_now      = (
                "This claim was previously active, "
                "disappeared from coverage, and is now resurfacing"
            )

        elif db_class == "FRESH":
            angle_shift = _detect_angle_shift(
                claim_text, angle, prev_keywords, prev_all_text
            )

            if angle_shift["shifted"]:
                change_type  = "new_angle"
                what_changed = angle_shift["description"]
                why_now      = angle_shift.get("why_now", "Framing has shifted")
            else:
                change_type  = "new_claim"
                what_changed = (
                    f"New claim appearing in coverage: "
                    f"'{claim.get('claim', '')[:100]}'"
                )
                why_now = (
                    "No prior detection of this specific claim"
                )

        else:
            # LEGACY — no meaningful change
            change_type  = "no_change"
            what_changed = "No meaningful change detected"
            why_now      = "N/A"

        # Source-based confidence
        source_type   = claim.get("source_type", "unknown")
        conf_level, conf_basis = _source_confidence(source_type)

        classified.append({
            **claim,
            "change_type":      change_type,
            "what_changed":     what_changed,
            "why_now":          why_now,
            "confidence":       conf_level,
            "confidence_basis": conf_basis
        })

    return classified


# ── Step 3: Detect Narrative Shift ────────────────────────────────────────────

def _detect_narrative_shift(entity: str, signals: list,
                             previous_state: dict) -> dict:
    """
    Detects whether overall narrative framing has shifted.

    shift_type:
      intensification — same narrative, stronger signal
      reversal        — narrative moving opposite direction
      pivot           — moving to entirely new topic
      none            — no meaningful shift

    RULE: If no shift → must explicitly state:
    "No meaningful narrative shift detected"
    """

    if not previous_state:
        return {
            "previous_framing": "No previous state — first cycle",
            "current_framing":  _summarize_current_framing(signals),
            "shift":            (
                "No meaningful narrative shift detected — "
                "first monitoring cycle, no baseline to compare"
            ),
            "shift_type":       "none",
            "confidence":       "low",
            "confidence_basis": "No baseline exists for comparison"
        }

    prev_dominant = previous_state.get("dominant_narrative", "Unknown")
    current_framing = _summarize_current_framing(signals)

    new_claims    = sum(
        1 for s in signals if s.get("change_type") == "new_claim"
    )
    new_angles    = sum(
        1 for s in signals if s.get("change_type") == "new_angle"
    )
    resurfaced    = sum(
        1 for s in signals if s.get("change_type") == "resurfaced_claim"
    )
    total         = len(signals)

    if total == 0:
        return {
            "previous_framing": prev_dominant,
            "current_framing":  "No actionable signals detected",
            "shift":            "No meaningful narrative shift detected",
            "shift_type":       "none",
            "confidence":       "low",
            "confidence_basis": "No signals to assess"
        }

    # Determine shift type from signal composition
    if new_angles >= 2:
        shift_type = "pivot"
        shift_desc = (
            f"Narrative pivot detected: {new_angles} signals show "
            f"existing topics reframed with new angles about {entity}. "
            f"The issue is the same — the framing has changed."
        )
    elif resurfaced >= 2:
        shift_type = "intensification"
        shift_desc = (
            f"Intensification detected: {resurfaced} previously dormant "
            f"claims have resurfaced simultaneously. "
            f"This suggests coordinated or event-driven reactivation."
        )
    elif new_claims >= 3:
        shift_type = "pivot"
        shift_desc = (
            f"Narrative pivot: {new_claims} entirely new claims emerging "
            f"that were not present in the previous cycle."
        )
    elif resurfaced == 1 and new_claims <= 1:
        shift_type = "intensification"
        shift_desc = (
            f"Minor intensification: one resurfaced claim and limited "
            f"new signals. Monitor for escalation next cycle."
        )
    else:
        shift_type = "none"
        shift_desc = "No meaningful narrative shift detected"

    # Confidence
    if total >= 4:
        conf_level = "medium"
        conf_basis = (
            f"{total} signals assessed — "
            f"moderate confidence in shift detection"
        )
    else:
        conf_level = "low"
        conf_basis = (
            f"Only {total} signals — "
            f"limited confidence in shift assessment"
        )

    return {
        "previous_framing": prev_dominant,
        "current_framing":  current_framing,
        "shift":            shift_desc,
        "shift_type":       shift_type,
        "confidence":       conf_level,
        "confidence_basis": conf_basis
    }


# ── Confidence Assessment ─────────────────────────────────────────────────────

def _assess_confidence(signals: list, previous_state: dict,
                       post_count: int) -> dict:
    """
    High:   4+ signals, 2+ source types, baseline exists
    Medium: 2+ signals, baseline exists
    Low:    few signals, no baseline, single source
    """

    if not signals:
        return {
            "level": "low",
            "score": 0.2,
            "basis": "No actionable signals extracted from this cycle"
        }

    source_types  = set(s.get("source_type", "") for s in signals)
    has_baseline  = previous_state is not None
    signal_count  = len(signals)

    if signal_count >= 4 and len(source_types) >= 2 and has_baseline:
        return {
            "level": "high",
            "score": 0.8,
            "basis": (
                f"{signal_count} signals from "
                f"{len(source_types)} source types "
                f"with baseline comparison — "
                f"multiple agreeing signals increase confidence"
            )
        }
    elif signal_count >= 2 and has_baseline:
        return {
            "level": "medium",
            "score": 0.55,
            "basis": (
                f"{signal_count} signals with baseline available. "
                f"Limited source diversity reduces confidence. "
                f"More cycles will improve accuracy."
            )
        }
    else:
        missing = []
        if not has_baseline:
            missing.append("no baseline for comparison")
        if signal_count < 2:
            missing.append(f"only {signal_count} signal(s) detected")
        if len(source_types) < 2:
            missing.append("single source type")

        return {
            "level": "low",
            "score": 0.3,
            "basis": (
                f"Low confidence: {', '.join(missing)}. "
                f"System needs more cycles to build reliable detection."
            )
        }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _describe_reactivation(claim: dict, prev_text: str) -> str:
    claim_text = claim.get("claim", "")
    angle      = claim.get("angle", "")

    # Check if the topic appeared in previous narrative
    claim_words = set(claim_text.lower().split())
    prev_words  = set(prev_text.lower().split())
    overlap     = claim_words & prev_words

    if len(overlap) >= 2:
        return (
            f"Resurfaced claim: '{claim_text[:120]}' — "
            f"previously present in coverage, now re-emerging "
            f"with '{angle}' framing"
        )

    return (
        f"Resurfaced claim: '{claim_text[:120]}' — "
        f"was absent from recent coverage and has now returned"
    )


def _detect_angle_shift(claim_text: str, current_angle: str,
                         prev_keywords: list, prev_text: str) -> dict:
    """
    Detects if a known topic is being framed with a new angle.
    Requires topic overlap with previous coverage.
    """
    claim_words = set(claim_text.lower().split())
    prev_words  = set(prev_text.lower().split())
    overlap     = claim_words & prev_words

    if len(overlap) >= 3:
        # Topic is known but angle may be new
        angle_in_prev = current_angle.lower() in prev_text.lower()

        if not angle_in_prev:
            return {
                "shifted":     True,
                "description": (
                    f"Known topic now framed differently: "
                    f"'{current_angle}' angle is new. "
                    f"Topic was present before but this framing was not."
                ),
                "why_now": (
                    f"The underlying issue is the same — "
                    f"the '{current_angle}' framing is the new element"
                )
            }

    return {"shifted": False}


def _source_confidence(source_type: str) -> tuple:
    """Returns confidence level and basis for a source type."""
    if source_type == "news":
        return (
            "medium",
            "News source — claim has editorial validation "
            "but single outlet. Needs corroboration."
        )
    elif source_type == "forum":
        return (
            "low",
            "Forum source — community discussion, "
            "not editorially verified. Watch for pickup by news."
        )
    elif source_type == "youtube":
        return (
            "low",
            "YouTube source — individual creator claim. "
            "Confidence increases if news outlets follow."
        )
    elif source_type == "social":
        return (
            "low",
            "Social media source — unverified. "
            "Only actionable if amplified by credible outlets."
        )
    else:
        return (
            "low",
            "Unknown source type — cannot assess credibility."
        )


def _summarize_current_framing(signals: list) -> str:
    if not signals:
        return "No signals detected this cycle"

    angles = [s.get("angle", "") for s in signals if s.get("angle")]
    if not angles:
        return "Signals present but framing unclear"

    angle_counts = {}
    for a in angles:
        angle_counts[a] = angle_counts.get(a, 0) + 1

    dominant     = max(angle_counts, key=angle_counts.get)
    other_angles = [
        a for a in angle_counts if a != dominant
    ]

    if other_angles:
        return (
            f"Dominant framing: '{dominant}' "
            f"({angle_counts[dominant]} signals). "
            f"Secondary framings: {', '.join(other_angles[:2])}"
        )

    return f"Single framing detected: '{dominant}'"


def _map_to_db_classification(change_type: str) -> str:
    """Maps engine change_type to DB classification enum."""
    mapping = {
        "new_claim":        "FRESH",
        "new_angle":        "FRESH",
        "resurfaced_claim": "REACTIVATED",
        "no_change":        "LEGACY"
    }
    return mapping.get(change_type, "FRESH")


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


def _empty_result(reason: str = "unknown") -> dict:
    return {
        "signals": [],
        "narrative_shift": {
            "previous_framing": "Unknown",
            "current_framing":  "Unknown",
            "shift":            "No meaningful narrative shift detected",
            "shift_type":       "none",
            "confidence":       "low",
            "confidence_basis": f"Signal extraction could not run: {reason}"
        },
        "signal_summary": {
            "total_posts":        0,
            "claims_extracted":   0,
            "actionable_signals": 0,
            "discarded_as_noise": 0,
            "new_claim_count":    0,
            "resurfaced_count":   0,
            "angle_shift_count":  0
        },
        "confidence": {
            "level": "low",
            "score": 0.1,
            "basis": f"Signal extraction failed: {reason}"
        }
    }