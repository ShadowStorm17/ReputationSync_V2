# engine_formation.py
# ============================================================
# ReputationSync — Engine 0: Early Narrative Formation Detection
# Detects narratives BEFORE they surface in mainstream coverage.
#
# How it works:
#   1. Pulls recent mentions from DB for the entity
#   2. Extracts current language patterns (phrases, sources, topics)
#   3. Compares against stored baseline (if exists)
#   4. If deviation exceeds threshold — calls Groq to hypothesize
#      what narrative is forming
#   5. Saves hypothesis to narrative_formation table
#   6. Returns formation signal for /analyze output
#
# If no baseline exists yet — builds one from current data
# and returns no signal (not enough history to detect deviation)
# ============================================================

import json
import re
import logging
import sqlite3
from datetime import datetime, timedelta
from collections import Counter
from ai_client import generate
import database

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
DEVIATION_THRESHOLD = 0.25   # 25% deviation triggers hypothesis
MIN_MENTIONS        = 10     # Minimum mentions needed for detection
BASELINE_DAYS       = 7      # Days of history for baseline


# ── Main Entry Point ──────────────────────────────────────────────────────────

def detect_formation(entity: str, current_posts: list,
                     engine2_result: dict = None) -> dict:
    """
    Engine 0 — Early Narrative Formation Detection.

    Inputs:
      entity        — entity name
      current_posts — posts from current monitoring cycle
      engine2_result — Engine 2 output (used to enrich hypothesis)

    Returns:
      formation dict with hypothesis, confidence, stage, origin_type
      OR empty dict if no signal detected or insufficient data
    """

    if not current_posts or len(current_posts) < 5:
        logger.info(f"[Formation] '{entity}': insufficient posts ({len(current_posts)}) — skipping")
        return _no_signal()

    # ── Step 1: Extract current patterns ─────────────────────────────────────
    current_patterns = _extract_patterns(current_posts)

    # ── Step 2: Get stored baseline ───────────────────────────────────────────
    baseline = database.get_language_baseline(entity)

    if not baseline:
        # No baseline exists — build one from current data and return
        logger.info(f"[Formation] '{entity}': no baseline found — building baseline")
        _build_and_save_baseline(entity, current_posts, current_patterns)
        return _no_signal(reason="baseline_building")

    # ── Step 3: Calculate deviation ───────────────────────────────────────────
    deviation = _calculate_deviation(current_patterns, baseline)

    logger.info(
        f"[Formation] '{entity}': deviation score {deviation:.2f} "
        f"(threshold: {DEVIATION_THRESHOLD})"
    )

    if deviation < DEVIATION_THRESHOLD:
        # Patterns are normal — no formation signal
        logger.info(f"[Formation] '{entity}': patterns normal — no signal")
        return _no_signal(reason="normal_patterns")

    # ── Step 4: Deviation detected — call Groq for hypothesis ─────────────────
    logger.info(
        f"[Formation] '{entity}': deviation {deviation:.2f} exceeds threshold "
        f"— generating hypothesis"
    )

    new_phrases  = _get_new_phrases(current_patterns, baseline)
    new_sources  = _get_new_sources(current_patterns, baseline)
    source_count = len(current_posts)

    hypothesis_result = _generate_hypothesis(
        entity, current_posts, baseline,
        new_phrases, new_sources, deviation,
        engine2_result
    )

    if not hypothesis_result:
        return _no_signal(reason="hypothesis_failed")

    # ── Step 5: Save to database ──────────────────────────────────────────────
    try:
        database.save_formation_signal(
            entity=entity,
            hypothesis=hypothesis_result.get("hypothesis", ""),
            confidence=hypothesis_result.get("confidence", 0),
            stage=hypothesis_result.get("stage", "seed"),
            origin_type=hypothesis_result.get("origin_type", "external"),
            source_count=source_count,
            time_to_surface=hypothesis_result.get("time_to_surface", 72)
        )
    except Exception as e:
        logger.error(f"[Formation] DB save failed for '{entity}': {e}")

    # ── Step 6: Update baseline weekly ───────────────────────────────────────
    baseline_age = _get_baseline_age(baseline)
    if baseline_age > 7:
        logger.info(f"[Formation] '{entity}': baseline is {baseline_age} days old — refreshing")
        _build_and_save_baseline(entity, current_posts, current_patterns)

    logger.info(
        f"[Formation] '{entity}': signal detected | "
        f"stage: {hypothesis_result.get('stage')} | "
        f"confidence: {hypothesis_result.get('confidence')}% | "
        f"origin: {hypothesis_result.get('origin_type')}"
    )

    return hypothesis_result


# ── Pattern Extraction ────────────────────────────────────────────────────────

def _extract_patterns(posts: list) -> dict:
    """
    Extracts language patterns from a list of posts.
    Returns phrases, sources, and tone indicators.
    """
    all_words   = []
    all_phrases = []
    sources     = []
    tones       = []

    for post in posts:
        if not isinstance(post, dict):
            continue

        text   = post.get("text", "").lower()
        source = post.get("source_name", "")
        sentiment = post.get("sentiment", "neutral")

        if source:
            sources.append(source)
        if sentiment:
            tones.append(sentiment)

        # Extract words (3+ chars, no stopwords)
        words = [
            w for w in re.findall(r'\b[a-z]{3,}\b', text)
            if w not in STOPWORDS
        ]
        all_words.extend(words)

        # Extract 2-word phrases
        for i in range(len(words) - 1):
            phrase = f"{words[i]} {words[i+1]}"
            all_phrases.append(phrase)

    # Count frequencies
    word_freq   = Counter(all_words).most_common(50)
    phrase_freq = Counter(all_phrases).most_common(30)
    source_freq = Counter(sources).most_common(20)
    tone_freq   = Counter(tones)

    # Calculate tone ratio
    total_tones = sum(tone_freq.values()) or 1
    neg_ratio   = tone_freq.get("negative", 0) / total_tones

    return {
        "top_words":   [w for w, _ in word_freq],
        "top_phrases": [p for p, _ in phrase_freq],
        "sources":     [s for s, _ in source_freq],
        "neg_ratio":   neg_ratio,
        "total_posts": len(posts)
    }


# ── Deviation Calculation ─────────────────────────────────────────────────────

def _calculate_deviation(current: dict, baseline: dict) -> float:
    """
    Calculates how much current patterns deviate from baseline.
    Returns 0.0 (identical) to 1.0 (completely different).

    Three components:
      - Phrase novelty: new phrases not in baseline
      - Source novelty: new sources not in baseline
      - Tone shift: change in negative ratio
    """
    # Component 1: Phrase novelty
    baseline_phrases = set(baseline.get("common_phrases", []))
    current_phrases  = set(current.get("top_phrases", []))

    if current_phrases:
        new_phrases    = current_phrases - baseline_phrases
        phrase_novelty = len(new_phrases) / len(current_phrases)
    else:
        phrase_novelty = 0.0

    # Component 2: Source novelty
    baseline_sources = set(baseline.get("source_mix", {}).keys()
                           if isinstance(baseline.get("source_mix"), dict)
                           else baseline.get("source_mix", []))
    current_sources  = set(current.get("sources", []))

    if current_sources:
        new_sources    = current_sources - baseline_sources
        source_novelty = len(new_sources) / len(current_sources)
    else:
        source_novelty = 0.0

    # Component 3: Tone shift
    baseline_neg = baseline.get("tone_baseline", 0.3)
    current_neg  = current.get("neg_ratio", 0.3)
    tone_shift   = abs(current_neg - baseline_neg)

    # Weighted combination
    deviation = (
        phrase_novelty * 0.45 +
        source_novelty * 0.35 +
        tone_shift     * 0.20
    )

    return round(deviation, 3)


# ── New Phrase/Source Detection ───────────────────────────────────────────────

def _get_new_phrases(current: dict, baseline: dict) -> list:
    baseline_phrases = set(baseline.get("common_phrases", []))
    current_phrases  = current.get("top_phrases", [])
    return [p for p in current_phrases if p not in baseline_phrases][:10]


def _get_new_sources(current: dict, baseline: dict) -> list:
    baseline_sources = set(
        baseline.get("source_mix", {}).keys()
        if isinstance(baseline.get("source_mix"), dict)
        else baseline.get("source_mix", [])
    )
    current_sources = current.get("sources", [])
    return [s for s in current_sources if s not in baseline_sources][:5]


# ── Groq Hypothesis Generation ────────────────────────────────────────────────

def _generate_hypothesis(
    entity: str, posts: list, baseline: dict,
    new_phrases: list, new_sources: list,
    deviation: float, engine2_result: dict = None
) -> dict:
    """
    Calls Groq to generate a narrative formation hypothesis.
    Only called when deviation exceeds threshold.
    """

    # Sample recent posts for context
    sample_texts = []
    for p in posts[:20]:
        if isinstance(p, dict) and p.get("text"):
            sample_texts.append(
                f"[{p.get('source_name', '?')}]: {p.get('text', '')[:150]}"
            )
    sample_block = "\n".join(sample_texts)

    # Engine 2 context if available
    e2_context = ""
    if engine2_result and isinstance(engine2_result, dict):
        narrative = engine2_result.get("narrative", {})
        e2_context = (
            f"Current narrative type: {narrative.get('narrative_type', 'unknown')}\n"
            f"Current momentum: {narrative.get('momentum', 'unknown')}\n"
            f"Current story: {narrative.get('current_story', '')[:200]}"
        )

    prompt = f"""You are an elite narrative intelligence analyst.
Your job is to detect narratives that are FORMING before they break into mainstream coverage.

ENTITY: {entity}

DEVIATION SIGNAL: {deviation:.0%} pattern deviation from baseline detected.
This means something is shifting in how {entity} is being discussed.

NEW PHRASES appearing that were not in baseline:
{', '.join(new_phrases) if new_phrases else 'None identified'}

NEW SOURCES covering {entity} that did not appear before:
{', '.join(new_sources) if new_sources else 'None identified'}

CURRENT ENGINE CONTEXT:
{e2_context if e2_context else 'Not available'}

RECENT CONTENT SAMPLE:
{sample_block}

Based on these signals, identify what narrative is FORMING about {entity}.
This is pre-mainstream — the narrative has not fully broken yet.

Be specific. Do not describe what is already known.
Describe what is EMERGING based on the pattern shifts above.

Return ONLY valid JSON, no explanation, no markdown:
{{
  "hypothesis": "<one specific sentence describing the narrative that is forming — what story is taking shape>",
  "confidence": <number 0-100 — how confident are you this narrative will surface>,
  "stage": "<seed or forming or consolidating>",
  "origin_type": "<internal or external or hybrid>",
  "time_to_surface": <estimated hours until this reaches mainstream coverage — number only>,
  "key_signals": [
    "<specific signal 1 that supports this hypothesis>",
    "<specific signal 2>"
  ],
  "recommended_pre_action": "<one sentence on what {entity} should do RIGHT NOW before this surfaces>"
}}"""

    try:
        raw = generate(prompt)
        raw = _clean_json(raw)
        result = json.loads(raw)

        # Validate minimum structure
        if not result.get("hypothesis"):
            logger.warning(f"[Formation] Empty hypothesis for '{entity}'")
            return None

        # Clamp confidence to 0-100
        result["confidence"] = max(0, min(100, result.get("confidence", 50)))

        # Clamp time_to_surface to reasonable range
        result["time_to_surface"] = max(
            6, min(168, result.get("time_to_surface", 72))
        )

        return result

    except json.JSONDecodeError as e:
        logger.warning(f"[Formation] JSON parse error for '{entity}': {e}")
        try:
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception:
            pass
        return None

    except Exception as e:
        logger.error(f"[Formation] Groq error for '{entity}': {e}")
        return None


# ── Baseline Builder ──────────────────────────────────────────────────────────

def _build_and_save_baseline(entity: str, posts: list, patterns: dict):
    """
    Builds and saves a language baseline from current mention data.
    Called when no baseline exists or baseline is older than 7 days.
    """
    try:
        # Build source mix as dict with counts
        source_counter = Counter(
            p.get("source_name", "") for p in posts
            if isinstance(p, dict)
        )
        source_mix = dict(source_counter.most_common(20))

        # Calculate tone baseline
        tones = [
            p.get("sentiment", "neutral") for p in posts
            if isinstance(p, dict)
        ]
        tone_counter = Counter(tones)
        total = sum(tone_counter.values()) or 1
        neg_ratio = tone_counter.get("negative", 0) / total

        database.save_language_baseline(
            entity=entity,
            common_phrases=patterns.get("top_phrases", []),
            topic_clusters=patterns.get("top_words", []),
            source_mix=source_mix,
            tone_baseline=neg_ratio
        )

        logger.info(
            f"[Formation] Baseline saved for '{entity}' | "
            f"phrases: {len(patterns.get('top_phrases', []))} | "
            f"sources: {len(source_mix)} | "
            f"neg_ratio: {neg_ratio:.2f}"
        )

    except Exception as e:
        logger.error(f"[Formation] Baseline save error for '{entity}': {e}")


# ── Baseline Age ──────────────────────────────────────────────────────────────

def _get_baseline_age(baseline: dict) -> int:
    """Returns age of baseline in days. Returns 999 if unknown."""
    try:
        window_date = baseline.get("window_date", "")
        if not window_date:
            return 999
        baseline_dt = datetime.strptime(window_date, "%Y-%m-%d")
        age = (datetime.utcnow() - baseline_dt).days
        return age
    except Exception:
        return 999


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
    open_braces = raw.count('{')
    close_braces = raw.count('}')
    if open_braces > close_braces:
        raw += '}' * (open_braces - close_braces)
    return raw


# ── No Signal Response ────────────────────────────────────────────────────────

def _no_signal(reason: str = "no_deviation") -> dict:
    return {
        "signal_detected": False,
        "reason": reason,
        "hypothesis": None,
        "confidence": 0,
        "stage": None,
        "origin_type": None,
        "time_to_surface": None,
        "key_signals": [],
        "recommended_pre_action": None
    }


# ── Stopwords ─────────────────────────────────────────────────────────────────

STOPWORDS = {
    "the", "and", "for", "that", "this", "with", "from", "has",
    "have", "are", "its", "was", "been", "but", "not", "they",
    "will", "would", "could", "should", "their", "than", "more",
    "also", "about", "which", "when", "what", "who", "how",
    "all", "said", "says", "new", "can", "may", "out", "one",
    "year", "last", "first", "after", "over", "into", "any",
    "had", "him", "his", "her", "she", "our", "your", "there",
    "been", "being", "were", "just", "even", "most", "some",
    "such", "both", "each", "does", "did", "use", "way", "get",
    "make", "made", "time", "now", "two", "very", "well", "back"
}