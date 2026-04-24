# engine_control_score.py
# ============================================================
# ReputationSync — Engine 6: Narrative Control Score
# Calculates how much control the brand has over the narrative.
# Logic-based (no API calls) to save quota.
#
# Factors:
#   Origin (internal/external)
#   Velocity (momentum)
#   Rigidity (actor type)
#   Defenders (third-party support)
# ============================================================

import database
from datetime import datetime


def calculate_control_score(entity: str, engine2_result: dict, 
                            engine3_result: dict, formation_result: dict = None) -> dict:
    """
    Calculates Narrative Control Score (0-100).
    
    Inputs:
      - engine2_result: Output from engine_understanding.py (sentiment, momentum, etc.)
      - engine3_result: Output from engine_actors.py (primary_driver, actors, etc.)
      - formation_result: Output from engine_formation.py (if available)
    
    Returns:
      - Full control score breakdown + interpretation
    """
    
    # ── Factor 1: Origin (0-25 points) ──────────────────────────────────────
    # Internal = brand controls the facts (higher control)
    # External = brand is reacting (lower control)
    origin_type = "external"  # Default
    if formation_result and formation_result.get("origin_type"):
        origin_type = formation_result.get("origin_type")
    
    if origin_type == "internal":
        origin_score = 20
    elif origin_type == "hybrid":
        origin_score = 12
    else:
        origin_score = 5
    
    # ── Factor 2: Spread Velocity (0-25 points) ─────────────────────────────
    # Based on Engine 2 momentum
    momentum = engine2_result.get("momentum", "stable")
    velocity_scores = {
        "improving": 20,
        "stable": 15,
        "declining": 8
    }
    velocity_score = velocity_scores.get(momentum, 12)
    
    # ── Factor 3: Actor Rigidity (0-25 points) ──────────────────────────────
    # Are the primary narrative drivers persuadable?
    # Based on Engine 3 primary_driver_type
    primary_driver_type = engine3_result.get("primary_driver_type", "news")
    actor_scores = {
        "news": 18,      # Can approach with facts/exclusives
        "youtube": 14,   # Can pitch counter-narrative content
        "forum": 8,      # Harder to influence directly
        "social": 5      # Lowest control, viral dynamics
    }
    rigidity_score = actor_scores.get(primary_driver_type, 10)
    
    # ── Factor 4: Defender Presence (0-25 points) ───────────────────────────
    # Are credible third parties speaking positively?
    # Based on Engine 3 defenders list
    defenders = engine3_result.get("defenders", [])
    defender_count = len(defenders) if isinstance(defenders, list) else 0
    
    if defender_count >= 3:
        defender_score = 22
    elif defender_count == 2:
        defender_score = 16
    elif defender_count == 1:
        defender_score = 10
    else:
        defender_score = 3
    
    # ── Total Score & Window ────────────────────────────────────────────────
    total = origin_score + velocity_score + rigidity_score + defender_score
    
    # Clamp to 0-100
    total = max(0, min(100, total))
    
    # Determine intervention window
    if total >= 70:
        window = "open"
    elif total >= 45:
        window = "narrowing"
    else:
        window = "closed"
    
    # Generate interpretation
    interpretation = _interpret_control(total)
    
    # Build result object
    result = {
        "narrative_control_score": total,
        "control_breakdown": {
            "origin": origin_score,
            "velocity": velocity_score,
            "actor_rigidity": rigidity_score,
            "defender_presence": defender_score
        },
        "intervention_window": window,
        "control_interpretation": interpretation,
        "calculated_at": datetime.utcnow().isoformat()
    }
    
    # Save to database
    database.save_control_score(
        entity=entity,
        control_score=total,
        origin_score=origin_score,
        velocity_score=velocity_score,
        rigidity_score=rigidity_score,
        defender_score=defender_score,
        intervention_window=window
    )
    
    return result


def _interpret_control(score: int) -> str:
    if score >= 75:
        return "Strong control. Narrative is shapeable. Proactive moves will land well."
    elif score >= 55:
        return "Moderate control. Window is open but closing. Act within 24-48 hours."
    elif score >= 35:
        return "Weak control. Narrative partially set. Focus on third-party voices, not direct response."
    else:
        return "Minimal control. Narrative is consolidated against you. Containment, not redirection."


# ── Local Test Block (Optional) ─────────────────────────────────────────────
if __name__ == "__main__":
    # Quick test to ensure imports work
    print("[Engine 6] Control Score module loaded successfully")
    
    # Mock data for testing
    mock_e2 = {"momentum": "declining"}
    mock_e3 = {"primary_driver_type": "news", "defenders": ["Partner A"]}
    mock_f = {"origin_type": "external"}
    
    result = calculate_control_score("TestEntity", mock_e2, mock_e3, mock_f)
    print(f"[Test] Control Score: {result['narrative_control_score']}/100")
    print(f"[Test] Window: {result['intervention_window']}")