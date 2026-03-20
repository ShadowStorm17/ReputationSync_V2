from engine_action import generate_playbook
import json

# Simulate what the full analyze endpoint would pass in
analysis = {
    'reputation_score': 35,
    'sentiment': {
        'label': 'negative',
        'reason': 'Regulatory issues and robotaxi safety concerns dominate coverage'
    },
    'narrative': {
        'current_story': 'Tesla is facing regulatory scrutiny over self-driving claims and robotaxi safety issues',
        'narrative_type': 'controversy',
        'momentum': 'stable'
    },
    'signals': {
        'crisis_indicators': ['Robotaxi crashes at 4x human rate', 'Suing California DMV'],
        'positive_signals': ['New AI investments', 'Cybertruck price cuts boosting sales']
    }
}

actors = {
    'top_actors': [
        {'name': 'Gizmodo', 'narrative_role': 'critic', 'what_they_say': 'Focuses on safety failures'},
        {'name': 'Yahoo Entertainment', 'narrative_role': 'neutral_reporter', 'what_they_say': 'High volume neutral coverage'}
    ],
    'narrative_breakdown': {
        'critics': ['Gizmodo', 'The Verge'],
        'defenders': []
    }
}

prediction = {
    'crisis_probability': 75,
    'trajectory': 'volatile',
    'risk_level': 'high',
    'alerts': [
        {'urgency': 'critical', 'description': 'Score dropped 24 points suddenly'}
    ],
    'forecast_7_days': 'Score likely to decline further to 30 without intervention',
    'recommendation': 'Immediate action required on safety narrative'
}

result = generate_playbook('Tesla', 'brand', analysis, actors, prediction)
print(json.dumps(result, indent=2))