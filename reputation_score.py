def calculate_score(sentiment):

    positive = sentiment["positive"]
    negative = sentiment["negative"]
    neutral = sentiment["neutral"]

    total = positive + negative + neutral

    if total == 0:
        return 50

    score = ((positive - negative) / total) * 100

    final_score = round(50 + score)

    if final_score < 0:
        final_score = 0
    if final_score > 100:
        final_score = 100

    return final_score