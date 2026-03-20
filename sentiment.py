from textblob import TextBlob

def analyze_sentiment(posts):

    positive = 0
    negative = 0
    neutral = 0

    for post in posts:

        analysis = TextBlob(post)
        polarity = analysis.sentiment.polarity

        if polarity > 0:
            positive += 1
        elif polarity < 0:
            negative += 1
        else:
            neutral += 1

    return {
        "positive": positive,
        "negative": negative,
        "neutral": neutral
    }