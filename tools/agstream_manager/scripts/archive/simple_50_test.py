#!/usr/bin/env python3
"""
Simple test: Process 50 reviews directly with timing
"""
import sys
import time

sys.path.insert(0, "/opt/flink/agentics/src")

from agentics.core.streaming.agstream_sql import agmap_row

# Sample reviews
reviews = [
    "I absolutely love this product! The quality exceeded my expectations.",
    "Outstanding purchase! This is exactly what I needed.",
    "Fantastic product! Highly recommend to everyone.",
    "Best purchase I've made this year. Five stars!",
    "Incredible quality and fast shipping. Very satisfied!",
    "The product is okay, does what it's supposed to do.",
    "It's decent. Nothing special but gets the job done.",
    "Average product. Meets basic requirements.",
    "Fair quality for the price. No complaints.",
    "Standard product. Works as described.",
    "Very disappointed with this purchase. Poor quality.",
    "Terrible product. Broke after two days of use.",
    "Would not recommend. Complete waste of money.",
    "Poor quality and bad customer service.",
    "Disappointed. Does not match the description at all.",
] * 4  # Repeat to get ~60 reviews

# Take first 50
reviews = reviews[:50]

print("=" * 80)
print(f"PROCESSING {len(reviews)} REVIEWS WITH SENTIMENT ANALYSIS")
print("=" * 80)

prompt = "Analyze the sentiment of this product review. Return only one word: positive, negative, or neutral."

start_time = time.time()
results = []

for i, review in enumerate(reviews, 1):
    review_start = time.time()

    try:
        sentiment = agmap_row(review, prompt)
        review_time = time.time() - review_start

        results.append({"review_num": i, "sentiment": sentiment, "time": review_time})

        if i % 10 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed
            print(
                f"[{elapsed:.1f}s] Processed {i}/{len(reviews)} reviews ({rate:.2f} reviews/sec)"
            )

    except Exception as e:
        print(f"Error processing review {i}: {e}")
        results.append(
            {"review_num": i, "sentiment": "ERROR", "time": time.time() - review_start}
        )

total_time = time.time() - start_time

print("\n" + "=" * 80)
print("RESULTS")
print("=" * 80)
print(f"Total reviews: {len(reviews)}")
print(f"Successful: {len([r for r in results if r['sentiment'] != 'ERROR'])}")
print(f"Errors: {len([r for r in results if r['sentiment'] == 'ERROR'])}")
print(f"Total time: {total_time:.2f} seconds")
print(f"Average rate: {len(reviews)/total_time:.2f} reviews/second")
print(f"Average time per review: {total_time/len(reviews):.2f} seconds")

# Show sentiment distribution
sentiments = [r["sentiment"] for r in results if r["sentiment"] != "ERROR"]
print(f"\nSentiment distribution:")
print(f"  Positive: {sentiments.count('positive')}")
print(f"  Negative: {sentiments.count('negative')}")
print(f"  Neutral: {sentiments.count('neutral')}")

print("=" * 80)

# Made with Bob
