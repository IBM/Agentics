#!/usr/bin/env python3
"""
Generate 50 sample product reviews and send them to Kafka
"""
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Sample review templates
positive_templates = [
    "I absolutely love this product! The quality exceeded my expectations.",
    "Outstanding purchase! This is exactly what I needed.",
    "Fantastic product! Highly recommend to everyone.",
    "Best purchase I've made this year. Five stars!",
    "Incredible quality and fast shipping. Very satisfied!",
]

neutral_templates = [
    "The product is okay, does what it's supposed to do.",
    "It's decent. Nothing special but gets the job done.",
    "Average product. Meets basic requirements.",
    "Fair quality for the price. No complaints.",
    "Standard product. Works as described.",
]

negative_templates = [
    "Very disappointed with this purchase. Poor quality.",
    "Terrible product. Broke after two days of use.",
    "Would not recommend. Complete waste of money.",
    "Poor quality and bad customer service.",
    "Disappointed. Does not match the description at all.",
]


def generate_reviews(count=50):
    """Generate a mix of positive, neutral, and negative reviews"""
    reviews = []

    for i in range(count):
        # Distribute: 40% positive, 30% neutral, 30% negative
        if i % 10 < 4:
            template = positive_templates[i % len(positive_templates)]
            sentiment = "positive"
        elif i % 10 < 7:
            template = neutral_templates[i % len(neutral_templates)]
            sentiment = "neutral"
        else:
            template = negative_templates[i % len(negative_templates)]
            sentiment = "negative"

        review = {
            "review_id": f"review_{i+1:03d}",
            "text": f"{template} (Review #{i+1})",
            "expected_sentiment": sentiment,
        }
        reviews.append(review)

    return reviews


def send_to_kafka(reviews, topic="product_reviews"):
    """Send reviews to Kafka topic"""
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Sending {len(reviews)} reviews to Kafka topic '{topic}'...")
    start_time = time.time()

    for i, review in enumerate(reviews, 1):
        try:
            future = producer.send(topic, value=review)
            future.get(timeout=10)  # Wait for confirmation
            if i % 10 == 0:
                print(f"Sent {i}/{len(reviews)} reviews...")
        except KafkaError as e:
            print(f"Error sending review {i}: {e}")

    producer.flush()
    producer.close()

    elapsed = time.time() - start_time
    print(f"\nCompleted! Sent {len(reviews)} reviews in {elapsed:.2f} seconds")
    print(f"Rate: {len(reviews)/elapsed:.2f} reviews/second")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate and send product reviews to Kafka"
    )
    parser.add_argument(
        "--count", type=int, default=50, help="Number of reviews to generate"
    )
    parser.add_argument(
        "--topic", type=str, default="product_reviews", help="Kafka topic name"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print reviews without sending to Kafka"
    )

    args = parser.parse_args()

    reviews = generate_reviews(args.count)

    if args.dry_run:
        print(f"Generated {len(reviews)} reviews (dry run):")
        for review in reviews[:5]:  # Show first 5
            print(f"  {review}")
        print(f"  ... and {len(reviews)-5} more")
    else:
        send_to_kafka(reviews, args.topic)

# Made with Bob
