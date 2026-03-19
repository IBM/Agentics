#!/usr/bin/env python3
"""
Sentiment Analysis Example using Generic Semantic UDF
======================================================

This example demonstrates how to use the generic semantic UDF to perform
sentiment analysis on a dataframe of text messages.
"""

import json
import sys
from pathlib import Path

import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from generic_semantic_udf import semantic_transform


def main():
    """Run sentiment analysis example."""

    print("=" * 60)
    print("Sentiment Analysis Example")
    print("=" * 60)

    # Sample data: customer reviews
    reviews_data = pd.DataFrame(
        {
            "review_id": [1, 2, 3, 4, 5],
            "text": [
                "This product exceeded my expectations! Highly recommend.",
                "Terrible quality. Broke after one week of use.",
                "It works okay, nothing special but gets the job done.",
                "Amazing! Best purchase I made this year.",
                "Disappointed with the customer service and product quality.",
            ],
            "product": ["Widget A", "Widget B", "Widget A", "Widget C", "Widget B"],
        }
    )

    print("\n📊 Input Data:")
    print(reviews_data)

    # Perform sentiment analysis using map operation
    print("\n🔄 Performing sentiment analysis...")

    result = semantic_transform(
        input_data=json.dumps(reviews_data.to_dict("records")),
        operation="map",
        target_type_name="Sentiment",
        instructions="""
        Analyze the sentiment of each review text.
        Provide:
        - label: POSITIVE, NEGATIVE, or NEUTRAL
        - confidence: score between 0 and 1
        - reasoning: brief explanation of the sentiment
        """,
        merge_output=True,
    )

    print("\n✅ Results:")
    print(result)

    # Analyze results
    if not result.empty and "label" in result.columns:
        print("\n📈 Summary Statistics:")
        sentiment_counts = result["label"].value_counts()
        print(sentiment_counts)

        print("\n🎯 Average Confidence:")
        if "confidence" in result.columns:
            avg_confidence = result["confidence"].mean()
            print(f"  {avg_confidence:.2%}")

    # Save results
    output_file = "sentiment_results.csv"
    result.to_csv(output_file, index=False)
    print(f"\n💾 Results saved to: {output_file}")


if __name__ == "__main__":
    main()

# Made with Bob
