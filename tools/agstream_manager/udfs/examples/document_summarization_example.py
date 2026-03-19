#!/usr/bin/env python3
"""
Document Summarization Example using Generic Semantic UDF
==========================================================

This example demonstrates how to use the reduce operation to summarize
multiple documents into a single comprehensive summary.
"""

import json
import sys
from pathlib import Path

import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from generic_semantic_udf import semantic_transform


def main():
    """Run document summarization example."""

    print("=" * 60)
    print("Document Summarization Example")
    print("=" * 60)

    # Sample data: multiple article excerpts
    articles_data = pd.DataFrame(
        {
            "article_id": [1, 2, 3, 4],
            "content": [
                "Recent studies show that AI adoption in healthcare has increased by 40% this year. "
                "Hospitals are using machine learning for diagnosis and treatment planning.",
                "The healthcare AI market is expected to reach $45 billion by 2026. "
                "Major investments are flowing into medical imaging and drug discovery applications.",
                "Privacy concerns remain a challenge for AI in healthcare. "
                "Regulations like HIPAA require careful handling of patient data in AI systems.",
                "Clinical trials using AI have shown promising results in early disease detection. "
                "AI algorithms can identify patterns that human doctors might miss.",
            ],
            "source": [
                "Journal A",
                "Market Report",
                "Privacy Review",
                "Clinical Study",
            ],
        }
    )

    print("\n📚 Input Documents:")
    for idx, row in articles_data.iterrows():
        print(f"\n{idx + 1}. {row['source']}:")
        print(f"   {row['content'][:100]}...")

    # Perform summarization using reduce operation
    print("\n🔄 Creating comprehensive summary...")

    result = semantic_transform(
        input_data=json.dumps(articles_data.to_dict("records")),
        operation="reduce",
        target_type_name="Summary",
        instructions="""
        Create a comprehensive summary that:
        1. Captures the main themes across all documents
        2. Highlights key statistics and findings
        3. Identifies common trends
        4. Lists the most important points
        Keep the summary concise but informative.
        """,
    )

    print("\n✅ Summary Result:")
    print(result)

    if not result.empty:
        if "summary" in result.columns:
            print("\n📝 Generated Summary:")
            print(result["summary"].iloc[0])

        if "key_points" in result.columns:
            print("\n🎯 Key Points:")
            key_points = result["key_points"].iloc[0]
            if isinstance(key_points, list):
                for i, point in enumerate(key_points, 1):
                    print(f"  {i}. {point}")

        if "word_count" in result.columns:
            print(f"\n📊 Summary Word Count: {result['word_count'].iloc[0]}")

    # Save results
    output_file = "summary_results.json"
    result.to_json(output_file, orient="records", indent=2)
    print(f"\n💾 Results saved to: {output_file}")


if __name__ == "__main__":
    main()

# Made with Bob
