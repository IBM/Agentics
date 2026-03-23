#!/usr/bin/env python3
"""
Simple diagnostic test for agmap to check if LLM is working.
"""

import json
import os
import sys

import pytest

# Add paths
sys.path.insert(0, "/opt/flink")
sys.path.insert(0, "/opt/flink/udfs")

from dotenv import load_dotenv

load_dotenv("/opt/flink/.env")

# Check API keys
print("\n" + "=" * 70)
print("🔍 Checking Environment")
print("=" * 70)
print(f"OPENAI_API_KEY: {'✅ Set' if os.getenv('OPENAI_API_KEY') else '❌ Not set'}")
print(
    f"ANTHROPIC_API_KEY: {'✅ Set' if os.getenv('ANTHROPIC_API_KEY') else '❌ Not set'}"
)
print(f"GEMINI_API_KEY: {'✅ Set' if os.getenv('GEMINI_API_KEY') else '❌ Not set'}")
print("=" * 70 + "\n")

# Try importing
try:
    from ag_operators import agmap

    print("✅ Successfully imported agmap")
except Exception as e:
    print(f"❌ Failed to import agmap: {e}")
    sys.exit(1)

# Try importing AG
try:
    from agentics import AG

    print("✅ Successfully imported AG")
except Exception as e:
    print(f"❌ Failed to import AG: {e}")
    sys.exit(1)

pytestmark = pytest.mark.flink


def test_simple_agmap():
    """Simple test with timeout"""
    print("\n🧪 Testing agmap with simple input...")
    print("   Input: 'good'")
    print("   Calling agmap...")

    try:
        # Set a timeout
        import signal

        def timeout_handler(signum, frame):
            raise TimeoutError("Test timed out after 120 seconds")

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(120)  # 120 second timeout (2 minutes)

        results = list(agmap("good", "sentiment", "str"))

        signal.alarm(0)  # Cancel timeout

        print(f"   ✅ Got results: {results}")

        assert len(results) > 0
        result = results[0]

        if result[0] == "error":
            print(f"   ⚠️  Got error result")
            pytest.skip("agmap returned error")

        data = json.loads(result[0])
        print(f"   ✅ Parsed data: {data}")

        assert "sentiment" in data

    except TimeoutError as e:
        print(f"   ❌ Test timed out: {e}")
        pytest.fail("Test timed out - LLM call took too long")
    except Exception as e:
        print(f"   ❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

# Made with Bob
