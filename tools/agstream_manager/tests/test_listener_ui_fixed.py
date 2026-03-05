"""
Diagnostic Test Script for AGStream Manager Listener UI

This script tests the unified AGStream Manager service on port 5003.
All functionality (schemas, functions, listeners) is now in one service.
"""

import json
from typing import Any, Dict

import requests

# Configuration - Unified service on port 5003
API_BASE_URL = "http://localhost:5003"
SCHEMA_API_URL = f"{API_BASE_URL}/api/schemas"
TRANSDUCTION_API_URL = f"{API_BASE_URL}/api/transductions"


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def test_endpoint(
    name: str, url: str, method: str = "GET", data: Dict[str, Any] | None = None
) -> Dict[str, Any]:
    """Test an API endpoint and return the response."""
    print(f"\n🔍 Testing: {name}")
    print(f"   URL: {url}")
    print(f"   Method: {method}")

    try:
        if method == "GET":
            response = requests.get(url, timeout=5)
        elif method == "POST":
            response = requests.post(url, json=data, timeout=5)
        elif method == "DELETE":
            response = requests.delete(url, timeout=5)
        else:
            print(f"   ❌ Unsupported method: {method}")
            return {}

        print(f"   Status: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ Success")
            print(f"   Response: {json.dumps(result, indent=2)[:500]}...")
            return result
        else:
            print(f"   ❌ Error: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return {}

    except requests.exceptions.ConnectionError:
        print(f"   ❌ Connection Error: Cannot connect to {url}")
        print(f"   💡 Make sure the service is running")
        return {}
    except requests.exceptions.Timeout:
        print(f"   ❌ Timeout: Request took too long")
        return {}
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return {}


def diagnose_listener_ui():
    """Run diagnostic tests for the listener UI."""

    print_section("AGStream Manager Listener UI Diagnostics")
    print("\nThe AGStream Manager is a unified service on port 5003.")
    print("It handles schemas, functions, and listeners in one place.\n")

    # Test Unified Service Health
    print_section("Unified AGStream Manager Service (Port 5003)")

    service_health = test_endpoint("Service Health Check", f"{API_BASE_URL}/health")

    if not service_health:
        print("\n❌ CRITICAL: AGStream Manager Service is not running on port 5003!")
        print("   Please start it with:")
        print("   ./scripts/manage_agstream.sh start")
        return

    # Test endpoints that the HTML actually uses
    print_section("Test 1: Types/Schemas")
    types = test_endpoint("List Types", f"{SCHEMA_API_URL}/types")

    if types and types.get("types"):
        print(f"\n   ✅ Found {len(types['types'])} type(s)")
        for type_info in types["types"][:5]:
            print(f"      - {type_info['name']}")
    else:
        print("\n   ⚠️  WARNING: No types found!")
        print("   Create types in the '📝 Define Types' tab")

    print_section("Test 2: Transduction Functions")
    functions = test_endpoint(
        "List Functions", f"{SCHEMA_API_URL}/transductions/functions"
    )

    if functions and functions.get("functions"):
        print(f"\n   ✅ Found {len(functions['functions'])} transduction(s)")
        for func in functions["functions"]:
            print(
                f"      - {func['name']}: {func['source_type']} → {func['target_type']}"
            )
    else:
        print("\n   ⚠️  WARNING: No transductions found!")
        print("   Create transductions in the '⚡ Define Transductions' tab")

    print_section("Test 3: Kafka Topics")
    topics = test_endpoint("List Topics", f"{SCHEMA_API_URL}/transductions/topics")

    if topics and topics.get("topics"):
        print(f"\n   ✅ Found {len(topics['topics'])} topic(s)")
        for topic in topics["topics"][:10]:
            print(f"      - {topic}")
    else:
        print("\n   ⚠️  WARNING: No Kafka topics found!")
        print("   Create topics in the '📨 Manage Topics' tab")

    print_section("Test 4: Active Listeners")
    listeners = test_endpoint(
        "List Listeners", f"{SCHEMA_API_URL}/transductions/listeners"
    )

    if listeners and listeners.get("listeners"):
        print(f"\n   ✅ Found {len(listeners['listeners'])} active listener(s)")
        for listener in listeners["listeners"]:
            print(
                f"      - {listener['function_name']}: {listener['input_topic']} → {listener['output_topic']}"
            )
    else:
        print("\n   ℹ️  No active listeners (normal if none started yet)")

    # Summary
    print_section("Summary & Recommendations")

    issues = []
    if not service_health:
        issues.append("❌ AGStream Manager Service not running (port 5003)")
    if not types or not types.get("types"):
        issues.append("⚠️  No types defined")
    if not functions or not functions.get("functions"):
        issues.append("⚠️  No transductions defined")
    if not topics or not topics.get("topics"):
        issues.append("⚠️  No Kafka topics available")

    if not issues:
        print("\n✅ All checks passed! The listener UI should work correctly.")
        print("\nTo create a new listener:")
        print("1. Open the AGStream Manager UI in your browser")
        print("2. Go to the '🎧 Manage Listeners' tab")
        print("3. Click '➕ Start New Listener'")
        print("4. Enter a listener name")
        print("5. Select a transduction function")
        print("6. Select input and output topics")
        print("7. Click '▶️ Create & Start Listener'")
    else:
        print("\n⚠️  Issues found:")
        for issue in issues:
            print(f"   {issue}")

        print("\n📋 Steps to fix:")
        if not service_health:
            print("   1. Start AGStream Manager Service:")
            print("      ./scripts/manage_agstream.sh start")
        if not types or not types.get("types"):
            print("   2. Define types in the '📝 Define Types' tab")
        if not topics or not topics.get("topics"):
            print("   4. Create Kafka topics in the '📨 Manage Topics' tab")
        if not functions or not functions.get("functions"):
            print("   5. Create transductions in the '⚡ Define Transductions' tab")
        print("   6. Then create listeners in the '🎧 Manage Listeners' tab")

    print("\n" + "=" * 70)
    print("\n💡 KEY FINDING:")
    print("   The agstream_manager.html file is configured to use port 5003")
    print("   (Schema Manager Service), NOT port 5002 (Transduction Manager).")
    print("   Make sure you're running the Schema Manager Service!")
    print("=" * 70)


if __name__ == "__main__":
    try:
        diagnose_listener_ui()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()

# Made with Bob
