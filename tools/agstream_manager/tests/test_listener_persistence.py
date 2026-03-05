"""
Test if listeners are being persisted and retrieved correctly
"""

import json
import time

import requests

API_URL = "http://localhost:5003/api/transductions"


def test_listener_persistence():
    """Test the full listener lifecycle"""

    print("=" * 70)
    print("Testing Listener Persistence")
    print("=" * 70)

    # Step 1: Get current listeners
    print("\n1. Checking current listeners...")
    response = requests.get(f"{API_URL}/listeners")
    initial_listeners = response.json()
    print(f"   Current listeners: {len(initial_listeners.get('listeners', []))}")

    if initial_listeners.get("listeners"):
        print("   Existing listeners:")
        for listener in initial_listeners["listeners"]:
            print(f"      - {listener['listener_id']}: {listener['function_name']}")
            print(f"        Status: {listener.get('status')}")
            print(
                f"        Input: {listener['input_topic']} → Output: {listener['output_topic']}"
            )

    # Step 2: Get functions
    print("\n2. Getting available functions...")
    response = requests.get(f"{API_URL}/functions")
    functions = response.json()

    if not functions.get("functions"):
        print("   ❌ No functions available. Create one first!")
        return

    function_name = functions["functions"][0]["name"]
    print(f"   Using function: {function_name}")

    # Step 3: Get topics
    print("\n3. Getting available topics...")
    response = requests.get(f"{API_URL}/topics")
    topics = response.json()

    if not topics.get("topics") or len(topics["topics"]) < 2:
        print("   ❌ Need at least 2 topics!")
        return

    input_topic = topics["topics"][0]
    output_topic = topics["topics"][1]
    print(f"   Using topics: {input_topic} → {output_topic}")

    # Step 4: Create a listener
    print("\n4. Creating a new listener...")
    payload = {
        "function_name": function_name,
        "input_topic": input_topic,
        "output_topic": output_topic,
        "lookback": 0,
    }

    response = requests.post(f"{API_URL}/listeners", json=payload)
    result = response.json()

    if response.status_code in [200, 201] and result.get("success"):
        listener_id = result["listener_id"]
        print(f"   ✅ Listener created: {listener_id}")
    else:
        print(f"   ❌ Failed to create listener: {result}")
        return

    # Step 5: Immediately check if it appears in the list
    print("\n5. Checking if listener appears in list (immediately)...")
    time.sleep(0.5)  # Small delay
    response = requests.get(f"{API_URL}/listeners")
    updated_listeners = response.json()

    print(f"   Total listeners now: {len(updated_listeners.get('listeners', []))}")

    found = False
    if updated_listeners.get("listeners"):
        print("   Current listeners:")
        for listener in updated_listeners["listeners"]:
            print(f"      - {listener['listener_id']}: {listener['function_name']}")
            print(f"        Status: {listener.get('status')}")
            if listener["listener_id"] == listener_id:
                found = True
                print(f"        ✅ FOUND our new listener!")

    if not found:
        print(f"   ❌ Listener {listener_id} NOT found in the list!")
        print("   This is the problem - listener is created but not showing up!")

    # Step 6: Check the persistence file
    print("\n6. Checking persistence file...")
    import os

    persistence_file = "examples/streaming_agent/transduction_listeners.json"

    if os.path.exists(persistence_file):
        with open(persistence_file, "r") as f:
            persisted_data = json.load(f)
        print(f"   ✅ Persistence file exists")
        print(f"   Persisted listeners: {len(persisted_data)}")
        if listener_id in persisted_data:
            print(f"   ✅ Our listener {listener_id} is persisted!")
        else:
            print(f"   ❌ Our listener {listener_id} is NOT in persistence file!")
    else:
        print(f"   ❌ Persistence file not found at {persistence_file}")

    # Step 7: Check listener logs
    print(f"\n7. Checking listener logs for {listener_id}...")
    response = requests.get(f"{API_URL}/listeners/{listener_id}/logs")

    if response.status_code == 200:
        logs = response.json()
        print(f"   ✅ Logs endpoint accessible")
        print(f"   Log lines: {len(logs.get('logs', []))}")
        if logs.get("logs"):
            print("   Recent logs:")
            for log in logs["logs"][:5]:
                print(f"      {log.strip()}")
    else:
        print(f"   ❌ Cannot access logs: {response.status_code}")

    print("\n" + "=" * 70)
    print("DIAGNOSIS:")
    print("=" * 70)

    if found:
        print("✅ Listener is working correctly!")
        print("   - Created successfully")
        print("   - Appears in the list")
        print("   - Should be visible in the UI")
    else:
        print("❌ PROBLEM IDENTIFIED:")
        print("   - Listener is created (returns 201)")
        print("   - But does NOT appear in GET /api/listeners")
        print("   - This is why the UI shows nothing!")
        print("\n   Possible causes:")
        print("   1. listeners_store is not being updated correctly")
        print("   2. GET endpoint is reading from wrong source")
        print("   3. Listener is being created but immediately removed")
        print("\n   Check the transduction_manager_service.py terminal for errors!")


if __name__ == "__main__":
    test_listener_persistence()

# Made with Bob
