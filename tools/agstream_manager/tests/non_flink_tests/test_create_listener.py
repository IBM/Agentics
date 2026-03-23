"""
Test script to simulate what happens when you click "Create & Start Listener" button
"""

import json

import requests

API_URL = "http://localhost:5003/api/transductions"


def test_create_listener():
    """Test creating a listener exactly as the UI does"""

    print("=" * 70)
    print("Testing Listener Creation (simulating UI button click)")
    print("=" * 70)

    # First, check what functions are available
    print("\n1. Checking available functions...")
    try:
        response = requests.get(f"{API_URL}/functions")
        functions_data = response.json()

        if not functions_data.get("functions"):
            print("   ❌ No functions available!")
            print("   You need to create a transduction function first.")
            return

        print(f"   ✅ Found {len(functions_data['functions'])} function(s):")
        for func in functions_data["functions"]:
            print(
                f"      - {func['name']}: {func['source_type']} → {func['target_type']}"
            )

        # Use the first function
        test_function = functions_data["functions"][0]
        function_name = test_function["name"]

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return

    # Check available topics
    print("\n2. Checking available topics...")
    try:
        response = requests.get(f"{API_URL}/topics")
        topics_data = response.json()

        if not topics_data.get("topics") or len(topics_data["topics"]) < 2:
            print("   ❌ Need at least 2 topics!")
            print("   Create topics in the UI first.")
            return

        print(f"   ✅ Found {len(topics_data['topics'])} topic(s):")
        for topic in topics_data["topics"][:5]:
            print(f"      - {topic}")

        input_topic = topics_data["topics"][0]
        output_topic = (
            topics_data["topics"][1]
            if len(topics_data["topics"]) > 1
            else topics_data["topics"][0] + "-output"
        )

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return

    # Now try to create a listener (exactly as the UI does)
    listener_name = f"test_{function_name.lower()}_listener"
    print("\n3. Creating listener...")
    print(f"   Listener Name: {listener_name}")
    print(f"   Function: {function_name}")
    print(f"   Input Topic: {input_topic}")
    print(f"   Output Topic: {output_topic}")
    print(f"   Lookback: 0")

    payload = {
        "listener_name": listener_name,
        "function_name": function_name,
        "input_topic": input_topic,
        "output_topic": output_topic,
        "lookback": 0,
    }

    print(f"\n   Sending POST to: {API_URL}/listeners")
    print(f"   Payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(
            f"{API_URL}/listeners",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10,
        )

        print(f"\n   Response Status: {response.status_code}")
        print(f"   Response Headers: {dict(response.headers)}")
        print(f"   Response Body:")

        try:
            data = response.json()
            print(json.dumps(data, indent=2))

            if response.status_code in [200, 201] and data.get("success"):
                print(f"\n   ✅ SUCCESS! Listener created: {data.get('listener_id')}")

                # Check if it's actually running
                print("\n4. Verifying listener is running...")
                response = requests.get(f"{API_URL}/listeners")
                listeners = response.json()

                if listeners.get("listeners"):
                    print(
                        f"   ✅ Found {len(listeners['listeners'])} active listener(s):"
                    )
                    for listener in listeners["listeners"]:
                        print(
                            f"      - {listener['listener_id']}: {listener['function_name']}"
                        )
                        print(f"        Status: {listener.get('status', 'unknown')}")
                        print(
                            f"        Input: {listener['input_topic']} → Output: {listener['output_topic']}"
                        )
                else:
                    print("   ⚠️  No listeners found in the list!")

            else:
                print(f"\n   ❌ FAILED: {data.get('error', 'Unknown error')}")

        except json.JSONDecodeError:
            print(f"   Raw response: {response.text}")

    except requests.exceptions.Timeout:
        print("   ❌ Request timed out!")
        print("   The service might be hanging or taking too long to respond.")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback

        traceback.print_exc()

    print("\n" + "=" * 70)


def check_service_logs():
    """Remind user to check service logs"""
    print("\n💡 IMPORTANT: Check the transduction_manager_service.py terminal output!")
    print("   Look for:")
    print("   - Any error messages")
    print("   - Stack traces")
    print("   - Listener creation attempts")
    print("   - Subprocess spawn messages")


if __name__ == "__main__":
    test_create_listener()
    check_service_logs()

# Made with Bob
