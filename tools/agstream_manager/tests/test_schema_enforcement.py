"""
Test script for produce_with_schema_enforcement method.
Demonstrates schema registry enforcement for individual state production.
"""

import os
import sys

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
backend_path = os.path.join(os.path.dirname(__file__), "..", "backend")
sys.path.insert(0, backend_path)

from atypes import ChatInput, ConversationHistory, UserMessage
from dotenv import load_dotenv

from agentics.core.streaming.streaming import AGStream

load_dotenv()

kafka_server = os.getenv("KAFKA_SERVER") or "localhost:9092"
input_topic = "test-schema-enforcement"
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL") or "http://localhost:8081"

print("🧪 Testing produce_with_schema_enforcement")
print(f"   Topic: {input_topic}")
print(f"   Schema Registry: {schema_registry_url}")
print()

# Create AGStream instance
ag = AGStream(
    atype=ChatInput,
    input_topic=input_topic,
    kafka_server=kafka_server,
    schema_registry_url=schema_registry_url,
)

# Add states to the AG
user_message1 = UserMessage(user_message="First test message with schema enforcement")
user_message2 = UserMessage(user_message="Second test message")
conversation_history = ConversationHistory(history=[])

chat_input1 = ChatInput(
    user_message=user_message1, conversation_history=conversation_history
)
chat_input2 = ChatInput(
    user_message=user_message2, conversation_history=conversation_history
)

# Add states to AG
ag.states = [chat_input1, chat_input2]

print(f"📤 Producing {len(ag.states)} states with schema enforcement...")
try:
    # This will:
    # 1. Check if schema exists in registry
    # 2. Register it if missing (register_if_missing=True by default)
    # 3. Validate each state
    # 4. Produce each state to Kafka one-by-one
    message_ids = ag.produce_with_schema_enforcement(
        register_if_missing=True, compatibility_mode="BACKWARD"
    )

    if message_ids:
        print(f"\n✅ SUCCESS! Sent {len(message_ids)} messages")
        for idx, msg_id in enumerate(message_ids, 1):
            print(f"   {idx}. {msg_id}")
        print()
        print("Schema was registered and enforced!")
        print(
            f"Check registry at: {schema_registry_url}/subjects/{input_topic}-value/versions/latest"
        )
    else:
        print("❌ Failed to produce messages")

except ValueError as e:
    print(f"❌ Validation Error: {e}")
except Exception as e:
    print(f"❌ Error: {e}")

print()
print("🔍 Try producing with an invalid state...")
try:
    # This should fail because we're adding the wrong type
    from atypes import AgentReply

    invalid_state = AgentReply(reply="This is the wrong type!")

    ag.states = [invalid_state]  # Wrong type!
    ag.produce_with_schema_enforcement()
    print("❌ Should have failed but didn't!")

except ValueError as e:
    print(f"✅ Correctly rejected invalid state: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")

# Made with Bob
