"""
Send a test message to the agentics-chat-input topic to test multi-listener
"""

import json
import time

from kafka import KafkaProducer

# Create a simple message
message = {"content": "Hello, this is a test message for multi-agent processing!"}

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Sending test message to agentics-chat-input...")
producer.send("agentics-chat-input", value=message)
producer.flush()
print("✓ Message sent!")
print(f"Message content: {message['content']}")

# Made with Bob
