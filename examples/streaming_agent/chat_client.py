import logging
import os

from atypes import AgentReply, ChatInput, ConversationHistory, UserMessage
from dotenv import load_dotenv
from openai import timeout

from agentics.core.streaming import AGStream

load_dotenv()

# Suppress Kafka consumer error logs
logging.getLogger("kafka.consumer.fetcher").setLevel(logging.CRITICAL)
logging.getLogger("kafka").setLevel(logging.WARNING)

kafka_server = os.getenv("KAFKA_SERVER") or "localhost:9092"
input_topic = os.getenv("KAFKA_INPUT_TOPIC") or "agentics-chat_input"
output_topic = os.getenv("KAFKA_OUTPUT_TOPIC") or "agentics-chat-output"
if not AGStream.topic_exists(input_topic):
    AGStream.create_topic(input_topic)
if not AGStream.topic_exists(output_topic):
    AGStream.create_topic(output_topic)


import uuid

conversation_history = ConversationHistory(history=[])

# Create a persistent group ID for this session to track offset
import uuid

session_group_id = f"chat-client-session-{uuid.uuid4()}"

while True:
    # Create input AGStream instance
    input_ag = AGStream(
        atype=ChatInput,
        input_topic=input_topic,
        kafka_server=kafka_server,
        instructions="Reply to the user's message",
    )

    # Get user input
    user_input_text = str(input("USER> "))
    user_message = UserMessage(user_message=user_input_text)

    # Create chat input with user message and conversation history
    chat_input = ChatInput(
        user_message=user_message, conversation_history=conversation_history
    )

    # Add to states and produce to Kafka
    input_ag.states.append(chat_input)
    input_ag.produce()

    # Create a collector instance to receive only the latest reply
    output_ag = AGStream(
        atype=AgentReply, input_topic=output_topic, kafka_server=kafka_server
    )

    # Use the new simple method to get the latest message
    latest_answer = output_ag.collect_latest_source(timeout_seconds=30)

    # Display the reply
    print("\nAGENT:")
    reply_received = False
    if latest_answer and latest_answer.states and len(latest_answer.states) > 0:
        first_state = latest_answer.states[0]  # Get the first state
        if hasattr(first_state, "reply") and first_state.reply:
            print(first_state.reply)
            # Update conversation history with Pydantic models
            agent_reply = AgentReply(reply=first_state.reply)
            conversation_history.history.append((user_message, agent_reply))
            reply_received = True

    if not reply_received:
        print("(No reply received within timeout)")


# Made with Bob
