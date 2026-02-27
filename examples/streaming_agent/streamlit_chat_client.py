"""
Streamlit Chat Client for AGStream with Schema Registry Support
A web-based chat interface that communicates with the AGStream listener via Kafka.
Supports dynamic target type selection from Schema Registry.

Run with: streamlit run examples/streaming_agent/streamlit_chat_client.py
"""

import logging
import os
import sys
import uuid
from typing import List, Optional

import requests
import streamlit as st
from dotenv import load_dotenv

# Add parent directory to path to import atypes
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from atypes import AgentReply, ChatInput, ConversationHistory, UserMessage

from agentics.core.streaming import AGStream

# Load environment variables
load_dotenv()

# Suppress Kafka consumer error logs
logging.getLogger("kafka.consumer.fetcher").setLevel(logging.CRITICAL)
logging.getLogger("kafka").setLevel(logging.WARNING)

# Kafka configuration
kafka_server = os.getenv("KAFKA_SERVER") or "localhost:9092"
input_topic = os.getenv("KAFKA_INPUT_TOPIC") or "agentics-chat-input"
output_topic = os.getenv("KAFKA_OUTPUT_TOPIC") or "agentics-chat-output"
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL") or "http://localhost:8081"


def get_available_schemas() -> List[str]:
    """Fetch available schemas from Schema Registry"""
    try:
        response = requests.get(f"{schema_registry_url}/subjects")
        if response.status_code == 200:
            subjects = response.json()
            # Filter for value schemas (exclude key schemas)
            return [s.replace("-value", "") for s in subjects if s.endswith("-value")]
        return []
    except Exception as e:
        st.error(f"Error fetching schemas: {e}")
        return []


def get_schema_info(topic: str) -> Optional[dict]:
    """Get schema information for a topic"""
    try:
        subject = f"{topic}-value"
        response = requests.get(
            f"{schema_registry_url}/subjects/{subject}/versions/latest"
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


# Ensure topics exist
if not AGStream.topic_exists(input_topic):
    AGStream.create_topic(input_topic)
if not AGStream.topic_exists(output_topic):
    AGStream.create_topic(output_topic)

# Page configuration
st.set_page_config(page_title="AGStream Chat", page_icon="💬", layout="wide")

# Initialize session state
if "conversation_history" not in st.session_state:
    st.session_state.conversation_history = ConversationHistory(history=[])

if "messages" not in st.session_state:
    st.session_state.messages = []

if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

# Title and description
st.title("💬 AGStream Chat Client")
st.markdown("Chat with an AI agent powered by AGStream and Kafka")

# Sidebar with configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    st.text(f"Kafka Server: {kafka_server}")
    st.text(f"Input Topic: {input_topic}")
    st.text(f"Output Topic: {output_topic}")
    st.text(f"Schema Registry: {schema_registry_url}")
    st.text(f"Session ID: {st.session_state.session_id[:8]}...")

    st.markdown("---")
    st.markdown("### 🎯 Target Type Selection")

    # Schema Registry integration
    use_schema_registry = st.checkbox(
        "Use Schema Registry", value=False, help="Load target type from Schema Registry"
    )

    if use_schema_registry:
        available_schemas = get_available_schemas()
        if available_schemas:
            selected_schema = st.selectbox(
                "Select Target Type",
                options=available_schemas,
                help="Choose a registered schema as the target type",
            )

            if selected_schema:
                schema_info = get_schema_info(selected_schema)
                if schema_info:
                    st.success(f"✓ Schema loaded: {selected_schema}")
                    with st.expander("📋 Schema Details"):
                        st.json(schema_info)

                    # Store selected schema in session state
                    if (
                        "selected_target_schema" not in st.session_state
                        or st.session_state.selected_target_schema != selected_schema
                    ):
                        st.session_state.selected_target_schema = selected_schema
                        st.session_state.target_atype = (
                            None  # Will be loaded dynamically
                        )
        else:
            st.warning("No schemas found in registry")
    else:
        st.info("Using default AgentReply type")
        if "selected_target_schema" in st.session_state:
            del st.session_state.selected_target_schema

    st.markdown("---")
    timeout = st.slider("Response Timeout (seconds)", 5, 60, 30)

    if st.button("🗑️ Clear Chat History"):
        st.session_state.messages = []
        st.session_state.conversation_history = ConversationHistory(history=[])
        st.rerun()

    st.markdown("---")
    st.markdown("### 📊 Stats")
    st.metric(
        "Messages Sent",
        len(st.session_state.messages) // 2 if st.session_state.messages else 0,
    )

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Type your message here..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)

    # Create and send message to Kafka
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        message_placeholder.markdown("🤔 Thinking...")

        try:
            # Create input AGStream instance
            input_ag = AGStream(
                atype=ChatInput,
                input_topic=input_topic,
                kafka_server=kafka_server,
                instructions="Reply to the user's message",
            )

            # Create user message
            user_message = UserMessage(user_message=prompt)

            # Create chat input with user message and conversation history
            chat_input = ChatInput(
                user_message=user_message,
                conversation_history=st.session_state.conversation_history,
            )

            # Add to states and produce to Kafka
            input_ag.states.append(chat_input)
            input_ag.produce()

            # Determine target atype (from schema registry or default)
            target_atype = AgentReply
            if "selected_target_schema" in st.session_state:
                # Load atype from schema registry using static method (no instance needed!)
                loaded_atype = AGStream.get_atype_from_registry_static(
                    topic=st.session_state.selected_target_schema,
                    schema_registry_url=schema_registry_url,
                    is_key=False,
                )
                if loaded_atype:
                    target_atype = loaded_atype
                    message_placeholder.markdown(
                        f"🎯 Using target type from registry: {st.session_state.selected_target_schema}"
                    )

            # Create a collector instance to receive the reply
            output_ag = AGStream(
                atype=target_atype,
                input_topic=output_topic,
                kafka_server=kafka_server,
                schema_registry_url=schema_registry_url,
            )

            # Get the latest message with timeout
            latest_answer = output_ag.collect_latest_source(timeout_seconds=timeout)

            # Process the reply
            reply_text = None
            if latest_answer and latest_answer.states and len(latest_answer.states) > 0:
                first_state = latest_answer.states[0]

                # Try to extract reply text from various possible field names
                if hasattr(first_state, "reply") and first_state.reply:
                    reply_text = first_state.reply
                elif hasattr(first_state, "response") and first_state.response:
                    reply_text = first_state.response
                elif hasattr(first_state, "answer") and first_state.answer:
                    reply_text = first_state.answer
                else:
                    # Fallback: show all fields
                    reply_text = str(first_state.model_dump())

                if reply_text:
                    # Update conversation history
                    agent_reply = AgentReply(reply=reply_text)
                    st.session_state.conversation_history.history.append(
                        (user_message, agent_reply)
                    )

            # Display the reply
            if reply_text:
                message_placeholder.markdown(reply_text)
                st.session_state.messages.append(
                    {"role": "assistant", "content": reply_text}
                )
            else:
                error_msg = f"⚠️ No reply received within {timeout} seconds. The agent might be processing or unavailable."
                message_placeholder.markdown(error_msg)
                st.session_state.messages.append(
                    {"role": "assistant", "content": error_msg}
                )

        except Exception as e:
            error_msg = f"❌ Error: {str(e)}"
            message_placeholder.markdown(error_msg)
            st.session_state.messages.append(
                {"role": "assistant", "content": error_msg}
            )

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 0.8em;'>
    Powered by AGStream | Built with Streamlit
    </div>
    """,
    unsafe_allow_html=True,
)

# Made with Bob
