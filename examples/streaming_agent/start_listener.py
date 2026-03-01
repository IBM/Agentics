import os

from atypes import AgentReply, ChatInput
from dotenv import load_dotenv

from agentics.core.streaming import AGStream
from agentics.core.streaming_utils import create_kafka_topic, kafka_topic_exists

load_dotenv()

kafka_server = os.getenv("KAFKA_SERVER") or "localhost:9092"
input_topic = os.getenv("KAFKA_INPUT_TOPIC") or "agentics-chat_input"
output_topic = os.getenv("KAFKA_OUTPUT_TOPIC") or "agentics-chat-output"
if not kafka_topic_exists(input_topic):
    create_kafka_topic(input_topic)
if not kafka_topic_exists(output_topic):
    create_kafka_topic(output_topic)


target = AGStream(
    atype=AgentReply,
    kafka_server=os.getenv("KAFKA_SERVER"),
    input_topic=os.getenv("KAFKA_INPUT_TOPIC"),
    output_topic=os.getenv("KAFKA_OUTPUT_TOPIC"),
)
target.listen(source_atype_name=ChatInput.__name__)
