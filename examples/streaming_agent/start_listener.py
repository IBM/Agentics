import os

from atypes import AgentReply, ChatInput
from dotenv import load_dotenv

from agentics.core.streaming import AGStream

load_dotenv()

kafka_server = os.getenv("KAFKA_SERVER") or "localhost:9092"
input_topic = os.getenv("KAFKA_INPUT_TOPIC") or "agentics-chat_input"
output_topic = os.getenv("KAFKA_OUTPUT_TOPIC") or "agentics-chat-output"
if not AGStream.topic_exists(input_topic):
    AGStream.create_topic(input_topic)
if not AGStream.topic_exists(output_topic):
    AGStream.create_topic(output_topic)


target = AGStream(
    atype=AgentReply,
    kafka_server=os.getenv("KAFKA_SERVER"),
    input_topic=os.getenv("KAFKA_INPUT_TOPIC"),
    output_topic=os.getenv("KAFKA_OUTPUT_TOPIC"),
)
target.listen(atype=ChatInput)
