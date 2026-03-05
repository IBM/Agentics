from typing import Any, List, Optional, Tuple

from pydantic import BaseModel, Field, model_validator


class AgentReply(BaseModel):
    reply: Optional[str] = Field(
        None, description="The agent's reply to the user message"
    )


class UserMessage(BaseModel):
    user_message: Optional[str] = Field(
        None, description="The user's message to the agent"
    )


class ConversationHistory(BaseModel):
    history: List[Tuple[UserMessage, AgentReply]] = Field(
        [], description="The conversation history with the agent"
    )


class ChatInput(BaseModel):
    # Use Any to allow dicts during deserialization, then convert in validator
    user_message: Optional[UserMessage] = Field(
        None, description="The user's last message to the agent"
    )
    conversation_history: Optional[ConversationHistory] = Field(
        None, description="The conversation history with the agent"
    )

    @model_validator(mode="after")
    def convert_dicts_to_models(self):
        """Convert dict representations to Pydantic models after initial validation"""
        # Convert user_message dict to UserMessage if needed
        if self.user_message is not None and isinstance(self.user_message, dict):
            self.user_message = UserMessage(**self.user_message)

        # Convert conversation_history dict to ConversationHistory if needed
        if self.conversation_history is not None and isinstance(
            self.conversation_history, dict
        ):
            self.conversation_history = ConversationHistory(**self.conversation_history)

        return self
