"""
Message model for storing individual chat messages.
"""
from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from app.db.database import Base


class MessageType(str, enum.Enum):
    """Enum for message types"""
    HUMAN = "human"
    AI = "ai"


class Message(Base):
    """
    Message table to store individual messages within conversations.

    Fields:
        id: Primary key
        conversation_id: Foreign key to parent conversation
        type: Type of message (human or ai)
        content: The actual message content
        created_at: Timestamp when message was created
    """
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    conversation_id = Column(Integer, ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False)
    type = Column(Enum(MessageType), nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    conversation = relationship("Conversation", back_populates="messages")

    def __repr__(self):
        content_preview = self.content[:50] + "..." if len(self.content) > 50 else self.content
        return f"<Message(id={self.id}, type={self.type}, content='{content_preview}')>"
