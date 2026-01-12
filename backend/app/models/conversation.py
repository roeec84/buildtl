"""
Conversation model for storing chat history.
"""
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.database import Base


class Conversation(Base):
    """
    Conversation table to store individual chat sessions.

    Fields:
        id: Primary key
        chat_id: UUID-like unique identifier for the chat
        user_id: Foreign key to user who owns this conversation
        title: Conversation title (first message excerpt)
        created_at: When conversation was created
        updated_at: When conversation was last updated
    """
    __tablename__ = "conversations"

    id = Column(Integer, primary_key=True, index=True)
    chat_id = Column(String, unique=True, index=True, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    title = Column(String, nullable=False, default="New Chat")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    user = relationship("User", back_populates="conversations")
    messages = relationship("Message", back_populates="conversation", cascade="all, delete-orphan", order_by="Message.id")

    def __repr__(self):
        return f"<Conversation(id={self.id}, chat_id='{self.chat_id}', title='{self.title}')>"
