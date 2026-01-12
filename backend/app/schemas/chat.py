"""
Pydantic schemas for chat requests and responses.
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional


class MessageCreate(BaseModel):
    """Schema for creating a message"""
    content: str = Field(..., min_length=1)
    type: str = Field(..., pattern="^(human|ai)$")


class MessageResponse(BaseModel):
    """Response schema for a message"""
    id: int
    type: str
    content: str
    timestamp: datetime

    model_config = {"from_attributes": True}


class ChatRequest(BaseModel):
    """Request schema for sending a chat message"""
    message: str = Field(..., min_length=1)
    chatId: Optional[str] = None
    model: str = Field(..., min_length=1)
    dataSource: str = "default"
    store: Optional[str] = None


class ChatMessageHistory(BaseModel):
    """Schema for message history format"""
    type: str
    data: dict


class ChatResponse(BaseModel):
    """Response schema for chat message"""
    response: str
    history: List[ChatMessageHistory]
    id: str
    error: bool = False


class ConversationResponse(BaseModel):
    """Response schema for a conversation"""
    id: int
    chat_id: str
    title: str
    history: List[ChatMessageHistory]
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = {
        "from_attributes": True,
        "populate_by_name": True
    }


class ConversationListResponse(BaseModel):
    """Response schema for list of conversations"""
    conversations: List[ConversationResponse]
