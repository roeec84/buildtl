"""
Chat endpoints for sending messages and managing conversations.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from typing import List
import uuid
from app.db.database import get_db
from app.models.user import User
from app.models.conversation import Conversation
from app.models.message import Message, MessageType
from app.models.settings import ModelSetting
from app.schemas.chat import (
    ChatRequest,
    ChatResponse,
    ConversationResponse,
    ChatMessageHistory
)
from app.api.deps import get_current_active_user
from app.services.llm_service import LLMService
from app.services.vector_store_service import VectorStoreFactory


router = APIRouter(prefix="/api/chat", tags=["Chat"])


@router.post("/send", response_model=ChatResponse)
async def send_message(
    request: ChatRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Send a chat message and get AI response.

    Args:
        request: Chat request with message and configuration
        current_user: Authenticated user
        db: Database session

    Returns:
        ChatResponse with AI response and conversation history
    """
    try:
        # Get or create conversation
        if request.chatId:
            # Load existing conversation
            result = await db.execute(
                select(Conversation)
                .where(Conversation.chat_id == request.chatId)
                .where(Conversation.user_id == current_user.id)
            )
            conversation = result.scalar_one_or_none()

            if not conversation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )

            # Load message history
            result = await db.execute(
                select(Message)
                .where(Message.conversation_id == conversation.id)
                .order_by(Message.id)
            )
            message_history = result.scalars().all()
        else:
            # Create new conversation
            chat_id = uuid.uuid4().hex
            conversation = Conversation(
                chat_id=chat_id,
                user_id=current_user.id,
                title=request.message[:50] + ("..." if len(request.message) > 50 else "")
            )
            db.add(conversation)
            await db.flush()  # Get conversation.id
            message_history = []

        # Get model settings
        result = await db.execute(
            select(ModelSetting)
            .where(ModelSetting.user_id == current_user.id)
            .where(ModelSetting.name == request.model)
        )
        model_setting = result.scalar_one_or_none()

        if not model_setting:
            # Use default settings if model not configured
            llm_service = LLMService(
                model_name=request.model,
                temperature=0.7,
                max_tokens=2000
            )
        else:
            llm_service = LLMService(
                model_name=model_setting.name,
                api_key=model_setting.api_key,
                temperature=model_setting.temperature,
                max_tokens=model_setting.max_tokens
            )

        # Check if using RAG with vector store
        if request.dataSource and request.dataSource != "default":
            # Normalize data source name by removing spaces (matches vector_store_service.py logic)
            normalized_data_source = request.dataSource.replace(" ", "")

            # Create vector store for retrieval
            vector_store = VectorStoreFactory.create_for_user(
                user_id=current_user.id,
                collection_name=normalized_data_source
            )

            # Retrieve relevant documents
            documents = await vector_store.similarity_search(
                query=request.message,
                k=4
            )

            # Generate response with RAG
            response_content, formatted_history = await llm_service.generate_response_with_rag(
                user_message=request.message,
                context_documents=[doc.page_content for doc in documents],
                conversation_history=message_history
            )
        else:
            # Generate response without RAG
            response_content, formatted_history = await llm_service.generate_response(
                user_message=request.message,
                conversation_history=message_history
            )

        # Save messages to database
        # Save user message
        user_message = Message(
            conversation_id=conversation.id,
            type=MessageType.HUMAN,
            content=request.message
        )
        db.add(user_message)

        # Save AI response
        ai_message = Message(
            conversation_id=conversation.id,
            type=MessageType.AI,
            content=response_content
        )
        db.add(ai_message)

        await db.commit()

        # Build response
        return ChatResponse(
            response=response_content,
            history=formatted_history,
            id=conversation.chat_id,
            error=False
        )

    except Exception as e:
        await db.rollback()
        print(f"Error in send_message: {e}")
        import traceback
        traceback.print_exc()

        return ChatResponse(
            response=f"An error occurred: {str(e)}",
            history=[],
            id=request.chatId or "error",
            error=True
        )


@router.get("/conversations", response_model=List[ConversationResponse])
async def get_conversations(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all conversations for the current user.

    Args:
        current_user: Authenticated user
        db: Database session

    Returns:
        List of conversations with message history
    """
    # Get user's conversations
    result = await db.execute(
        select(Conversation)
        .where(Conversation.user_id == current_user.id)
        .order_by(Conversation.updated_at.desc())
    )
    conversations = result.scalars().all()

    # Build response with message history
    response = []
    for conv in conversations:
        # Get messages for this conversation
        result = await db.execute(
            select(Message)
            .where(Message.conversation_id == conv.id)
            .order_by(Message.id)
        )
        messages = result.scalars().all()

        # Format message history
        history = [
            ChatMessageHistory(
                type=msg.type.value,
                data={"content": msg.content}
            )
            for msg in messages
        ]

        response.append(
            ConversationResponse(
                id=conv.id,
                chat_id=conv.chat_id,
                title=conv.title,
                history=history,
                created_at=conv.created_at,
                updated_at=conv.updated_at
            )
        )

    return response


@router.delete("/conversations/{chat_id}")
async def delete_conversation(
    chat_id: str,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete a conversation.

    Args:
        chat_id: Conversation ID to delete
        current_user: Authenticated user
        db: Database session

    Returns:
        Success message
    """
    # Find conversation
    result = await db.execute(
        select(Conversation)
        .where(Conversation.chat_id == chat_id)
        .where(Conversation.user_id == current_user.id)
    )
    conversation = result.scalar_one_or_none()

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    # Delete conversation (messages will be cascade deleted)
    await db.delete(conversation)
    await db.commit()

    return {"message": "Conversation deleted successfully"}
