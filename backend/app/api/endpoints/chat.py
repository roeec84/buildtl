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
    ChatMessageHistory,
    BuilderRequest,
    BuilderResponse
)
from app.api.deps import get_current_active_user
from app.services.llm_service import LLMService
from app.services.vector_store_service import VectorStoreFactory
from app.models.etl import ETLDataSource
from app.services.etl_service import ETLService
from sqlalchemy.orm import joinedload


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
            # 1. Check if it's an SQL Data Source
            print(f"DEBUG: Checking for SQL DataSource: '{request.dataSource}'")
            result = await db.execute(
                select(ETLDataSource)
                .options(joinedload(ETLDataSource.linked_service))
                .where(ETLDataSource.name == request.dataSource)
            )
            datasource = result.scalar_one_or_none()
            
            # List of SQL types supported by our agent
            sql_types = ['postgresql', 'mysql', 'sql_server', 'azure_sql', 'bigquery']
            
            if datasource:
                 print(f"DEBUG: Found DataSource: {datasource.name}, Service Type: {datasource.linked_service.service_type if datasource.linked_service else 'None'}")

            if datasource and datasource.linked_service and datasource.linked_service.service_type in sql_types:
                # Use SQL Agent
                print(f"DEBUG: Using SQL Agent for {datasource.name}")
                try:

                    engine = await ETLService.get_sqlalchemy_engine(datasource.id, db)
                    # Mask password for logs
                    conn_info = str(engine.url)
                    masked_conn = conn_info.replace(conn_info.split(':')[2].split('@')[0], '******') if '@' in conn_info else conn_info
                    print(f"DEBUG: Connection Engine: {masked_conn}")
                    
                    response_content, formatted_history = await llm_service.generate_response_with_sql_agent(
                        user_message=request.message,
                        engine=engine,
                        conversation_history=message_history
                    )
                except Exception as e:
                    print(f"SQL Agent failed: {e}")
                    import traceback
                    traceback.print_exc()
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                        detail=f"Failed to query database: {str(e)}"
                    )
            else:
                print(f"DEBUG: Fallback to RAG for {request.dataSource}")
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
        history = []
        for msg in messages:
            msg_data = {"content": msg.content}
            if msg.metadata_json:
                msg_data.update(msg.metadata_json)
                
            history.append(
                ChatMessageHistory(
                    type=msg.type.value,
                    data=msg_data
                )
            )

        response.append(
            ConversationResponse(
                id=conv.id,
                chat_id=conv.chat_id,
                title=conv.title,
                dashboard_id=conv.dashboard_id,
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


@router.post("/builder", response_model=BuilderResponse)
async def generate_chart(
    request: BuilderRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Generate a chart configuration from natural language using a specific data source.
    Uses LangGraph for stateful execution (Router -> SQL -> Chart).
    """
    try:
        from app.services.graph_service import graph
        from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
        
        conversation = None
        if request.dashboardId:
            stmt = select(Conversation).where(
                Conversation.dashboard_id == request.dashboardId,
                Conversation.user_id == current_user.id
            )
            result = await db.execute(stmt)
            conversation = result.scalars().first()
            
            if not conversation:
                conversation = Conversation(
                    chat_id=str(uuid.uuid4()),
                    user_id=current_user.id,
                    dashboard_id=request.dashboardId,
                    title="Dashboard Chat"
                )
                db.add(conversation)
                await db.commit()
                await db.refresh(conversation)

            user_msg = Message(
                conversation_id=conversation.id,
                type=MessageType.HUMAN,
                content=request.message
            )
            db.add(user_msg)
            db.add(user_msg)
            await db.commit()

        engine = await ETLService.get_sqlalchemy_engine(request.dataSourceId, db)

        result = await db.execute(
            select(ModelSetting)
            .where(ModelSetting.user_id == current_user.id)
            .where(ModelSetting.name == request.model)
        )
        model_setting = result.scalar_one_or_none()
        
        if model_setting:
            llm_service = LLMService(
                model_name=model_setting.name,
                api_key=model_setting.api_key,
                temperature=0.1 
            )
        else:
            llm_service = LLMService(model_name=request.model, temperature=0.1)

        previous_messages = []
        last_chart_config = None
        last_query_result = None
        
        if conversation:
            result = await db.execute(
                 select(Message)
                 .where(Message.conversation_id == conversation.id)
                 .order_by(Message.id.desc())
                 .limit(5)
            )
            history_msgs = result.scalars().all()[::-1]
            
            for msg in history_msgs:
                if msg.type == MessageType.HUMAN:
                    previous_messages.append(HumanMessage(content=msg.content))
                elif msg.type == MessageType.AI:
                    previous_messages.append(AIMessage(content=msg.content))
                    if msg.metadata_json and "chartConfig" in msg.metadata_json:
                        last_chart_config = msg.metadata_json["chartConfig"]
                        if "dataset" in last_chart_config:
                            last_query_result = last_chart_config["dataset"]
        
        if request.chartContext:
            last_chart_config = request.chartContext
            if "dataset" in request.chartContext:
                last_query_result = request.chartContext["dataset"]
        
        initial_state = {
            "messages": previous_messages + [HumanMessage(content=request.message)],
            "user_id": str(current_user.id),
            "dashboard_id": request.dashboardId,
            "connection_string": conn_string,
            "retry_count": 0,
            "llm_service": llm_service,
            "chart_config": last_chart_config,
            "query_result": last_query_result
        }
        
        final_state = await graph.ainvoke(initial_state)
        
        if final_state.get("error") and not final_state.get("chart_config"):
             response_msg = f"I encountered an error: {final_state['error']}"
             return BuilderResponse(message=response_msg, error=True)

        chart_config = final_state.get("chart_config")
        chart_dataset = final_state.get("query_result")
        
        last_msg = final_state["messages"][-1]
        response_text = last_msg.content
        
        config_obj = None
        if chart_config:
            from app.schemas.chart import ChartConfig
            
            config_obj = ChartConfig(**chart_config)
            if chart_dataset:
                config_obj.dataset = chart_dataset

        if conversation:
            ai_msg_content = response_text
            meta = {}
            if config_obj:
                meta = {"chartConfig": config_obj.model_dump(mode='json')}
            
            ai_msg = Message(
                conversation_id=conversation.id,
                type=MessageType.AI,
                content=ai_msg_content,
                metadata_json=meta
            )
            db.add(ai_msg)
            await db.commit()

        return BuilderResponse(
            message=response_text,
            chartConfig=config_obj
        )

    except Exception as e:
        print(f"Builder error: {e}")
        import traceback
        traceback.print_exc()
        return BuilderResponse(
            message=f"An error occurred: {str(e)}",
            error=True
        )
