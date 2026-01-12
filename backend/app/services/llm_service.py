"""
LLM Service - Integrates with OpenAI, Anthropic, and other LLM providers using LangChain.
Handles message history, conversation context, and model selection.
"""
from typing import List, Dict, Any, Optional, Tuple
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage, ToolMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_classic.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.tools import Tool
from app.core.config import settings
from app.models.message import Message as DBMessage


class LLMService:
    """
    Service for interacting with LLM providers.
    Supports OpenAI (GPT-4, GPT-3.5) and Anthropic (Claude).
    """

    def __init__(
        self,
        model_name: str,
        api_key: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000
    ):
        """
        Initialize LLM service with specified model.

        Args:
            model_name: Name of the model (e.g., 'gpt-4', 'claude-3-opus')
            api_key: Optional API key (falls back to settings)
            temperature: Model temperature (0-1, controls randomness)
            max_tokens: Maximum tokens in response
        """
        self.model_name = model_name
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.llm = self._initialize_model(model_name, api_key)
        self.tools = self._initialize_tools()

    def _initialize_model(self, model_name: str, api_key: Optional[str] = None):
        """
        Initialize the appropriate LangChain model based on model name.

        Args:
            model_name: Name of the model
            api_key: Optional API key

        Returns:
            Initialized LangChain chat model

        Raises:
            ValueError: If model is not supported
        """
        model_lower = model_name.lower()

        # OpenAI models
        if any(x in model_lower for x in ['gpt-4', 'gpt-3.5', 'gpt4', 'gpt3']):
            return ChatOpenAI(
                model=model_name,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                api_key=api_key or settings.OPENAI_API_KEY
            )

        # Anthropic Claude models
        elif any(x in model_lower for x in ['claude', 'anthropic']):
            return ChatAnthropic(
                model=model_name,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                api_key=api_key or settings.ANTHROPIC_API_KEY
            )

        else:
            raise ValueError(f"Unsupported model: {model_name}")

    def _extract_text_content(self, content: Any) -> str:
        """
        Extract text content from response.content which might be a string or list.

        Args:
            content: Response content (str or list of content blocks)

        Returns:
            Extracted text as string
        """
        if isinstance(content, str):
            return content
        elif isinstance(content, list):
            # Extract text from content blocks
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get('type') == 'text':
                    text_parts.append(block.get('text', ''))
                elif isinstance(block, str):
                    text_parts.append(block)
            return ''.join(text_parts)
        else:
            return str(content)

    def _initialize_tools(self) -> List[Tool]:
        """
        Initialize tools for the LLM agent.

        Returns:
            List of available tools
        """
        # Web search tool
        search = DuckDuckGoSearchRun()

        tools = [
            Tool(
                name="web_search",
                description="Search the web for current information, news, facts, and real-time data. Use this when you need up-to-date information or when asked about current events, today's date, recent news, or any information you don't have in your training data.",
                func=search.run,
            )
        ]

        return tools

    def _convert_db_messages_to_langchain(
        self,
        messages: List[DBMessage]
    ) -> List[BaseMessage]:
        """
        Convert database messages to LangChain message format.

        Args:
            messages: List of database Message objects

        Returns:
            List of LangChain BaseMessage objects
        """
        langchain_messages = []

        for msg in messages:
            if msg.type == "human":
                langchain_messages.append(HumanMessage(content=msg.content))
            elif msg.type == "ai":
                langchain_messages.append(AIMessage(content=msg.content))

        return langchain_messages

    def _format_message_history(
        self,
        messages: List[BaseMessage]
    ) -> List[Dict[str, str]]:
        """
        Format LangChain messages for frontend consumption.
        Filters out ToolMessage and SystemMessage, only keeping HumanMessage and AIMessage.

        Args:
            messages: List of LangChain messages

        Returns:
            List of dictionaries with type and content
        """
        formatted = []

        for msg in messages:
            if isinstance(msg, HumanMessage):
                formatted.append({
                    "type": "human",
                    "data": {"content": msg.content}
                })
            elif isinstance(msg, AIMessage):
                # Extract text content (handles both string and list formats)
                text_content = self._extract_text_content(msg.content)
                # Only include AIMessage if it has actual text (not just tool calls)
                if text_content:
                    formatted.append({
                        "type": "ai",
                        "data": {"content": text_content}
                    })
            # Skip ToolMessage, SystemMessage, and AIMessages that are just tool calls

        return formatted

    async def generate_response(
        self,
        user_message: str,
        conversation_history: Optional[List[DBMessage]] = None,
        system_prompt: Optional[str] = None
    ) -> Tuple[str, List[Dict[str, str]]]:
        """
        Generate a response from the LLM with tool support.

        Args:
            user_message: The user's message
            conversation_history: Optional list of previous messages
            system_prompt: Optional system prompt to guide the model

        Returns:
            Tuple of (AI response, full conversation history)
        """
        # Convert conversation history to LangChain format
        messages = []

        if conversation_history:
            messages = self._convert_db_messages_to_langchain(conversation_history)

        # Add system prompt if provided
        if system_prompt:
            messages.insert(0, SystemMessage(content=system_prompt))

        # Add current user message
        messages.append(HumanMessage(content=user_message))

        # Bind tools to the LLM
        llm_with_tools = self.llm.bind_tools(self.tools)

        # Generate response
        response = await llm_with_tools.ainvoke(messages)

        # Loop until we get a response without tool calls
        while hasattr(response, 'tool_calls') and response.tool_calls:
            # Add the AI's response with tool calls
            messages.append(response)

            # Execute each tool call and add as ToolMessage
            for tool_call in response.tool_calls:
                tool_name = tool_call.get('name')
                tool_args = tool_call.get('args', {})
                tool_call_id = tool_call.get('id', '')

                # Find and execute the tool
                for tool in self.tools:
                    if tool.name == tool_name:
                        try:
                            tool_result = tool.func(tool_args.get('query', ''))
                            # Add tool result as ToolMessage (required by Anthropic)
                            messages.append(ToolMessage(
                                content=str(tool_result),
                                tool_call_id=tool_call_id
                            ))
                        except Exception as e:
                            messages.append(ToolMessage(
                                content=f"Error: {str(e)}",
                                tool_call_id=tool_call_id
                            ))
                        break

            # Get next response - keep looping until no more tool calls
            response = await llm_with_tools.ainvoke(messages)

        # Now we have a response without tool calls - extract the text
        response_content = self._extract_text_content(response.content)
        messages.append(AIMessage(content=response_content))

        # Format history for return
        formatted_history = self._format_message_history(messages)

        return response_content, formatted_history

    async def generate_response_with_rag(
        self,
        user_message: str,
        context_documents: List[str],
        conversation_history: Optional[List[DBMessage]] = None
    ) -> Tuple[str, List[Dict[str, str]]]:
        """
        Generate a response using RAG (Retrieval-Augmented Generation).

        Args:
            user_message: The user's message
            context_documents: Retrieved documents to use as context
            conversation_history: Optional list of previous messages

        Returns:
            Tuple of (AI response, full conversation history)
        """
        # Build RAG system prompt
        context = "\n\n".join(context_documents)
        system_prompt = f"""You are a helpful assistant. Use the following context to answer the user's question.
If you cannot find the answer in the context, say so clearly.

Context:
{context}

Answer the question based on the context above."""

        return await self.generate_response(
            user_message,
            conversation_history,
            system_prompt
        )


class LLMFactory:
    """Factory for creating LLM service instances based on user settings."""

    @staticmethod
    def create_from_settings(
        model_setting: Dict[str, Any]
    ) -> LLMService:
        """
        Create LLM service from model settings.

        Args:
            model_setting: Dictionary with model configuration
                - name: Model name
                - api_key: Optional API key
                - temperature: Optional temperature
                - max_tokens: Optional max tokens

        Returns:
            Configured LLMService instance
        """
        return LLMService(
            model_name=model_setting.get("name"),
            api_key=model_setting.get("api_key"),
            temperature=model_setting.get("temperature", 0.7),
            max_tokens=model_setting.get("max_tokens", 2000)
        )
