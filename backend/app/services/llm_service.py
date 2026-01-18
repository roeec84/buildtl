"""
LLM Service - Integrates with OpenAI, Anthropic, and other LLM providers using LangChain.
Handles message history, conversation context, and model selection.
"""
from typing import List, Dict, Any, Optional, Tuple
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage, ToolMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_core.tools import Tool
from app.core.config import settings
from app.models.message import Message as DBMessage
from app.schemas.chart import ChartConfig
from app.services.llm_models import LLMModelFactory


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
            model_name: Name of the model
            api_key: Optional API key
            temperature: Model temperature
            max_tokens: Maximum tokens in response
        """
        self.model_name = model_name
        self.temperature = temperature
        self.max_tokens = max_tokens
        
        # Initialize Strategy Factory
        self.model_factory = LLMModelFactory()
        
        self.llm = self._initialize_model(model_name, api_key)
        self.tools = self._initialize_tools()

    def _initialize_model(self, model_name: str, api_key: Optional[str] = None):
        """
        Initialize the appropriate LangChain model using the Strategy Pattern.

        Args:
            model_name: Name of the model
            api_key: Optional API key

        Returns:
            Initialized LangChain chat model
        """
        return self.model_factory.create_llm(
            model_name=model_name,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            api_key=api_key
        )

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
        # Only returning empty list for now as SQL tools are handled separately in generate_response_with_sql_agent.
        # If i need other tools later, i can add them here.
        return []

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



    async def _handle_file_source_agent(
        self, 
        user_message: str, 
        engine_config: Dict[str, Any],
        conversation_history: List[DBMessage]
    ) -> Tuple[str, List[Dict[str, str]]]:
        """
        File-based "Agent" simulation. Generates SQL, executes via Spark, summarizes result.
        """
        try:
             # 1. Generate SQL
             print("DEBUG: Generating SQL for File Source...")
             sql = await self.generate_sql_query(user_message, engine_config)
             print(f"DEBUG: Generated SQL: {sql}")
             
             # 2. Execute SQL (Import locally to avoid circular dep)
             from app.services.etl_service import ETLService
             
             try:
                 print("DEBUG: Executing SQL on Spark...")
                 results = await ETLService.execute_sql_query(engine_config, sql)
                 # Limit results for context window
                 results_sample = results[:20] 
                 results_str = str(results_sample)
                 if len(results) > 20:
                     results_str += f"\n... ({len(results) - 20} more rows)"
             except Exception as e:
                 results_str = f"Execution Error: {str(e)}"
                 
             # 3. Generate Answer
             prompt = f"""You are a data assistant. 
User Query: {user_message}
SQL Query Used: {sql}
Data Results: 
{results_str}

Answer the user's question based on the data results.
If the results contain an error, explain it.
"""
             print("DEBUG: Generating Final Answer...")
             response = await self.llm.ainvoke([HumanMessage(content=prompt)])
             response_content = response.content
             
             # History
             messages = []
             if conversation_history:
                 messages = self._convert_db_messages_to_langchain(conversation_history)
             messages.append(HumanMessage(content=user_message))
             messages.append(AIMessage(content=response_content))
             
             return response_content, self._format_message_history(messages)
             
        except Exception as e:
             return f"File Agent Failed: {e}", []

    async def generate_response_with_sql_agent(
        self,
        user_message: str,
        engine: Any,
        conversation_history: Optional[List[DBMessage]] = None
    ) -> Tuple[str, List[Dict[str, str]]]:
        """
        Generate a response using the LangChain SQL Agent.

        Args:
            user_message: The user's query
            engine: SQLAlchemy Engine object OR Dict for file source
            conversation_history: Optional list of previous messages

        Returns:
            Tuple of (AI response, full conversation history)
        """
        # Handle File Sources (Dict)
        if isinstance(engine, dict) and engine.get('is_file_source'):
            return await self._handle_file_source_agent(user_message, engine, conversation_history)

        try:
            print(f"DEBUG: Initializing SQL Agent...")
            # Initialize SQL Database
            db = SQLDatabase(engine=engine)
            print(f"DEBUG: SQL Database initialized. Dialect: {db.dialect}, Tables: {db.get_usable_table_names()}")

            # Create SQL Agent
            agent_executor = create_sql_agent(
                llm=self.llm,
                db=db,
                agent_type="openai-tools",
                verbose=True
            )

            # Format history for context
            full_prompt = user_message
            if conversation_history:
                 # Provide last few messages as context
                 history_text = "\n".join([f"{msg.type}: {msg.content}" for msg in conversation_history[-4:]])
                 full_prompt = f"Previous conversation:\n{history_text}\n\nCurrent User Query: {user_message}"

            print(f"DEBUG: Invoking SQL Agent with prompt: {full_prompt}")
            # Execute Agent
            response = await agent_executor.ainvoke({"input": full_prompt})
            print(f"DEBUG: SQL Agent Response: {response}")
            response_content = response.get("output", "I could not generate an answer from the database.")
            
            # Re-convert existing history
            messages = []
            if conversation_history:
                messages = self._convert_db_messages_to_langchain(conversation_history)
            
            messages.append(HumanMessage(content=user_message))
            messages.append(AIMessage(content=response_content))
            
            formatted_history = self._format_message_history(messages)

            return response_content, formatted_history

        except Exception as e:
            error_msg = f"SQL Agent Error: {str(e)}"
            print(error_msg)
            return error_msg, []

    async def generate_chart_config(
        self,
        user_message: str,
        data_sample: List[Dict[str, Any]],
        columns: List[str],
        previous_config: Optional[Dict[str, Any]] = None
    ) -> ChartConfig:
        """
        Generate a chart configuration based on the user's request and data sample.

        Args:
            user_message: The user's query description
            data_sample: A sample of the data (rows)
            columns: List of column names
            previous_config: Optional previous configuration to refine

        Returns:
            ChartConfig object
        """
        refinement_context = ""
        if previous_config:
            import json
            config_str = json.dumps(previous_config, indent=2)
            refinement_context = f"""
- Previous Configuration:
{config_str}

Refinement Mode:
The user wants to MODIFY the previous chart.
1. Use the 'Previous Configuration' as your baseline.
2. Apply the changes requested in user_message.
3. Keep other settings (colors, titles) if they are still relevant.
4. If the user asks to change the data/metric, update the 'series' and 'xAxis' accordingly.
"""
        else:
            refinement_context = """
Rules:
1. Choose 'bar', 'line', 'pie', or 'scatter' appropriate for the data.
2. Map 'series' and 'xAxis' correctly.
3. For Bar/Line charts: 'xAxis' usually contains the labels/categories (scaleType='band' for categories, 'point' for time/linear).
4. `series` data MUST be extracted from the provided `data_sample`.
5. Return a valid JSON matching the ChartConfig schema.
"""

        system_msg_template = """You are an expert Data Visualization Architect using MUI X Charts.
Your goal is to choose the best chart type to visualize the provided data based on the user's request.
Internalize the data structure and schema.

Input Context:
- Columns: {columns}
- Data Sample (first 5 rows): {data_sample}

{refinement_context}
"""

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_msg_template),
            ("human", "{user_message}"),
        ])

        chain = prompt | self.llm.with_structured_output(ChartConfig)
        
        try:
            safe_sample = data_sample[:5]
            result = await chain.ainvoke({
                "columns": columns,
                "data_sample": safe_sample,
                "user_message": user_message,
                "refinement_context": refinement_context
            })
            return result
        except Exception as e:
            print(f"Error generating chart config: {e}")
            raise e

    async def generate_sql_query(
        self,
        user_message: str,
        engine: Any
    ) -> str:
        """
        Generate SQL query from natural language.
        
        Args:
            user_message: User's request
            engine: SQLAlchemy Engine
            
        Returns:
            SQL Query string
        """
        try:
            schema = ""
            dialect = "generic"
            
            # Handle File Source (Dict)
            if isinstance(engine, dict) and engine.get('is_file_source'):
                schema = f"Table: {engine.get('view_name')}\nColumns:\n{engine.get('schema_info', 'Unknown')}"
                dialect = "Spark SQL"
            else:
                # Handle SQLAlchemy Engine
                db = SQLDatabase(engine=engine)
                schema = db.get_table_info()
                dialect = db.dialect
            
            prompt = f"""You are an expert SQL Data Analyst.
Target Database Dialect: {dialect}

Schema:
{schema}

User Request: {user_message}

Return ONLY the SQL query to answer the request.
Do not wrap it in markdown block.
Do not include explanations.
If you use markdown, I will strip it, but prefer raw text.
"""
            
            response = await self.llm.ainvoke([HumanMessage(content=prompt)])
            sql = response.content.strip()
            
            # Clean up SQL (sometimes markdown blocks are included)
            if sql.startswith("```sql"):
                sql = sql[6:]
            if sql.startswith("```"):
                sql = sql[3:]
            if sql.endswith("```"):
                sql = sql[:-3]
            if "SQLQuery:" in sql:
                sql = sql.split("SQLQuery:")[1]
            
            return sql.strip()
        except Exception as e:
            print(f"Error generating SQL: {e}")
            raise e


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
