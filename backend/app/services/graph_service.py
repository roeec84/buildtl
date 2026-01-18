from typing import TypedDict, List, Optional, Any, Dict, Annotated, Literal
from langgraph.graph import StateGraph, END
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langgraph.graph.message import add_messages
from app.services.llm_service import LLMService


class AgentState(TypedDict):
    messages: Annotated[List[BaseMessage], add_messages] 
    user_id: str
    dashboard_id: Optional[str]
    connection_string: Any # SQLAlchemy Engine
    llm_service: Any
    
    sql_query: Optional[str]
    query_result: Optional[List[Dict]]
    chart_config: Optional[Dict]
    error: Optional[str]
    retry_count: int
    next_step: Optional[Literal["sql_gen", "chart_gen", "responder", "error"]]


def router_node(state: AgentState):
    """
    Decides next step based on prompt and current state.
    """
    messages = state["messages"]
    last_message = messages[-1]
    
    llm_service = state["llm_service"]
    
    if not state.get("connection_string"):
        return {"next_step": "responder"}

    system_prompt = """You are a routing agent for a BI Dashboard AI.
    Your job is to classify the user's message into one of THREE paths:
    
    1. 'sql_gen': The user is asking for NEW data, metrics, or filtering changes that require a database query.
       Examples: "Show me sales", "Filter by region", "What is the revenue?".
       
    2. 'chart_gen': The user wants to change the VISUALIZATION style of the CURRENT data.
       Examples: "Make it a bar chart", "Change color to red", "Show labels".
       (Only usage valid if we already have data).
       
    3. 'responder': General chat, greeting, or clarifying question.
       Examples: "Hi", "What can you do?", "Thanks".
       
    Return ONLY one of these words: 'sql_gen', 'chart_gen', or 'responder'.
    """
    
    response = llm_service.llm.invoke([
        SystemMessage(content=system_prompt),
        last_message
    ])
    
    choice = response.content.strip().lower()
    
    if "chart" in choice:
        if state.get("query_result"):
             return {"next_step": "chart_gen"}
        else:
             return {"next_step": "sql_gen"}
    elif "sql" in choice:
        return {"next_step": "sql_gen"}
    else:
        return {"next_step": "responder"}



async def sql_generator_node(state: AgentState):
    """
    Generates SQL using LLMService.
    Transition: -> sql_exec
    """
    messages = state["messages"]
    last_message = messages[-1].content
    llm_service = state["llm_service"]
    
    sql = await llm_service.generate_sql_query(
        user_message=str(last_message),
        engine=state["connection_string"]
    )
    
    return {"sql_query": sql, "next_step": "sql_exec"}

async def sql_executor_node(state: AgentState):
    """
    Executes SQL against logic.
    Transition: -> chart_gen (success) or error (failure)
    """
    from app.services.etl_service import ETLService
    
    sql = state.get("sql_query")
    if not sql:
        return {"error": "No SQL generated", "next_step": "error"}
        
    try:
        engine = state["connection_string"]
        rows = await ETLService.execute_sql_query(engine, sql)
            
        return {"query_result": rows, "next_step": "chart_gen", "error": None}
    except Exception as e:
        return {"error": str(e), "next_step": "error"}


async def chart_configurator_node(state: AgentState):
    """
    Generates Chart JSON.
    """
    llm_service = state["llm_service"]
    if "query_result" not in state or not state["query_result"]:
        return {"error": "No data available to chart", "next_step": "error"}
    
    data_sample = state["query_result"]
    columns = list(data_sample[0].keys()) if data_sample else []
    
    messages = state["messages"]
    last_message = messages[-1].content
    
    previous_config = state.get("chart_config")
    
    config = await llm_service.generate_chart_config(
        user_message=str(last_message),
        data_sample=data_sample,
        columns=columns,
        previous_config=previous_config
    )
    
    if hasattr(config, "model_dump"):
        config_dict = config.model_dump()
    else:
        config_dict = config
        
    return {"chart_config": config_dict, "next_step": "responder"}


async def sql_fixer_node(state: AgentState):
    """
    Attempts to fix broken SQL.
    Transition: -> sql_exec
    """
    llm_service = state["llm_service"]
    error = state.get("error")
    bad_sql = state.get("sql_query")
    retry_count = state.get("retry_count", 0)
    
    if retry_count >= 3:
        return {"next_step": "responder", "messages": [AIMessage(content=f"I tried to fix the query 3 times but failed. Error: {error}")]}
        
    system_prompt = f"""The following SQL query failed:
    ```sql
    {bad_sql}
    ```
    Error message:
    {error}
    
    Fix the SQL query. Return ONLY the fixed SQL query."""
    
    response = await llm_service.llm.ainvoke([HumanMessage(content=system_prompt)])
    fixed_sql = response.content.strip()
    if fixed_sql.startswith("```"):
         fixed_sql = fixed_sql.replace("```sql", "").replace("```", "").strip()
         
    return {
        "sql_query": fixed_sql, 
        "retry_count": retry_count + 1, 
        "next_step": "sql_exec",
        "error": None
    }

async def responder_node(state: AgentState):
    """
    Generates final text response or returns the chart.
    """
    llm_service = state["llm_service"]
    
    if state.get("chart_config"):
        return {"messages": [AIMessage(content="I've generated the chart for you based on the live data.")]}
    
    if state.get("error"):
        return {"messages": [AIMessage(content=f"I encountered an error: {state['error']}")]}
        
    messages = state["messages"]
    response_text, _ = await llm_service.generate_response(
        user_message=messages[-1].content,
        conversation_history=[] 
    )
    return {"messages": [AIMessage(content=response_text)]}


workflow = StateGraph(AgentState)

workflow.add_node("router", router_node)
workflow.add_node("sql_gen", sql_generator_node)
workflow.add_node("sql_exec", sql_executor_node)
workflow.add_node("sql_fix", sql_fixer_node) 
workflow.add_node("chart_gen", chart_configurator_node)
workflow.add_node("responder", responder_node)

workflow.set_entry_point("router")

def route_decision(state: AgentState):
    step = state.get("next_step")
    if step == "sql_gen": return "sql_gen"
    if step == "sql_exec": return "sql_exec"
    if step == "chart_gen": return "chart_gen"
    if step == "error": return "sql_fix"
    return "responder"

def after_exec_decision(state: AgentState):
    if state.get("error"):
        return "sql_fix"
    return "chart_gen"

def after_fix_decision(state: AgentState):
    if state.get("retry_count", 0) >= 3:
        return "responder"
    return "sql_exec"

workflow.add_conditional_edges(
    "router",
    route_decision,
    {
        "sql_gen": "sql_gen",
        "chart_gen": "chart_gen",
        "responder": "responder"
    }
)

workflow.add_edge("sql_gen", "sql_exec")

workflow.add_conditional_edges(
    "sql_exec",
    after_exec_decision,
    {
        "chart_gen": "chart_gen",
        "sql_fix": "sql_fix"
    }
)

workflow.add_conditional_edges(
    "sql_fix",
    after_fix_decision,
    {
        "sql_exec": "sql_exec",
        "responder": "responder"
    }
)

workflow.add_edge("chart_gen", "responder")
workflow.add_edge("responder", END)

graph = workflow.compile()
