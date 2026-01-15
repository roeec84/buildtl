from typing import Optional
from langchain_openai import ChatOpenAI
from langchain_core.language_models.chat_models import BaseChatModel
from .base import LLMBaseModel
from app.core.config import settings

class OpenAIModel(LLMBaseModel):
    """
    Strategy for creating OpenAI Chat Models.
    """
    
    def is_provider_for(self, model_name: str) -> bool:
        model_lower = model_name.lower()
        return any(x in model_lower for x in ['gpt-4', 'gpt-3.5', 'gpt4', 'gpt3', 'o1-mini', 'gpt', 'openai'])

    def create_model(
        self,
        model_name: str,
        temperature: float,
        max_tokens: int,
        api_key: Optional[str] = None,
        **kwargs
    ) -> BaseChatModel:
        return ChatOpenAI(
            model=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key or settings.OPENAI_API_KEY
        )
