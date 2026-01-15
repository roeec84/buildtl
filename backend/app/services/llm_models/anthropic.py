from typing import Optional
from langchain_anthropic import ChatAnthropic
from langchain_core.language_models.chat_models import BaseChatModel
from .base import LLMBaseModel
from app.core.config import settings

class AnthropicModel(LLMBaseModel):
    """
    Strategy for creating Anthropic Claude Models.
    """

    def is_provider_for(self, model_name: str) -> bool:
        model_lower = model_name.lower()
        return any(x in model_lower for x in ['claude', 'anthropic'])

    def create_model(
        self,
        model_name: str,
        temperature: float,
        max_tokens: int,
        api_key: Optional[str] = None,
        **kwargs
    ) -> BaseChatModel:
        return ChatAnthropic(
            model=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key or settings.ANTHROPIC_API_KEY
        )
