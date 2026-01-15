from abc import ABC, abstractmethod
from typing import Optional, Any
from langchain_core.language_models.chat_models import BaseChatModel

class LLMBaseModel(ABC):
    """
    Abstract base class for LLM provider strategies.
    Different providers (OpenAI, Anthropic, etc.) should implement this interface.
    """
    
    @abstractmethod
    def create_model(
        self,
        model_name: str,
        temperature: float,
        max_tokens: int,
        api_key: Optional[str] = None,
        **kwargs
    ) -> BaseChatModel:
        """
        Creates and returns a LangChain chat model instance.
        """
        pass

    @abstractmethod
    def is_provider_for(self, model_name: str) -> bool:
        """
        Determines if this strategy handles the given model name.
        """
        pass
