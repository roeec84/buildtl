from typing import List, Optional
from langchain_core.language_models.chat_models import BaseChatModel
from .base import LLMBaseModel
from .openai import OpenAIModel
from .anthropic import AnthropicModel

class LLMModelFactory:
    """
    Factory class to create LLM models using registered strategies.
    Implements the Strategy Pattern context.
    """
    
    def __init__(self):
        # Register available strategies
        self.strategies: List[LLMBaseModel] = [
            OpenAIModel(),
            AnthropicModel()
        ]

    def create_llm(
        self, 
        model_name: str, 
        temperature: float, 
        max_tokens: int, 
        api_key: Optional[str] = None
    ) -> BaseChatModel:
        """
        Iterates through registered strategies to find one that supports the model_name.
        """
        for strategy in self.strategies:
            if strategy.is_provider_for(model_name):
                return strategy.create_model(
                    model_name=model_name,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    api_key=api_key
                )
        
       
        fallback_strategy = OpenAIModel()
        return fallback_strategy.create_model(
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key
        )
