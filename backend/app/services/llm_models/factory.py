from typing import List, Optional
from langchain_core.language_models.chat_models import BaseChatModel
from .base import LLMBaseModel
from .openai import OpenAIModel
from .anthropic import AnthropicModel

class LLMModelFactory:
    """
    Factory class to create LLM models using registered models.
    Implements the Strategy Pattern context.
    """
    
    def __init__(self):
        # Register available models
        self.models: List[LLMBaseModel] = [
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
        Iterates through registered models to find one that supports the model_name.
        """
        for model in self.models:
            if model.is_provider_for(model_name):
                return model.create_model(
                    model_name=model_name,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    api_key=api_key
                )
        
       
        fallback_model = OpenAIModel()
        return fallback_model.create_model(
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key
        )
