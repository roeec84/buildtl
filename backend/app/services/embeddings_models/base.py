from abc import ABC, abstractmethod
from typing import Optional
from langchain_core.embeddings import Embeddings

class EmbeddingBaseModel(ABC):
    """
    Abstract base class for Embedding strategies.
    Different providers (OpenAI, HuggingFace, etc.) should implement this interface.
    """
    
    @abstractmethod
    def create_embedding(self, model_name: str, api_key: Optional[str] = None, **kwargs) -> Embeddings:
        """
        Creates and returns a LangChain Embeddings instance.
        """
        pass
