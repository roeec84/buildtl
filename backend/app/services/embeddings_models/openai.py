from typing import Optional
from langchain_openai import OpenAIEmbeddings
from langchain_core.embeddings import Embeddings
from .base import EmbeddingBaseModel

class OpenAIEmbeddingModel(EmbeddingBaseModel):
    """
    Strategy for creating OpenAI Embeddings.
    """
    
    def create_embedding(self, model_name: str, api_key: Optional[str] = None, **kwargs) -> Embeddings:
        return OpenAIEmbeddings(
            model=model_name,
            api_key=api_key
        )
