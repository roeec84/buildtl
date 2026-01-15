from typing import Optional
from langchain_core.embeddings import Embeddings
from .base import EmbeddingBaseModel
from .openai import OpenAIEmbeddingModel
from .huggingface import HuggingFaceEmbeddingModel
from app.core.config import settings

class EmbeddingModelFactory:
    """
    Factory class to create Embedding models using registered strategies.
    Implementation includes default logic to choose between OpenAI and Local.
    """
    
    def __init__(self):
        self.openai_strategy = OpenAIEmbeddingModel()
        self.huggingface_strategy = HuggingFaceEmbeddingModel()

    def create_embedding_model(
        self, 
        embedding_model: str,
        api_key: Optional[str] = None
    ) -> Embeddings:
        """
        Create structured embedding model.
        Logic: If API key provided or in settings, try OpenAI. Else fallback to HuggingFace.
        """
        openai_key = api_key or settings.OPENAI_API_KEY
        
        if openai_key:
            return self.openai_strategy.create_embedding(
                model_name=embedding_model,
                api_key=openai_key
            )
        else:
            print("No OpenAI API key found. Using local HuggingFace embeddings strategy.")
            return self.huggingface_strategy.create_embedding(
                model_name="sentence-transformers/all-MiniLM-L6-v2" # Force local model name or pass embedding_model if valid
            )
