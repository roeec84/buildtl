from typing import Optional
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_core.embeddings import Embeddings
from .base import EmbeddingBaseModel

class HuggingFaceEmbeddingModel(EmbeddingBaseModel):
    """
    Strategy for creating local HuggingFace Embeddings.
    """
    
    def create_embedding(self, model_name: str, api_key: Optional[str] = None, **kwargs) -> Embeddings:
        # Default to a decent model if none specified (though usually handled by caller)
        model = model_name if model_name else "sentence-transformers/all-MiniLM-L6-v2"
        return HuggingFaceEmbeddings(
            model_name=model,
            model_kwargs={'device': 'cpu'},
            encode_kwargs={'normalize_embeddings': True}
        )
