"""
Vector Store Service - Manages embeddings and retrieval using ChromaDB.
Supports RAG (Retrieval-Augmented Generation) for document-based chat.
"""
from typing import List, Optional, Dict, Any
import chromadb
from chromadb.config import Settings as ChromaSettings
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_core.documents import Document
from app.core.config import settings


class VectorStoreService:
    """
    Service for managing vector embeddings and similarity search.
    Uses ChromaDB as the vector database.
    """

    def __init__(
        self,
        collection_name: str,
        chroma_url: Optional[str] = None,
        embedding_model: str = "text-embedding-3-small",
        api_key: Optional[str] = None
    ):
        """
        Initialize vector store service.

        Args:
            collection_name: Name of the ChromaDB collection
            chroma_url: Optional ChromaDB server URL
            embedding_model: Embedding model to use
            api_key: Optional OpenAI API key (if not using local embeddings)
        """
        self.collection_name = collection_name.replace(" ", "")
        self.chroma_url = chroma_url or settings.chroma_connection_url
        self.embedding_model = embedding_model
        self.api_key = api_key

        # Initialize ChromaDB client
        self.client = self._initialize_client()

        # Initialize embeddings - use local model if no API key
        self.embeddings = self._initialize_embeddings()

        # Initialize vector store
        self.vector_store = self._initialize_vector_store()

    def _initialize_client(self) -> chromadb.ClientAPI:
        """
        Initialize ChromaDB client.

        Returns:
            ChromaDB client instance
        """
        try:
            # Try to connect to ChromaDB server
            client = chromadb.HttpClient(
                host=settings.CHROMA_HOST,
                port=settings.CHROMA_PORT
            )
            # Test connection
            client.heartbeat()
            return client
        except Exception as e:
            print(f"Warning: Could not connect to ChromaDB server: {e}")
            print("Falling back to in-memory ChromaDB")
            # Fall back to in-memory client for development
            return chromadb.Client()

    def _initialize_embeddings(self):
        """
        Initialize embeddings model.
        Uses OpenAI if API key is available, otherwise falls back to local HuggingFace model.

        Returns:
            Embeddings instance
        """
        # Check if OpenAI API key is available
        openai_key = self.api_key or settings.OPENAI_API_KEY

        if openai_key:
            # Use OpenAI embeddings
            return OpenAIEmbeddings(
                model=self.embedding_model,
                api_key=openai_key
            )
        else:
            # Fall back to local HuggingFace embeddings
            print("No OpenAI API key found. Using local HuggingFace embeddings (all-MiniLM-L6-v2)")
            return HuggingFaceEmbeddings(
                model_name="sentence-transformers/all-MiniLM-L6-v2",
                model_kwargs={'device': 'cpu'},
                encode_kwargs={'normalize_embeddings': True}
            )

    def _initialize_vector_store(self) -> Chroma:
        """
        Initialize LangChain Chroma vector store.

        Returns:
            Chroma vector store instance
        """
        try:
            return Chroma(
                client=self.client,
                collection_name=self.collection_name,
                embedding_function=self.embeddings
            )
        except Exception as e:
            print(f"Chroma creation error: {e}")
            raise

    async def add_documents(
        self,
        texts: List[str],
        metadatas: Optional[List[Dict[str, Any]]] = None,
        ids: Optional[List[str]] = None
    ) -> List[str]:
        """
        Add documents to the vector store.

        Args:
            texts: List of text documents to add
            metadatas: Optional metadata for each document
            ids: Optional custom IDs for documents

        Returns:
            List of document IDs
        """
        documents = [
            Document(page_content=text, metadata=meta or {})
            for text, meta in zip(texts, metadatas or [{}] * len(texts))
        ]

        return self.vector_store.add_documents(documents, ids=ids)

    async def similarity_search(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
        """
        Search for similar documents.

        Args:
            query: Search query
            k: Number of results to return
            filter: Optional metadata filter

        Returns:
            List of similar documents
        """
        return self.vector_store.similarity_search(
            query,
            k=k,
            filter=filter
        )

    async def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Dict[str, Any]] = None
    ) -> List[tuple[Document, float]]:
        """
        Search for similar documents with similarity scores.

        Args:
            query: Search query
            k: Number of results to return
            filter: Optional metadata filter

        Returns:
            List of (document, score) tuples
        """
        return self.vector_store.similarity_search_with_score(
            query,
            k=k,
            filter=filter
        )

    async def delete_documents(self, ids: List[str]) -> None:
        """
        Delete documents from vector store.

        Args:
            ids: List of document IDs to delete
        """
        self.vector_store.delete(ids=ids)

    async def clear_collection(self) -> None:
        """Delete all documents from the collection."""
        try:
            self.client.delete_collection(self.collection_name)
            # Reinitialize the vector store
            self.vector_store = self._initialize_vector_store()
        except Exception as e:
            print(f"Error clearing collection: {e}")

    async def delete(self) -> None:
        """Delete the collection permanently."""
        try:
            self.client.delete_collection(self.collection_name)
            print(f"[VECTOR STORE] Deleted collection: {self.collection_name}")
        except Exception as e:
            # It's possible the collection doesn't exist, which is fine
            print(f"Warning: Error deleting collection {self.collection_name}: {e}")

    def get_retriever(self, k: int = 4):
        """
        Get a LangChain retriever for use in chains.

        Args:
            k: Number of documents to retrieve

        Returns:
            LangChain retriever
        """
        return self.vector_store.as_retriever(
            search_kwargs={"k": k}
        )


class VectorStoreFactory:
    """Factory for creating vector store instances."""

    @staticmethod
    def create_for_user(
        user_id: int,
        collection_name: Optional[str] = None,
        api_key: Optional[str] = None
    ) -> VectorStoreService:
        """
        Create a vector store for a specific user.

        Args:
            user_id: User ID
            collection_name: Optional collection name (defaults to user_{user_id})
            api_key: Optional OpenAI API key

        Returns:
            VectorStoreService instance
        """
        if collection_name is None:
            collection_name = f"user_{user_id}_documents"

        return VectorStoreService(
            collection_name=collection_name,
            api_key=api_key
        )

    @staticmethod
    def create_from_settings(
        config: Dict[str, Any],
        collection_name: str
    ) -> VectorStoreService:
        """
        Create vector store from configuration.

        Args:
            config: Configuration dictionary
            collection_name: Collection name

        Returns:
            VectorStoreService instance
        """
        return VectorStoreService(
            collection_name=collection_name,
            chroma_url=config.get("url"),
            embedding_model=config.get("embedding_model", "text-embedding-3-small"),
            api_key=config.get("api_key")
        )
