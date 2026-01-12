"""
Settings models for user configurations (models, data sources, vector stores).
"""
from sqlalchemy import Column, Integer, String, ForeignKey, Text, Float, JSON
from sqlalchemy.orm import relationship
from app.db.database import Base


class ModelSetting(Base):
    """
    Model settings for LLM configurations.

    Fields:
        id: Primary key
        user_id: Foreign key to user
        name: Model identifier (e.g., 'gpt-4', 'claude-3')
        display_name: User-friendly name
        provider: LLM provider (openai, anthropic, etc.)
        api_key: Encrypted API key
        temperature: Model temperature setting
        max_tokens: Maximum tokens for generation
        config: Additional JSON configuration
    """
    __tablename__ = "model_settings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    provider = Column(String, nullable=False)  # openai, anthropic, etc.
    api_key = Column(String, nullable=True)
    temperature = Column(Float, default=0.7)
    max_tokens = Column(Integer, default=2000)
    config = Column(JSON, default=dict)

    # Relationships
    user = relationship("User", back_populates="model_settings")

    def __repr__(self):
        return f"<ModelSetting(id={self.id}, name='{self.name}', provider='{self.provider}')>"


class DataSource(Base):
    """
    Data source configurations for RAG.

    Fields:
        id: Primary key
        user_id: Foreign key to user
        name: Data source identifier
        display_name: User-friendly name
        type: Type of data source (github, file, url, etc.)
        config: JSON configuration for the data source
    """
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    display_name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # github, file, url, database, etc.
    config = Column(JSON, default=dict)  # Store type-specific configuration

    # Relationships
    user = relationship("User", back_populates="data_sources")

    def __repr__(self):
        return f"<DataSource(id={self.id}, name='{self.name}', type='{self.type}')>"


class VectorStore(Base):
    """
    Vector store configurations for embeddings.

    Fields:
        id: Primary key
        user_id: Foreign key to user
        name: Store identifier
        type: Type of vector store (chroma, pinecone, etc.)
        config: JSON configuration (host, port, credentials, etc.)
    """
    __tablename__ = "vector_stores"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # chroma, pinecone, weaviate, etc.
    config = Column(JSON, default=dict)  # Store connection details

    # Relationships
    user = relationship("User", back_populates="vector_stores")

    def __repr__(self):
        return f"<VectorStore(id={self.id}, name='{self.name}', type='{self.type}')>"
