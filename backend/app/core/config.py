"""
Application configuration management using Pydantic Settings.
This file handles all environment variables and app settings.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Pydantic will automatically read from .env file and environment variables.
    Priority: Environment variables > .env file > default values
    """

    # Application
    APP_NAME: str = "BuildTL"
    VERSION: str = "2.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "development"

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://user:password@localhost:5432/chatbot_db"

    # Security - JWT Configuration
    SECRET_KEY: str = "your-secret-key-change-in-production-use-openssl-rand-hex-32"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7  # 7 days

    # CORS - Allow frontend to connect
    BACKEND_CORS_ORIGINS: list[str] = ["http://localhost:5173", "http://localhost:5174", "http://localhost:3000"]

    # LLM API Keys
    OPENAI_API_KEY: Optional[str] = None
    ANTHROPIC_API_KEY: Optional[str] = None

    # Vector Store - ChromaDB
    CHROMA_HOST: str = "localhost"
    CHROMA_PORT: int = 8000
    CHROMA_URL: Optional[str] = None

    @property
    def chroma_connection_url(self) -> str:
        """Build ChromaDB connection URL"""
        if self.CHROMA_URL:
            return self.CHROMA_URL
        return f"http://{self.CHROMA_HOST}:{self.CHROMA_PORT}"

    # Redis (for caching/sessions)
    REDIS_URL: str = "redis://localhost:6379"

    # File Upload
    UPLOAD_DIR: str = "./uploads"
    MAX_UPLOAD_SIZE: int = 10 * 1024 * 1024  # 10MB

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True
    )


# Create a global settings instance
settings = Settings()
