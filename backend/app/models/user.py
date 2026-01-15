"""
User model for authentication and authorization.
"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db.database import Base


class User(Base):
    """
    User table for authentication.

    Fields:
        id: Primary key
        username: Unique username for login
        email: User's email address
        hashed_password: Bcrypt hashed password
        organization: Organization name the user belongs to
        is_org_admin: Whether user is an organization admin
        is_active: Whether user account is active
        created_at: Timestamp when user was created
        updated_at: Timestamp when user was last updated
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    organization = Column(String, nullable=True)
    is_org_admin = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    conversations = relationship("Conversation", back_populates="user", cascade="all, delete-orphan")
    model_settings = relationship("ModelSetting", back_populates="user", cascade="all, delete-orphan")
    data_sources = relationship("DataSource", back_populates="user", cascade="all, delete-orphan")
    vector_stores = relationship("VectorStore", back_populates="user", cascade="all, delete-orphan")
    
    # ETL Relationships
    etl_data_sources = relationship("ETLDataSource", back_populates="user", cascade="all, delete-orphan")
    linked_services = relationship("LinkedService", back_populates="user", cascade="all, delete-orphan")
    etl_pipelines = relationship("ETLPipeline", back_populates="user", cascade="all, delete-orphan")
    dashboards = relationship("Dashboard", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', org='{self.organization}')>"
