from sqlalchemy import Column, Integer, String, Text, ForeignKey, JSON, DateTime, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.database import Base

class LinkedService(Base):
    """
    Represents a reusable connection to an external system (DB, API, etc.)
    """
    __tablename__ = "etl_linked_services"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    
    # Connection details
    service_type = Column(String, nullable=False)  # 'postgresql', 'bigquery', etc.
    connection_config = Column(JSON, nullable=False) # Host, port, credentials
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="linked_services")
    data_sources = relationship("ETLDataSource", back_populates="linked_service", cascade="all, delete-orphan")


class ETLDataSource(Base):
    """
    Represents a specific dataset (table/query) derived from a Linked Service.
    """
    __tablename__ = "etl_data_sources"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    linked_service_id = Column(Integer, ForeignKey("etl_linked_services.id", ondelete="CASCADE"), nullable=False)
    
    name = Column(String, nullable=False) # Display name of the dataset
    description = Column(String, nullable=True)
    
    # Specifics
    table_name = Column(String, nullable=True)
    schema_json = Column(JSON, nullable=True) # Cached schema information
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="etl_data_sources")
    linked_service = relationship("LinkedService", back_populates="data_sources")
    pipelines = relationship("ETLPipeline", back_populates="data_source", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ETLDataSource(id={self.id}, name='{self.name}', table='{self.table_name}')>"


class ETLPipeline(Base):
    """
    Represents an ETL workflow graph.
    """
    __tablename__ = "etl_pipelines"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    
    # React Flow Graph
    nodes = Column(JSON, default=list)
    edges = Column(JSON, default=list)
    
    # Linking if this pipeline is built primarily around a specific source? 
    # Or purely independent? Let's make it independent usually.
    # But for "Data Factory" style, maybe input/output are just nodes.
    # We might keep a reference if created from a source.
    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", back_populates="etl_pipelines")
    data_source = relationship("ETLDataSource", back_populates="pipelines")
    executions = relationship("ETLExecution", back_populates="pipeline", cascade="all, delete-orphan")


class ETLExecution(Base):
    """
    Tracks execution history of an ETL pipeline.
    """
    __tablename__ = "etl_executions"

    id = Column(Integer, primary_key=True, index=True)
    pipeline_id = Column(Integer, ForeignKey("etl_pipelines.id", ondelete="CASCADE"), nullable=False)
    
    status = Column(String, nullable=False) # 'pending', 'running', 'completed', 'failed'
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    
    logs = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    
    pipeline = relationship("ETLPipeline", back_populates="executions")
