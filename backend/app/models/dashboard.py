from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, JSON
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.database import Base

class Dashboard(Base):
    __tablename__ = "dashboards"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    title = Column(String, index=True)
    description = Column(String, nullable=True)
    layout_config = Column(JSON, default={}) # Stores React Grid Layout structure
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="dashboards")
    widgets = relationship("DashboardWidget", back_populates="dashboard", cascade="all, delete-orphan")
    conversations = relationship("Conversation", back_populates="dashboard", cascade="all, delete-orphan")

class DashboardWidget(Base):
    __tablename__ = "dashboard_widgets"

    id = Column(Integer, primary_key=True, index=True)
    dashboard_id = Column(Integer, ForeignKey("dashboards.id"), nullable=False)
    title = Column(String)
    widget_type = Column(String) # 'chart', 'text', 'metric'
    chart_config = Column(JSON, nullable=True) # The ChartConfig JSON
    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=True)
    
    # Layout props specific to this widget (x, y, w, h)
    # Stored here or in dashboard.layout_config? 
    # Usually better to separate content from layout, but layout often linked to widget ID.
    # We will store core config here. Position is in dashboard.layout_config.

    created_at = Column(DateTime, default=datetime.utcnow)
    
    dashboard = relationship("Dashboard", back_populates="widgets")
    data_source = relationship("ETLDataSource")
