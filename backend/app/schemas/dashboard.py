from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class DashboardWidgetBase(BaseModel):
    title: str
    widget_type: str = Field(..., description="'chart', 'text'")
    chart_config: Optional[Dict[str, Any]] = None
    data_source_id: Optional[int] = None
    layout: Optional[Dict[str, Any]] = None # x, y, w, h within the widget (optional)

class DashboardWidgetCreate(DashboardWidgetBase):
    pass

class DashboardWidget(DashboardWidgetBase):
    id: int
    dashboard_id: int
    created_at: datetime

    class Config:
        from_attributes = True

class DashboardBase(BaseModel):
    title: str
    description: Optional[str] = None
    layout_config: Optional[Dict[str, Any]] = Field(default={}, description="Layout and widgets configuration")

class DashboardCreate(DashboardBase):
    pass

class DashboardUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    layout_config: Optional[Dict[str, Any]] = None

class Dashboard(DashboardBase):
    id: int
    user_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    widgets: List[DashboardWidget] = []

    class Config:
        from_attributes = True
