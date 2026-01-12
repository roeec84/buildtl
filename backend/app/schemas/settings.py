"""
Pydantic schemas for settings (models, data sources, vector stores).
"""
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional


class ModelSettingBase(BaseModel):
    """Base schema for model settings"""
    name: str
    display_name: str = Field(alias="displayName")
    provider: str
    temperature: float = 0.7
    max_tokens: int = 2000

    model_config = {"populate_by_name": True}


class ModelSettingCreate(ModelSettingBase):
    """Schema for creating a model setting"""
    api_key: Optional[str] = None


class ModelSettingResponse(ModelSettingBase):
    """Response schema for model setting"""
    id: int

    model_config = {
        "from_attributes": True,
        "populate_by_name": True
    }


class DataSourceBase(BaseModel):
    """Base schema for data source"""
    name: str
    display_name: str = Field(alias="displayName")
    type: str
    config: Dict[str, Any] = {}

    model_config = {"populate_by_name": True}


class DataSourceCreate(DataSourceBase):
    """Schema for creating a data source"""
    pass


class DataSourceResponse(DataSourceBase):
    """Response schema for data source"""
    id: int

    model_config = {
        "from_attributes": True,
        "populate_by_name": True
    }


class VectorStoreBase(BaseModel):
    """Base schema for vector store"""
    name: str
    type: str
    config: Dict[str, Any] = {}


class VectorStoreCreate(VectorStoreBase):
    """Schema for creating a vector store"""
    pass


class VectorStoreResponse(VectorStoreBase):
    """Response schema for vector store"""
    id: int

    model_config = {"from_attributes": True}


class SettingsResponse(BaseModel):
    """Response schema for all user settings"""
    models: Dict[str, ModelSettingResponse]
    data_sources: Dict[str, list[DataSourceResponse]]
    stores: Dict[str, VectorStoreResponse]
