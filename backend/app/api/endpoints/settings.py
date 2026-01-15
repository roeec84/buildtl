"""
Settings endpoints for managing models, data sources, and vector stores.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Dict, List
from app.db.database import get_db
from app.models.user import User
from app.models.settings import ModelSetting, DataSource, VectorStore
from app.services.vector_store_service import VectorStoreFactory
from app.schemas.settings import (
    ModelSettingCreate,
    ModelSettingResponse,
    DataSourceCreate,
    DataSourceResponse,
    VectorStoreCreate,
    VectorStoreResponse,
    SettingsResponse
)
from app.api.deps import get_current_active_user
from app.core.security import encrypt_value

SENSITIVE_KEYS = {"token", "password", "secret", "api_key", "key"}


router = APIRouter(prefix="/api/settings", tags=["Settings"])


@router.get("", response_model=SettingsResponse)
async def get_settings(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all settings for the current user.

    Returns:
        All user settings (models, data sources, vector stores)
    """
    # Get model settings
    result = await db.execute(
        select(ModelSetting).where(ModelSetting.user_id == current_user.id)
    )
    model_settings = result.scalars().all()

    # Get data sources
    result = await db.execute(
        select(DataSource).where(DataSource.user_id == current_user.id)
    )
    data_sources = result.scalars().all()

    # Get vector stores
    result = await db.execute(
        select(VectorStore).where(VectorStore.user_id == current_user.id)
    )
    vector_stores = result.scalars().all()

    # Format response
    models_dict = {
        str(model.id): ModelSettingResponse.model_validate(model)
        for model in model_settings
    }

    # Group data sources by type
    data_sources_dict: Dict[str, List[DataSourceResponse]] = {}
    for ds in data_sources:
        ds_response = DataSourceResponse.model_validate(ds)
        if ds.type not in data_sources_dict:
            data_sources_dict[ds.type] = []
        data_sources_dict[ds.type].append(ds_response)

    stores_dict = {
        str(store.id): VectorStoreResponse.model_validate(store)
        for store in vector_stores
    }

    return SettingsResponse(
        models=models_dict,
        data_sources=data_sources_dict,
        stores=stores_dict
    )


@router.get("/models", response_model=List[ModelSettingResponse])
async def get_models(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all model settings for the current user.

    Returns:
        List of model settings
    """
    result = await db.execute(
        select(ModelSetting).where(ModelSetting.user_id == current_user.id)
    )
    models = result.scalars().all()

    return [ModelSettingResponse.model_validate(model) for model in models]


@router.post("/models", response_model=ModelSettingResponse, status_code=status.HTTP_201_CREATED)
async def create_model_setting(
    model_data: ModelSettingCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new model setting.

    Args:
        model_data: Model configuration
        current_user: Authenticated user
        db: Database session

    Returns:
        Created model setting
    """
    new_model = ModelSetting(
        user_id=current_user.id,
        name=model_data.name,
        display_name=model_data.display_name,
        provider=model_data.provider,
        api_key=model_data.api_key,
        temperature=model_data.temperature,
        max_tokens=model_data.max_tokens
    )

    db.add(new_model)
    await db.commit()
    await db.refresh(new_model)

    return ModelSettingResponse.model_validate(new_model)


@router.get("/data-sources", response_model=List[DataSourceResponse])
async def get_data_sources(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get all data sources for the current user.

    Returns:
        List of data sources
    """
    result = await db.execute(
        select(DataSource).where(DataSource.user_id == current_user.id)
    )
    data_sources = result.scalars().all()

    return [DataSourceResponse.model_validate(ds) for ds in data_sources]


@router.post("/data-sources", response_model=DataSourceResponse, status_code=status.HTTP_201_CREATED)
async def create_data_source(
    data_source_data: DataSourceCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new data source.

    Args:
        data_source_data: Data source configuration
        current_user: Authenticated user
        db: Database session

    Returns:
        Created data source
    """
    # Normalize name by removing spaces and add username prefix
    normalized_name = data_source_data.name.replace(" ", "")
    # Add username prefix to make collection unique per user
    collection_with_user = f"{current_user.username}_{normalized_name}"

    # Encrypt sensitive data in config
    processed_config = data_source_data.config.copy()
    for key, value in processed_config.items():
        if key in SENSITIVE_KEYS and value:
            processed_config[key] = encrypt_value(str(value))

    new_data_source = DataSource(
        user_id=current_user.id,
        name=collection_with_user,
        display_name=data_source_data.display_name,
        type=data_source_data.type,
        config=processed_config
    )

    db.add(new_data_source)
    await db.commit()
    await db.refresh(new_data_source)

    return DataSourceResponse.model_validate(new_data_source)


@router.post("/vector-stores", response_model=VectorStoreResponse, status_code=status.HTTP_201_CREATED)
async def create_vector_store(
    store_data: VectorStoreCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new vector store configuration.

    Args:
        store_data: Vector store configuration
        current_user: Authenticated user
        db: Database session

    Returns:
        Created vector store configuration
    """
    new_store = VectorStore(
        user_id=current_user.id,
        name=store_data.name,
        type=store_data.type,
        config=store_data.config
    )

    db.add(new_store)
    await db.commit()
    await db.refresh(new_store)

    return VectorStoreResponse.model_validate(new_store)


@router.put("/models/{model_id}", response_model=ModelSettingResponse)
async def update_model_setting(
    model_id: int,
    model_data: ModelSettingCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """Update an existing model setting."""
    result = await db.execute(
        select(ModelSetting)
        .where(ModelSetting.id == model_id)
        .where(ModelSetting.user_id == current_user.id)
    )
    model = result.scalar_one_or_none()

    if not model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model setting not found"
        )

    model.name = model_data.name
    model.display_name = model_data.display_name
    model.provider = model_data.provider
    model.api_key = model_data.api_key
    model.temperature = model_data.temperature
    model.max_tokens = model_data.max_tokens

    await db.commit()
    await db.refresh(model)

    return ModelSettingResponse.model_validate(model)


@router.delete("/models/{model_id}")
async def delete_model_setting(
    model_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """Delete a model setting."""
    result = await db.execute(
        select(ModelSetting)
        .where(ModelSetting.id == model_id)
        .where(ModelSetting.user_id == current_user.id)
    )
    model = result.scalar_one_or_none()

    if not model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model setting not found"
        )

    await db.delete(model)
    await db.commit()

    return {"message": "Model setting deleted successfully"}


@router.put("/data-sources/{data_source_id}", response_model=DataSourceResponse)
async def update_data_source(
    data_source_id: int,
    data_source_data: DataSourceCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """Update an existing data source."""
    result = await db.execute(
        select(DataSource)
        .where(DataSource.id == data_source_id)
        .where(DataSource.user_id == current_user.id)
    )
    data_source = result.scalar_one_or_none()

    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )

    # Normalize name by removing spaces and add username prefix
    normalized_name = data_source_data.name.replace(" ", "")
    # Add username prefix to make collection unique per user
    collection_with_user = f"{current_user.username}_{normalized_name}"

    # Encrypt sensitive data in config
    processed_config = data_source_data.config.copy()
    for key, value in processed_config.items():
        if key in SENSITIVE_KEYS and value:
            processed_config[key] = encrypt_value(str(value))

    data_source.name = collection_with_user
    data_source.display_name = data_source_data.display_name
    data_source.type = data_source_data.type
    data_source.config = processed_config

    await db.commit()
    await db.refresh(data_source)

    return DataSourceResponse.model_validate(data_source)


@router.delete("/data-sources/{data_source_id}")
async def delete_data_source(
    data_source_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """Delete a data source."""
    result = await db.execute(
        select(DataSource)
        .where(DataSource.id == data_source_id)
        .where(DataSource.user_id == current_user.id)
    )
    data_source = result.scalar_one_or_none()

    if not data_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data source not found"
        )

    # Delete vector store collection
    try:
        if data_source.name:
            vector_store = VectorStoreFactory.create_for_user(
                user_id=current_user.id,
                collection_name=data_source.name
            )
            await vector_store.delete()
    except Exception as e:
        print(f"Error deleting vector store collection for data source {data_source.id}: {e}")

    await db.delete(data_source)
    await db.commit()

    return {"message": "Data source deleted successfully"}
