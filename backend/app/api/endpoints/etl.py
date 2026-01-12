from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from app.db.database import get_db
from app.models.user import User
from app.models.etl import ETLDataSource, ETLPipeline, ETLExecution, LinkedService
from app.api.deps import get_current_active_user
from app.core.security import encrypt_value

router = APIRouter(prefix="/api/etl", tags=["ETL"])

# Schemas (Internal for now, should move to schemas/etl.py later)
class LinkedServiceCreate(BaseModel):
    name: str
    description: Optional[str] = None
    service_type: str
    connection_config: dict

class LinkedServiceResponse(LinkedServiceCreate):
    id: int
    created_at: datetime
    class Config:
        from_attributes = True

class LinkedServiceTestRequest(BaseModel):
    service_type: str
    connection_config: dict

class ETLDataSourceCreate(BaseModel):
    name: str
    description: Optional[str] = None
    linked_service_id: int
    table_name: str

class ETLDataSourceResponse(ETLDataSourceCreate):
    id: int
    created_at: datetime
    linked_service: Optional[LinkedServiceResponse] = None
    class Config:
        from_attributes = True

class ETLPipelineCreate(BaseModel):
    name: str
    description: Optional[str] = None
    nodes: list
    edges: list

class ETLPipelineResponse(ETLPipelineCreate):
    id: int
    created_at: datetime
    updated_at: datetime
    class Config:
        from_attributes = True

class TransformationSource(BaseModel):
    datasource_id: int
    selected_columns: List[str]
    table_name: str

class TransformationPreviewRequest(BaseModel):
    sources: List[TransformationSource]
    transformation_prompt: str
    limit: int = 1000
    model_name: Optional[str] = "gpt-4o"

class TransformationPreviewResponse(BaseModel):
    columns: List[str]
    data: List[List]
    row_count: int
    generated_code: str
    source_schema: dict

# --- Data Source Endpoints ---

# --- Linked Service Endpoints ---
@router.post("/linked-services", response_model=LinkedServiceResponse, status_code=status.HTTP_201_CREATED)
async def create_linked_service(
    data: LinkedServiceCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    # Basic encryption for sensitive fields
    secure_config = data.connection_config.copy()
    if "password" in secure_config:
        secure_config["password"] = encrypt_value(secure_config["password"])
    
    ls = LinkedService(
        user_id=current_user.id,
        name=data.name,
        description=data.description,
        service_type=data.service_type,
        connection_config=secure_config
    )
    db.add(ls)
    await db.commit()
    await db.refresh(ls)
    return ls

@router.get("/linked-services", response_model=List[LinkedServiceResponse])
async def list_linked_services(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(LinkedService).where(LinkedService.user_id == current_user.id))
    return result.scalars().all()

@router.post("/linked-services/test")
async def test_linked_service_connection(
    data: LinkedServiceTestRequest,
    current_user: User = Depends(get_current_active_user)
):
    from app.services.etl_service import ETLService
    # For testing, we just check minimal connectivity. 
    # Logic in service might try to query meta-tables or just "SELECT 1".
    # Since we don't have a table name here, our current test_connection expects a table_name.
    # I should update ETLService.test_connection to handle optional table_name or check `SELECT 1`.
    
    # Passing dummy table name or updating service?
    # Better to update service later. For now, try checking without table if supported?
    # Or for SQL, use a dummy query.
    
    # Let's assume user provides a table for dataset creation test, but for Linked Service?
    # Linked Service is connection only. 
    # Postgres/MySQL: can connect without table.
    # BigQuery: Needs project/dataset.
    
    # I will pass "1" or None as table_name and handle in service.
    success, message = await ETLService.test_connection(data.service_type, data.connection_config, table_name=None)
    return {"success": success, "message": message}

# --- Data Source Endpoints ---

@router.post("/datasources", response_model=ETLDataSourceResponse, status_code=status.HTTP_201_CREATED)
async def create_etl_data_source(
    data: ETLDataSourceCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    # Verify Linked Service exists and belongs to user
    result = await db.execute(select(LinkedService).where(LinkedService.id == data.linked_service_id, LinkedService.user_id == current_user.id))
    ls = result.scalar_one_or_none()
    if not ls:
        raise HTTPException(status_code=404, detail="Linked Service not found")

    ds = ETLDataSource(
        user_id=current_user.id,
        name=data.name,
        description=data.description,
        linked_service_id=data.linked_service_id,
        table_name=data.table_name
    )
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    # Manually populate relationship to avoid lazy load error in response validation
    ds.linked_service = ls
    return ds

@router.get("/datasources", response_model=List[ETLDataSourceResponse])
async def list_etl_data_sources(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(ETLDataSource).options(joinedload(ETLDataSource.linked_service)).where(ETLDataSource.user_id == current_user.id)
    )
    return result.scalars().all()

@router.delete("/datasources/{id}")
async def delete_etl_data_source(
    id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(ETLDataSource).where(ETLDataSource.id == id).where(ETLDataSource.user_id == current_user.id)
    )
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Data source not found")
        
    await db.delete(ds)
    await db.commit()
    return {"message": "Deleted successfully"}

# --- Pipeline Endpoints ---

@router.post("/pipelines", response_model=ETLPipelineResponse)
async def create_pipeline(
    data: ETLPipelineCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    pipeline = ETLPipeline(
        user_id=current_user.id,
        name=data.name,
        description=data.description,
        nodes=data.nodes,
        edges=data.edges
    )
    db.add(pipeline)
    await db.commit()
    await db.refresh(pipeline)
    return pipeline

@router.get("/pipelines", response_model=List[ETLPipelineResponse])
async def list_pipelines(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(select(ETLPipeline).where(ETLPipeline.user_id == current_user.id))
    return result.scalars().all()

@router.get("/pipelines/{id}", response_model=ETLPipelineResponse)
async def get_pipeline(
    id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(ETLPipeline).where(ETLPipeline.id == id).where(ETLPipeline.user_id == current_user.id)
    )
    pipeline = result.scalar_one_or_none()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return pipeline

@router.put("/pipelines/{id}", response_model=ETLPipelineResponse)
async def update_pipeline(
    id: int,
    data: ETLPipelineCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(ETLPipeline).where(ETLPipeline.id == id).where(ETLPipeline.user_id == current_user.id)
    )
    pipeline = result.scalar_one_or_none()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
        
    pipeline.name = data.name
    pipeline.description = data.description
    pipeline.nodes = data.nodes
    pipeline.edges = data.edges
    
    await db.commit()
    await db.refresh(pipeline)
    return pipeline

@router.delete("/pipelines/{id}")
async def delete_pipeline(
    id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    result = await db.execute(
        select(ETLPipeline).where(ETLPipeline.id == id).where(ETLPipeline.user_id == current_user.id)
    )
    pipeline = result.scalar_one_or_none()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
        
    await db.delete(pipeline)
    await db.commit()
    return {"message": "Deleted successfully"}

@router.post("/pipelines/{id}/run")
async def run_pipeline(
    id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    from app.services.etl_service import ETLService
    
    try:
        # Run execution (in-process for MVP)
        result = await ETLService.execute_pipeline(id, db)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")



class ETLExecutionResponse(BaseModel):
    id: int
    pipeline_id: int
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    logs: Optional[str] = None
    class Config:
        from_attributes = True

@router.get("/executions", response_model=List[ETLExecutionResponse])
async def list_executions(
    pipeline_id: Optional[int] = None,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    query = select(ETLExecution).join(ETLPipeline).where(ETLPipeline.user_id == current_user.id)
    
    if pipeline_id:
        query = query.where(ETLExecution.pipeline_id == pipeline_id)
        
    query = query.order_by(ETLExecution.started_at.desc())
    
    result = await db.execute(query)
    return result.scalars().all()

@router.get("/datasources/{id}/schema")
async def get_datasource_schema(
    id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get table schema (columns, types) for an ETL data source.
    """
    from app.services.etl_service import ETLService
    
    # Fetch datasource
    result = await db.execute(select(ETLDataSource).options(joinedload(ETLDataSource.linked_service)).where(ETLDataSource.id == id))
    datasource = result.scalar_one_or_none()
    
    if not datasource:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    if datasource.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    try:
        schema = await ETLService.get_table_schema(datasource, db)
        return {"columns": schema}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch schema: {str(e)}")


@router.post("/datasources/test-connection")
async def test_connection(
    request: dict,
    current_user: User = Depends(get_current_active_user),
):
    """
    Test database connection before creating a data source.
    Request body should contain: db_type, connection_config, table_name
    """
    from app.services.etl_service import ETLService
    try:
        db_type = request.get("db_type")
        connection_config = request.get("connection_config")
        table_name = request.get("table_name")
        
        if not all([db_type, connection_config, table_name]):
            raise HTTPException(status_code=400, detail="Missing required fields")
        
        # Test connection
        success, message = await ETLService.test_connection(db_type, connection_config, table_name)
        
        return {
            "success": success,
            "message": message
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": f"Connection test failed: {str(e)}"
        }


@router.post("/transformations/preview", response_model=TransformationPreviewResponse)
async def preview_transformation(
    request: TransformationPreviewRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Preview transformation results before executing full pipeline.
    Generates PySpark code from natural language and executes on sample data.
    """
    from app.services.etl_service import ETLService
    
    try:
        result = await ETLService.preview_transformation(
            sources=request.sources,
            transformation_prompt=request.transformation_prompt,
            db_session=db,
            user_id=current_user.id,
            limit=request.limit,
            model_name=request.model_name
        )
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")
