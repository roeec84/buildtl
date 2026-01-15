from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from typing import List

from app.db.database import get_db
from app.models.user import User
from app.api.deps import get_current_active_user
from app.models.dashboard import Dashboard, DashboardWidget
from app.schemas.dashboard import Dashboard as DashboardSchema, DashboardCreate, DashboardUpdate, DashboardWidgetCreate

router = APIRouter()

@router.get("/", response_model=List[DashboardSchema])
async def get_dashboards(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    skip: int = 0,
    limit: int = 100
):
    """
    Retrieve dashboards for the current user.
    """
    stmt = (
        select(Dashboard)
        .where(Dashboard.user_id == current_user.id)
        .options(selectinload(Dashboard.widgets))
        .offset(skip)
        .limit(limit)
    )
    result = await db.execute(stmt)
    dashboards = result.scalars().all()
    return dashboards

@router.post("/", response_model=DashboardSchema)
async def create_dashboard(
    dashboard_in: DashboardCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Create a new dashboard.
    """
    dashboard = Dashboard(
        title=dashboard_in.title,
        description=dashboard_in.description,
        layout_config=dashboard_in.layout_config,
        user_id=current_user.id
    )
    db.add(dashboard)
    await db.commit()
    await db.refresh(dashboard)
    
    # Re-fetch with widgets (empty initially) to match schema
    stmt = select(Dashboard).where(Dashboard.id == dashboard.id).options(selectinload(Dashboard.widgets))
    result = await db.execute(stmt)
    return result.scalar_one()

@router.get("/{dashboard_id}", response_model=DashboardSchema)
async def get_dashboard(
    dashboard_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Get a specific dashboard by ID.
    """
    stmt = (
        select(Dashboard)
        .where(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .options(selectinload(Dashboard.widgets))
    )
    result = await db.execute(stmt)
    dashboard = result.scalars().first()
    
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
        
    return dashboard

@router.put("/{dashboard_id}", response_model=DashboardSchema)
async def update_dashboard(
    dashboard_id: int,
    dashboard_in: DashboardUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Update a dashboard.
    """
    stmt = (
        select(Dashboard)
        .where(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .options(selectinload(Dashboard.widgets))
    )
    result = await db.execute(stmt)
    dashboard = result.scalars().first()
    
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
        
    if dashboard_in.title is not None:
        dashboard.title = dashboard_in.title
    if dashboard_in.description is not None:
        dashboard.description = dashboard_in.description
    if dashboard_in.layout_config is not None:
        # Pydantic handles JSON validation usually, but we assume it's valid JSON
        # Need to ensure we replace the existing structure or merge?
        # Usually full replacement for layout state
        dashboard.layout_config = dashboard_in.layout_config
        
    await db.commit()
    await db.refresh(dashboard)
    return dashboard

@router.post("/{dashboard_id}/widgets", response_model=DashboardSchema)
async def add_widget(
    dashboard_id: int,
    widget_in: DashboardWidgetCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Add a widget to a dashboard (Alternative to saving layout config, if we store widgets separately).
    Currently the plan is to store layout in dashboard.layout_config, 
    but we might also want to persist widget content if it's not regenerated every time.
    """
    # Verify dashboard ownership
    stmt = select(Dashboard).where(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
    result = await db.execute(stmt)
    dashboard = result.scalars().first()
    
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")

    widget = DashboardWidget(
        dashboard_id=dashboard_id,
        title=widget_in.title,
        widget_type=widget_in.widget_type,
        chart_config=widget_in.chart_config,
        data_source_id=widget_in.data_source_id
    )
    db.add(widget)
    await db.commit()
    await db.refresh(dashboard) # Refresh dashboard to get updated widgets list?
    
    # Easier to just re-fetch dashboard
    stmt = (
        select(Dashboard)
        .where(Dashboard.id == dashboard_id)
        .options(selectinload(Dashboard.widgets))
    )
    result = await db.execute(stmt)
    return result.scalar_one()

@router.delete("/{dashboard_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dashboard(
    dashboard_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Delete a dashboard.
    """
    stmt = select(Dashboard).where(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
    result = await db.execute(stmt)
    dashboard = result.scalars().first()
    
    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found")
        
    await db.delete(dashboard)
    await db.commit()
    return None
