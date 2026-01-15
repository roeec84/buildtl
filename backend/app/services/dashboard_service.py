from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from sqlalchemy.orm import joinedload, selectinload
from typing import List, Optional
from app.models.dashboard import Dashboard, DashboardWidget
from app.schemas.chart import ChartConfig

class DashboardService:
    @staticmethod
    async def get_user_dashboards(db: AsyncSession, user_id: int) -> List[Dashboard]:
        result = await db.execute(
            select(Dashboard).where(Dashboard.user_id == user_id).order_by(Dashboard.updated_at.desc())
        )
        return result.scalars().all()

    @staticmethod
    async def get_dashboard(db: AsyncSession, dashboard_id: int, user_id: int) -> Optional[Dashboard]:
        result = await db.execute(
            select(Dashboard)
            .options(selectinload(Dashboard.widgets))
            .where(Dashboard.id == dashboard_id, Dashboard.user_id == user_id)
        )
        return result.scalar_one_or_none()

    @staticmethod
    async def create_dashboard(db: AsyncSession, user_id: int, title: str, description: str = None) -> Dashboard:
        dashboard = Dashboard(user_id=user_id, title=title, description=description, layout_config={})
        db.add(dashboard)
        await db.commit()
        await db.refresh(dashboard)
        return dashboard

    @staticmethod
    async def update_dashboard_layout(db: AsyncSession, dashboard_id: int, layout_config: dict) -> Dashboard:
        dashboard = await db.get(Dashboard, dashboard_id)
        if dashboard:
            dashboard.layout_config = layout_config
            await db.commit()
            await db.refresh(dashboard)
        return dashboard

    @staticmethod
    async def add_widget(db: AsyncSession, dashboard_id: int, title: str, widget_type: str, chart_config: dict, data_source_id: int = None) -> DashboardWidget:
        widget = DashboardWidget(
            dashboard_id=dashboard_id,
            title=title,
            widget_type=widget_type,
            chart_config=chart_config,
            data_source_id=data_source_id
        )
        db.add(widget)
        await db.commit()
        await db.refresh(widget)
        return widget
        
    @staticmethod
    async def delete_widget(db: AsyncSession, widget_id: int):
        widget = await db.get(DashboardWidget, widget_id)
        if widget:
            await db.delete(widget)
            await db.commit()
