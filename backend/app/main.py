"""
Main FastAPI application.
This is the entry point for the backend server.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.core.config import settings
from app.db.database import init_db
from app.api.endpoints import auth, chat, files, datasource
from app.api.endpoints import settings as settings_router
from app.api.endpoints import etl, dashboard


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    This replaces the deprecated @app.on_event decorators.
    """
    print(f"Environment: {settings.ENVIRONMENT}")
    print(f"SECRET_KEY (first 10 chars): {settings.SECRET_KEY[:10]}...")
    await init_db()
    print("✅ Database initialized")

    yield

    print("👋 Shutting down...")


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    description="Modern ETL platform with LLM integration",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router)
app.include_router(chat.router)
app.include_router(settings_router.router)
app.include_router(files.router)
app.include_router(datasource.router)
app.include_router(etl.router)
app.include_router(dashboard.router, prefix="/api/dashboards", tags=["dashboards"])

@app.get("/")
async def root():
    """Root endpoint - health check"""
    return {
        "message": "BuildTL",
        "version": settings.VERSION,
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
