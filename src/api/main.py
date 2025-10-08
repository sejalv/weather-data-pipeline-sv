"""
FastAPI application for Weather Data Pipeline
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.endpoints import health, weather

app = FastAPI(
    title="Weather Data Pipeline API",
    description="API for accessing processed weather data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(weather.router, prefix="/weather", tags=["weather"])

@app.get("/")
async def root():
    return {"message": "Weather Data Pipeline API", "version": "1.0.0"}
