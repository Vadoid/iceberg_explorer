"""
FastAPI backend for Iceberg Explorer
Handles GCS bucket access and Iceberg table analysis
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import projects, buckets, analyze

app = FastAPI(title="Iceberg Explorer API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(projects.router, tags=["projects"])
app.include_router(buckets.router, tags=["buckets"])
app.include_router(analyze.router, tags=["analyze"])

@app.get("/")
async def root():
    return {"message": "Iceberg Explorer API", "version": "1.0.0"}
