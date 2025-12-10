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
# Include routers with API prefix to match dispatch.yaml routing
API_PREFIX = "/api/backend"

app.include_router(projects.router, prefix=API_PREFIX, tags=["projects"])
app.include_router(buckets.router, prefix=API_PREFIX, tags=["buckets"])
app.include_router(analyze.router, prefix=API_PREFIX, tags=["analyze"])

from fastapi import Request
from fastapi.responses import JSONResponse
from google.auth.exceptions import RefreshError, DefaultCredentialsError, TransportError
from google.api_core.exceptions import Unauthorized, Forbidden

@app.exception_handler(RefreshError)
@app.exception_handler(DefaultCredentialsError)
@app.exception_handler(TransportError)
@app.exception_handler(Unauthorized)
@app.exception_handler(Forbidden)
async def auth_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=401,
        content={"detail": "Authentication failed. Please login again.", "error": str(exc)},
    )

@app.get("/")
async def root():
    return {"message": "Iceberg Explorer API", "version": "1.0.0"}
