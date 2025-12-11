import pytest
from unittest.mock import patch
from google.api_core.exceptions import Unauthorized, Forbidden
from google.auth.exceptions import RefreshError

@pytest.mark.asyncio
async def test_auth_unauthorized(client):
    """Test that Unauthorized exception returns 401"""
    with patch("app.routers.buckets.get_storage_client") as mock_get_client:
        mock_get_client.side_effect = Unauthorized("Invalid token")
        
        response = await client.get("/api/backend/buckets")
        
        assert response.status_code == 401
        assert response.json()["detail"] == "Authentication failed. Please login again."

@pytest.mark.asyncio
async def test_auth_forbidden(client):
    """Test that Forbidden exception returns 403"""
    with patch("app.routers.buckets.get_storage_client") as mock_get_client:
        mock_get_client.side_effect = Forbidden("Access denied")
        
        response = await client.get("/api/backend/buckets")
        
        assert response.status_code == 403
        assert response.json()["detail"] == "Access denied. You do not have permission to access this resource."

@pytest.mark.asyncio
async def test_auth_refresh_error(client):
    """Test that RefreshError returns 401"""
    with patch("app.routers.buckets.get_storage_client") as mock_get_client:
        mock_get_client.side_effect = RefreshError("Token expired")
        
        response = await client.get("/api/backend/buckets")
        
        assert response.status_code == 401
        assert response.json()["detail"] == "Authentication failed. Please login again."
