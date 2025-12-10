import pytest
from unittest.mock import patch

@pytest.mark.asyncio
async def test_root(client):
    response = await client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Iceberg Explorer API", "version": "1.0.0"}

@pytest.mark.asyncio
async def test_analyze_table_mocked(client):
    """Test analyze_table endpoint with mocked dependencies"""
    bucket = "test-bucket"
    path = "test-table"
    
    # Mock the analyze_with_pyiceberg_metadata function to avoid actual GCS calls
    with patch("app.routers.analyze.analyze_with_pyiceberg_metadata") as mock_analyze:
        mock_analyze.return_value = {
            "tableName": "test-table",
            "location": f"gs://{bucket}/{path}",
            "formatVersion": 2,
            "schema": [],
            "partitionSpec": [],
            "snapshots": []
        }
        
        # Call the endpoint
        response = await client.get(f"/analyze?bucket={bucket}&path={path}")
        
        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["tableName"] == "test-table"
        assert data["location"] == f"gs://{bucket}/{path}"
        
        # Verify mock was called with correct arguments (including token=None since we didn't send one)
        mock_analyze.assert_called_once()
        args, kwargs = mock_analyze.call_args
        assert args[0] == bucket
        assert args[1] == path
        assert kwargs.get("token") is None

@pytest.mark.asyncio
async def test_analyze_table_with_token(client):
    """Test analyze_table endpoint propagates token"""
    bucket = "test-bucket"
    path = "test-table"
    token = "fake-token"
    
    with patch("app.routers.analyze.analyze_with_pyiceberg_metadata") as mock_analyze:
        mock_analyze.return_value = {"tableName": "test-table"}
        
        # Call with Authorization header
        headers = {"Authorization": f"Bearer {token}"}
        response = await client.get(f"/analyze?bucket={bucket}&path={path}", headers=headers)
        
        assert response.status_code == 200
        
        # Verify token was passed to analysis function
        mock_analyze.assert_called_once()
        _, kwargs = mock_analyze.call_args
        assert kwargs.get("token") == token
