import pytest
from unittest.mock import patch, MagicMock

@pytest.mark.asyncio
async def test_search_iceberg_tables(client):
    """Test search_iceberg_tables endpoint with mocked BigQuery client"""
    project_id = "test-project"
    
    # Mock BigQuery Client and its methods
    with patch("app.routers.bigquery.get_bigquery_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        # Mock datasets
        mock_dataset = MagicMock()
        mock_dataset.dataset_id = "test_dataset"
        mock_client.list_datasets.return_value = [mock_dataset]
        
        # Mock tables
        mock_table = MagicMock()
        mock_table.table_id = "test_table"
        mock_table.table_type = "EXTERNAL"
        mock_table.full_table_id = "test-project.test_dataset.test_table"
        
        # Mock table list iterator
        mock_client.list_tables.return_value = [mock_table]
        
        # Mock table details (get_table)
        mock_table_details = MagicMock()
        mock_table_details.table_id = "test_table"
        mock_table_details.table_type = "EXTERNAL"
        from datetime import datetime, timezone
        mock_table_details.created = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        
        # Mock external configuration
        mock_ext_config = MagicMock()
        mock_ext_config.source_format = "ICEBERG"
        mock_ext_config.source_uris = ["gs://bucket/path"]
        mock_table_details.external_data_configuration = mock_ext_config
        
        mock_client.get_table.return_value = mock_table_details
        
        # Call the endpoint
        response = await client.get(f"/api/backend/bigquery/search-iceberg?project_id={project_id}")
        
        # Verify response
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")
        assert response.status_code == 200
        data = response.json()
        assert "tables" in data
        tables = data["tables"]
        assert len(tables) == 1
        assert tables[0]["dataset_id"] == "test_dataset"
        assert tables[0]["table_id"] == "test_table"
        assert tables[0]["location"] == "gs://bucket/path"

@pytest.mark.asyncio
async def test_search_iceberg_tables_error_handling(client):
    """Test search_iceberg_tables gracefully handles errors"""
    project_id = "test-project"
    
    with patch("app.routers.bigquery.get_bigquery_client") as mock_get_client:
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        # Mock list_datasets to raise an exception
        mock_client.list_datasets.side_effect = Exception("API Error")
        
        # Call the endpoint - should return 500 but handled
        response = await client.get(f"/api/backend/bigquery/search-iceberg?project_id={project_id}")
        
        # The endpoint catches exceptions and raises HTTPException 500
        assert response.status_code == 500
        assert "Error searching Iceberg tables" in response.json()["detail"]
