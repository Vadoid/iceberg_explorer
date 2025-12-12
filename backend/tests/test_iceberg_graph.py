import pytest
from unittest.mock import MagicMock, patch
from app.services.iceberg import analyze_with_pyiceberg_metadata

@pytest.mark.asyncio
async def test_analyze_unpartitioned_table():
    """Test that unpartitioned tables return dataFiles directly under manifest"""
    bucket = "test-bucket"
    path = "test-table"
    
    # Mock PyIceberg components
    with patch("app.services.iceberg.read_iceberg_metadata_manual") as mock_read_metadata, \
         patch("app.services.iceberg.StaticTable") as mock_static_table:
        
        # Mock metadata
        mock_read_metadata.return_value = (
            {"location": f"gs://{bucket}/{path}", "current-snapshot-id": 123}, 
            f"gs://{bucket}/{path}/metadata/v1.metadata.json", 
            []
        )
        
        # Mock Table
        mock_table = MagicMock()
        mock_static_table.from_metadata.return_value = mock_table
        
        # Mock unpartitioned spec (empty fields)
        mock_spec = MagicMock()
        mock_spec.fields = []
        mock_table.spec.return_value = mock_spec
        
        # Mock current snapshot
        mock_snapshot = MagicMock()
        mock_snapshot.snapshot_id = 123
        mock_table.current_snapshot.return_value = mock_snapshot
        
        # Mock manifests
        mock_manifest = MagicMock()
        mock_manifest.manifest_path = f"gs://{bucket}/{path}/metadata/snap-123.avro"
        mock_snapshot.manifests.return_value = [mock_manifest]
        
        # Mock manifest entries (data files)
        mock_entry = MagicMock()
        mock_entry.status = 1 # ADDED
        mock_data_file = MagicMock()
        mock_data_file.file_path = f"gs://{bucket}/{path}/data/file1.parquet"
        mock_data_file.file_format = "PARQUET"
        mock_data_file.record_count = 100
        mock_data_file.file_size_in_bytes = 1024
        mock_entry.data_file = mock_data_file
        
        mock_manifest.fetch_manifest_entry.return_value = [mock_entry]
        
        # Run analysis
        result = analyze_with_pyiceberg_metadata(bucket, path)
        
        # Verify result
        assert result is not None
        assert "snapshots" in result
        snapshots = result["snapshots"]
        assert len(snapshots) > 0
        snapshot = snapshots[0]
        assert "manifests" in snapshot
        manifests = snapshot["manifests"]
        assert len(manifests) > 0
        manifest = manifests[0]
        
        # CRITICAL CHECK: dataFiles should be present directly, partitions should be missing or empty
        assert "dataFiles" in manifest
        assert len(manifest["dataFiles"]) == 1
        assert manifest["dataFiles"][0]["path"] == f"gs://{bucket}/{path}/data/file1.parquet"
        
        # Ensure partitions are NOT present or empty (depending on implementation details, 
        # but our change ensures we don't populate it for unpartitioned)
        # Actually, looking at the code, we didn't explicitly remove "partitions" key if it existed, 
        # but we didn't populate it.
        # Let's check if it's there.
        if "partitions" in manifest:
            assert len(manifest["partitions"]) == 0

@pytest.mark.asyncio
async def test_analyze_partitioned_table():
    """Test that partitioned tables still use partition grouping"""
    bucket = "test-bucket"
    path = "test-table-partitioned"
    
    # Mock PyIceberg components
    with patch("app.services.iceberg.read_iceberg_metadata_manual") as mock_read_metadata, \
         patch("app.services.iceberg.StaticTable") as mock_static_table:
        
        # Mock metadata
        mock_read_metadata.return_value = (
            {"location": f"gs://{bucket}/{path}", "current-snapshot-id": 123}, 
            f"gs://{bucket}/{path}/metadata/v1.metadata.json", 
            []
        )
        
        # Mock Table
        mock_table = MagicMock()
        mock_static_table.from_metadata.return_value = mock_table
        
        # Mock partitioned spec
        mock_spec = MagicMock()
        mock_field = MagicMock()
        mock_spec.fields = [mock_field] # Not empty
        mock_table.spec.return_value = mock_spec
        
        # Mock current snapshot
        mock_snapshot = MagicMock()
        mock_snapshot.snapshot_id = 123
        mock_table.current_snapshot.return_value = mock_snapshot
        
        # Mock manifests
        mock_manifest = MagicMock()
        mock_manifest.manifest_path = f"gs://{bucket}/{path}/metadata/snap-123.avro"
        mock_snapshot.manifests.return_value = [mock_manifest]
        
        # Mock manifest entries
        mock_entry = MagicMock()
        mock_entry.status = 1
        mock_data_file = MagicMock()
        mock_data_file.file_path = f"gs://{bucket}/{path}/data/file1.parquet"
        mock_data_file.partition = MagicMock() # Has partition
        mock_entry.data_file = mock_data_file
        
        mock_manifest.fetch_manifest_entry.return_value = [mock_entry]
        
        # Run analysis
        result = analyze_with_pyiceberg_metadata(bucket, path)
        
        # Verify result
        assert result is not None
        snapshot = result["snapshots"][0]
        manifest = snapshot["manifests"][0]
        
        # CRITICAL CHECK: partitions should be present
        assert "partitions" in manifest
        assert len(manifest["partitions"]) > 0
        # dataFiles should NOT be present (or empty)
        assert "dataFiles" not in manifest or len(manifest["dataFiles"]) == 0
