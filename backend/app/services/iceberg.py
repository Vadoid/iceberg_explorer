from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime
from .gcs import get_storage_client

# Try to import PyIceberg for proper metadata parsing
try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import Table, StaticTable
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.schema import Schema
    from pyiceberg.typedef import Record
    import pyarrow.fs as pafs
    PYICEBERG_AVAILABLE = True
except ImportError as e:
    PYICEBERG_AVAILABLE = False
    print(f"Warning: PyIceberg not available: {e}. Using manual metadata parsing.")

# Check for fastavro availability
try:
    import fastavro
    FASTAVRO_AVAILABLE = True
    print(f"fastavro available, version: {fastavro.__version__}")
except ImportError:
    FASTAVRO_AVAILABLE = False
    print("Warning: fastavro not available. Avro manifest files cannot be parsed.")


def get_manifest_files(bucket: str, path: str, manifest_list_path: str, project_id: Optional[str] = None, token: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get data files from manifest list using PyIceberg or fastavro for Avro parsing"""
    try:
        client = get_storage_client(project_id=project_id, token=token)
        bucket_obj = client.bucket(bucket)
        
        # Normalize manifest list path (remove gs:// prefix if present)
        manifest_path_clean = manifest_list_path.replace(f"gs://{bucket}/", "").lstrip("/")
        
        # Download manifest list
        manifest_list_blob = bucket_obj.blob(manifest_path_clean)
        
        # Try to parse as Avro (Iceberg manifest lists are Avro files)
        manifest_list_data = None
        
        # Use fastavro for Avro parsing (PyIceberg AvroFile is not available in this version)
        # fastavro is working well and is the preferred method
        if manifest_list_data is None and FASTAVRO_AVAILABLE:
            try:
                from io import BytesIO
                
                manifest_bytes = manifest_list_blob.download_as_bytes()
                
                # fastavro.reader can read Avro files with embedded schema
                # Need to seek to beginning
                manifest_bytes_io = BytesIO(manifest_bytes)
                manifest_bytes_io.seek(0)
                manifest_list_data = list(fastavro.reader(manifest_bytes_io))
            except Exception as e:
                print(f"fastavro parsing failed: {str(e)}")
                import traceback
                print(traceback.format_exc())
                # Try JSON as last resort
                try:
                    manifest_list_content = manifest_list_blob.download_as_text()
                    manifest_list_data = json.loads(manifest_list_content)
                except Exception as json_err:
                    print(f"JSON fallback also failed: {json_err}")
                    return []
        elif manifest_list_data is None:
            print("ERROR: fastavro not available and PyIceberg Avro parsing failed. Cannot parse Avro manifest files.")
            print("Please install fastavro: pip install fastavro")
            return []
        
        if manifest_list_data is None or (isinstance(manifest_list_data, list) and len(manifest_list_data) == 0):
            return []
        
        data_files = []
        
        # Process manifest list - it's an Avro file with manifest entries
        # Each entry has a manifest_path pointing to a manifest file
        # Iceberg manifest list format: list of dicts with "manifest_path" field
        manifests = []
        if isinstance(manifest_list_data, list):
            manifests = manifest_list_data
        elif isinstance(manifest_list_data, dict):
            # Could be a single entry or wrapped
            if "manifests" in manifest_list_data:
                manifests = manifest_list_data["manifests"]
            else:
                manifests = [manifest_list_data]
        
        for idx, manifest_entry in enumerate(manifests):
            # Handle different manifest entry formats
            manifest_path = None
            if isinstance(manifest_entry, str):
                manifest_path = manifest_entry
            elif isinstance(manifest_entry, dict):
                # Try various field names for manifest path
                manifest_path = (
                    manifest_entry.get("manifest_path") or
                    manifest_entry.get("manifestPath") or
                    manifest_entry.get("path") or
                    manifest_entry.get("file_path") or
                    manifest_entry.get("filePath")
                )
            
            if not manifest_path:
                continue
                
            manifest_path_clean = manifest_path.replace(f"gs://{bucket}/", "").lstrip("/")
            
            try:
                # Download and parse manifest file (also Avro)
                manifest_blob = bucket_obj.blob(manifest_path_clean)
                manifest_data = None
                
                # Use fastavro for manifest parsing
                if FASTAVRO_AVAILABLE:
                    try:
                        from io import BytesIO
                        manifest_bytes = manifest_blob.download_as_bytes()
                        manifest_bytes_io = BytesIO(manifest_bytes)
                        manifest_bytes_io.seek(0)
                        manifest_data = list(fastavro.reader(manifest_bytes_io))
                    except Exception as e:
                        print(f"fastavro manifest parsing failed: {str(e)}")
                        import traceback
                        print(traceback.format_exc())
                        # Last resort: try JSON
                        try:
                            manifest_content = manifest_blob.download_as_text()
                            manifest_data = json.loads(manifest_content)
                        except Exception:
                            continue
                elif manifest_data is None:
                    print(f"ERROR: Cannot parse manifest {manifest_path_clean} - fastavro not available")
                    continue
                
                # Extract data files from manifest
                # Iceberg manifest format: list of entries, each with a "data_file" field
                entries = []
                if isinstance(manifest_data, list):
                    entries = manifest_data
                elif isinstance(manifest_data, dict):
                    # Could be wrapped or a single entry
                    if "entries" in manifest_data:
                        entries = manifest_data["entries"]
                    elif "data_file" in manifest_data or "dataFile" in manifest_data:
                        entries = [manifest_data]
                    else:
                        # Try to find any list-like structure
                        for key, value in manifest_data.items():
                            if isinstance(value, list):
                                entries = value
                                break
                        if not entries:
                            entries = [manifest_data]
                
                for entry_idx, entry in enumerate(entries):
                    # Handle different entry formats
                    if not isinstance(entry, dict):
                        continue
                    
                    # Avro format: data_file field contains the file info
                    # Try various field name variations
                    data_file = (
                        entry.get("data_file") or
                        entry.get("dataFile") or
                        entry  # If entry itself is the data file
                    )
                    
                    if not isinstance(data_file, dict):
                        continue
                    
                    # Extract file path - try various field names
                    file_path = (
                        data_file.get("file_path") or
                        data_file.get("filePath") or
                        data_file.get("path") or
                        data_file.get("content_path") or
                        data_file.get("contentPath")
                    )
                    
                    if not file_path:
                        continue
                    
                    # Extract partition - could be in various formats
                    partition = {}
                    partition_data = (
                        data_file.get("partition") or
                        data_file.get("partition_data") or
                        data_file.get("partitionData") or
                        {}
                    )
                    if isinstance(partition_data, dict):
                        # Serialize partition to handle datetime objects
                        partition = {}
                        for key, value in partition_data.items():
                            if isinstance(value, datetime):
                                partition[key] = value.isoformat()
                            elif hasattr(value, 'isoformat'):  # datetime-like objects
                                partition[key] = value.isoformat()
                            else:
                                partition[key] = value
                    
                    # Extract record count
                    record_count = (
                        data_file.get("record_count") or
                        data_file.get("recordCount") or
                        data_file.get("num_rows") or
                        data_file.get("numRows") or
                        entry.get("record_count") or
                        entry.get("recordCount") or
                        0
                    )
                    
                    # Extract file size
                    file_size = (
                        data_file.get("file_size_in_bytes") or
                        data_file.get("fileSizeInBytes") or
                        data_file.get("file_size") or
                        data_file.get("fileSize") or
                        data_file.get("length") or
                        entry.get("file_size_in_bytes") or
                        entry.get("fileSizeInBytes") or
                        0
                    )
                    
                    data_files.append({
                        "filePath": file_path,
                        "fileFormat": data_file.get("file_format") or data_file.get("fileFormat") or data_file.get("format") or "parquet",
                        "partition": partition,
                        "recordCount": int(record_count) if record_count else 0,
                        "fileSizeInBytes": int(file_size) if file_size else 0,
                        "columnSizes": data_file.get("column_sizes") or data_file.get("columnSizes") or {},
                        "valueCounts": data_file.get("value_counts") or data_file.get("valueCounts") or {},
                        "nullValueCounts": data_file.get("null_value_counts") or data_file.get("nullValueCounts") or {},
                    })
                
            except Exception as e:
                # Skip manifests that can't be read
                print(f"Warning: Could not read manifest {manifest_path_clean}: {str(e)}")
                import traceback
                print(traceback.format_exc())
                continue
        
        return data_files
    except Exception:
        # Return empty list if we can't read manifests
        return []


def read_iceberg_metadata_manual(bucket: str, path: str, project_id: Optional[str] = None, token: Optional[str] = None) -> Tuple[Dict[str, Any], str, List[Dict[str, Any]]]:
    """Manually read Iceberg metadata from GCS"""
    try:
        client = get_storage_client(project_id=project_id, token=token)
        bucket_obj = client.bucket(bucket)
        
        # Normalize path (remove leading/trailing slashes)
        normalized_path = path.strip("/")
        
        # Iceberg standard: metadata files are in metadata/ subdirectory
        # Try the standard path first: {table_path}/metadata/
        metadata_dir = f"{normalized_path}/metadata/"
        
        # List all files in the metadata directory
        metadata_files = []
        try:
            all_metadata_files = list(bucket_obj.list_blobs(prefix=metadata_dir))
            metadata_files = all_metadata_files
        except Exception as e:
            print(f"Error listing metadata directory {metadata_dir}: {str(e)}")
        
        # If no files found, try alternative paths
        if not metadata_files:
            alternative_paths = [
                f"{normalized_path}/metadata",  # Without trailing slash
                f"{normalized_path}metadata/",  # No separator
            ]
            for alt_path in alternative_paths:
                try:
                    files = list(bucket_obj.list_blobs(prefix=alt_path))
                    metadata_files.extend(files)
                except Exception:
                    continue
        
        # Filter for metadata JSON files
        # Iceberg format: v{number}.metadata.json or {number}-{hash}.metadata.json
        metadata_json_files = []
        for blob in metadata_files:
            blob_name = blob.name
            # Check if it's a metadata.json file
            if blob_name.endswith(".metadata.json"):
                # Extract the directory to ensure it's in a metadata/ directory
                dir_path = "/".join(blob_name.split("/")[:-1])
                if "metadata" in dir_path.lower():
                    metadata_json_files.append(blob)

        # If still no files, try broader search
        if not metadata_json_files:
            # Search for any .metadata.json files in the table path
            try:
                all_blobs = list(bucket_obj.list_blobs(prefix=normalized_path + "/"))
                metadata_json_files = [
                    blob for blob in all_blobs
                    if blob.name.endswith(".metadata.json")
                ]
            except Exception:
                pass
        
        if not metadata_json_files:
            # Provide helpful error message with detailed file listing
            available_files = []
            metadata_dir_files = []
            metadata_prefixes_to_check = [
                f"{normalized_path}/metadata/",
                f"{normalized_path}/metadata",
                f"{normalized_path}metadata/",
            ]
            
            try:
                # List files at the exact path
                sample_files = list(bucket_obj.list_blobs(prefix=normalized_path, max_results=20))
                available_files = [f.name for f in sample_files]
                
                # Also check metadata directory specifically
                for prefix in metadata_prefixes_to_check:
                    try:
                        meta_files = list(bucket_obj.list_blobs(prefix=prefix, max_results=20))
                        metadata_dir_files.extend([f.name for f in meta_files])
                    except Exception:
                        pass
            except Exception as e:
                pass
            
            error_msg = f"No metadata files found at path: {normalized_path}"
            error_msg += f"\n\nSearched prefixes: {', '.join(metadata_prefixes_to_check)}"
            
            if metadata_dir_files:
                error_msg += f"\n\nFiles found in metadata directories:\n" + "\n".join(metadata_dir_files[:10])
            elif available_files:
                error_msg += f"\n\nFiles found at path (first 10):\n" + "\n".join(available_files[:10])
            else:
                error_msg += f"\n\nNo files found at path: {normalized_path}"
                # Try parent directory
                try:
                    parent_path = "/".join(normalized_path.split("/")[:-1])
                    if parent_path:
                        parent_files = list(bucket_obj.list_blobs(prefix=parent_path + "/", max_results=10))
                        if parent_files:
                            error_msg += f"\n\nFiles in parent directory ({parent_path}):\n" + "\n".join([f.name for f in parent_files[:10]])
                except Exception:
                    pass
            
            raise FileNotFoundError(error_msg)
            
        # Parse metadata files to find the latest one and collect info
        latest_version = -1
        latest_metadata_blob = None
        latest_metadata_dict = {}
        
        metadata_files_info = []
        
        for blob in metadata_json_files:
            try:
                # Extract version from filename
                filename = blob.name.split("/")[-1]
                version = -1
                
                # Try v{version}.metadata.json format
                if filename.startswith("v") and ".metadata.json" in filename:
                    try:
                        version_part = filename.split(".")[0][1:]
                        version = int(version_part)
                    except ValueError:
                        pass
                
                # Try {version}-{uuid}.metadata.json format
                if version == -1 and "-" in filename:
                    try:
                        version_part = filename.split("-")[0]
                        version = int(version_part)
                    except ValueError:
                        pass
                
                # If version found, add to list
                file_info = {
                    "file": f"gs://{bucket}/{blob.name}",
                    "version": version,
                    "timestamp": blob.updated.timestamp() * 1000 if blob.updated else 0,
                    "currentSnapshotId": None,
                    "previousMetadataFile": None
                }
                
                metadata_files_info.append(file_info)
                
                if version > latest_version:
                    latest_version = version
                    latest_metadata_blob = blob
            except Exception as e:
                print(f"Error parsing metadata file {blob.name}: {str(e)}")
                continue
        
        # If we couldn't determine version from filename, use timestamp
        if latest_version == -1 and metadata_json_files:
            metadata_json_files.sort(key=lambda x: x.updated if x.updated else datetime.min, reverse=True)
            latest_metadata_blob = metadata_json_files[0]
        
        if not latest_metadata_blob:
            raise FileNotFoundError(f"Could not determine latest metadata file in {normalized_path}")
            
        # Read the latest metadata file
        json_content = latest_metadata_blob.download_as_text()
        latest_metadata_dict = json.loads(json_content)
        
        # Update the info for the latest file with actual content
        latest_file_path = f"gs://{bucket}/{latest_metadata_blob.name}"
        for info in metadata_files_info:
            if info["file"] == latest_file_path:
                info["currentSnapshotId"] = str(latest_metadata_dict.get("current-snapshot-id", -1))
                info["previousMetadataFile"] = latest_metadata_dict.get("previous-metadata-file")
                break
                
        return latest_metadata_dict, latest_file_path, metadata_files_info
        
    except Exception as e:
        import traceback
        error_detail = f"Failed to read metadata: {str(e)}"
        error_detail += f"\nPath: {path}"
        error_detail += f"\nBucket: {bucket}"
        error_detail += f"\nTraceback: {traceback.format_exc()}"
        raise Exception(error_detail)


def analyze_with_pyiceberg_metadata(bucket: str, path: str, project_id: Optional[str] = None, token: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Use PyIceberg's Table API to properly load and analyze Iceberg table"""
    if not PYICEBERG_AVAILABLE:
        return None
    
    try:
        normalized_path = path.strip("/")
        table_location = f"gs://{bucket}/{normalized_path}"
        
        # First, find the metadata file to get the exact table location
        metadata_dict, metadata_location, metadata_files = read_iceberg_metadata_manual(bucket, path, project_id, token)
        actual_table_location = metadata_dict.get("location", table_location)
        
        # Use PyIceberg's StaticTable to load the table directly from metadata
        try:
            # StaticTable.from_metadata expects the full path to the metadata file
            # We need to construct the full GCS path if it's not already
            if not metadata_location.startswith("gs://"):
                full_metadata_location = f"gs://{bucket}/{metadata_location}"
            else:
                full_metadata_location = metadata_location
                
            # Prepare properties with token if available
            properties = {}
            if token:
                properties["gcs.oauth2.token"] = token
                # Set expiration to 1 hour from now (in milliseconds)
                # PyArrow GcsFileSystem requires both token and expiration
                import time
                expiration_ms = int((time.time() + 3600) * 1000)
                properties["gcs.oauth2.token-expires-at"] = str(expiration_ms)
                
            print(f"Loading StaticTable from metadata: {full_metadata_location}")
            table = StaticTable.from_metadata(full_metadata_location, properties=properties)
            
            # Extract namespace and table name from path
            table_name = normalized_path.split("/")[-1] if "/" in normalized_path else normalized_path
            
            # Now use PyIceberg's API to get all information
            # Get schema
            schema = table.schema()
            schema_fields = []
            for field in schema.fields:
                schema_fields.append({
                    "id": field.field_id,
                    "name": field.name,
                    "type": str(field.field_type),
                    "required": field.required,
                    "doc": field.doc if hasattr(field, 'doc') else None,
                })
            
            # Get partition spec
            partition_spec = table.spec()
            partition_spec_fields = []
            for field in partition_spec.fields:
                partition_spec_fields.append({
                    "fieldId": field.field_id,
                    "sourceId": field.source_id,
                    "name": field.name,
                    "transform": str(field.transform),
                })
            
            # Get sort order
            sort_order_obj = table.sort_order()
            sort_order_fields = []
            for field in sort_order_obj.fields:
                sort_order_fields.append({
                    "orderId": field.direction,
                    "direction": "asc" if field.direction == 1 else "desc",
                    "nullOrder": "nulls-first" if field.null_order == 1 else "nulls-last",
                    "sortFieldId": field.source_id,
                })
            
            # Scan the table to get all data files and partitions
            scan_builder = table.scan()
            file_scan_tasks = scan_builder.plan_files()
            
            data_files = []
            partition_map = {}
            
            for task in file_scan_tasks:
                data_file = task.file
                # Extract partition information
                # Extract partition information
                partition = {}
                if hasattr(data_file, 'partition') and data_file.partition:
                    try:
                        spec = table.specs()[data_file.spec_id]
                        # Map partition fields to values
                        for field in spec.fields:
                            # Access value by field name from the partition record
                            val = getattr(data_file.partition, field.name, None)
                            partition[field.name] = val
                    except Exception:
                        # Fallback if something goes wrong
                        partition = {}
                
                partition_key = json.dumps(partition, sort_keys=True)
                
                file_info = {
                    "filePath": data_file.file_path if hasattr(data_file, 'file_path') else str(data_file),
                    "fileFormat": "parquet",  # Iceberg typically uses Parquet
                    "partition": partition,
                    "recordCount": data_file.record_count if hasattr(data_file, 'record_count') else 0,
                    "fileSizeInBytes": data_file.file_size_in_bytes if hasattr(data_file, 'file_size_in_bytes') else 0,
                }
                data_files.append(file_info)
                
                # Aggregate partition stats
                if partition_key not in partition_map:
                    partition_map[partition_key] = {
                        "partition": partition,
                        "fileCount": 0,
                        "recordCount": 0,
                        "totalSize": 0,
                    }
                partition_map[partition_key]["fileCount"] += 1
                partition_map[partition_key]["recordCount"] += file_info["recordCount"]
                partition_map[partition_key]["totalSize"] += file_info["fileSizeInBytes"]
            
            partition_stats = list(partition_map.values())
            
            # Get snapshots
            snapshots = []
            current_snapshot = table.current_snapshot()
            if current_snapshot:
                snapshot_data = {
                    "snapshotId": str(current_snapshot.snapshot_id),
                    "timestamp": datetime.fromtimestamp(current_snapshot.timestamp_ms / 1000).isoformat() if current_snapshot.timestamp_ms else None,
                    "summary": current_snapshot.summary if hasattr(current_snapshot, 'summary') else {},
                    "manifestList": current_snapshot.manifest_list if hasattr(current_snapshot, 'manifest_list') else "",
                    "manifests": []
                }
                
                try:
                    # Get manifests
                    for manifest in current_snapshot.manifests(table.io):
                        manifest_data = {
                            "path": manifest.manifest_path,
                            "length": manifest.manifest_length,
                            "partitionSpecId": manifest.partition_spec_id,
                            "addedSnapshotId": manifest.added_snapshot_id,
                            "dataFiles": []
                        }
                        
                        # Get data files for this manifest (grouped by partition)
                        # Get data files for this manifest (grouped by partition)
                        try:
                            entries = manifest.fetch_manifest_entry(table.io)
                            
                            # Check if table is partitioned
                            is_partitioned = len(table.spec().fields) > 0
                            
                            if not is_partitioned:
                                # Unpartitioned table: add files directly to manifest
                                manifest_data["dataFiles"] = []
                                file_count = 0
                                
                                for entry in entries:
                                    # Skip deleted files
                                    if entry.status == 2: # DELETED
                                        continue
                                        
                                    data_file = entry.data_file
                                    file_count += 1
                                    
                                    # Limit to 10 files for unpartitioned tables to avoid clutter
                                    if len(manifest_data["dataFiles"]) < 10:
                                        manifest_data["dataFiles"].append({
                                            "path": data_file.file_path,
                                            "format": str(data_file.file_format),
                                            "recordCount": data_file.record_count,
                                            "fileSizeInBytes": data_file.file_size_in_bytes
                                        })
                                
                                # Add "more" indicator if needed (handled by frontend if dataFiles < file_count?)
                                # The frontend expects "dataFiles" list. If we want to show "more", we might need a way to indicate total count.
                                # Current frontend logic for "dataFiles" doesn't seem to show "more" node explicitly for direct dataFiles,
                                # but we can add a "more" node if we want, or just rely on the user seeing 10 files.
                                # Actually, let's check frontend: 
                                # if (m.dataFiles) { m.dataFiles.forEach... }
                                # It doesn't seem to have "more" logic for direct dataFiles in the code I saw earlier (lines 248-261 of IcebergTree.tsx).
                                # It just iterates all of them.
                                # So we should probably limit strictly or add a "more" node manually if we want.
                                # For now, let's just limit to 10.
                                
                            else:
                                # Partitioned table: group by partition
                                partition_groups = {}
                                partition_counts = {}
                                
                                for entry in entries:
                                    # Skip deleted files
                                    if entry.status == 2: # DELETED
                                        continue

                                    data_file = entry.data_file
                                    # Extract partition info
                                    partition_str = "Unpartitioned"
                                    if hasattr(data_file, 'partition') and data_file.partition:
                                        try:
                                            # Convert partition record to string representation
                                            # data_file.partition is a Record.
                                            partition_str = str(data_file.partition).replace("Record", "").strip("()")
                                            if not partition_str or partition_str == "()":
                                                partition_str = "Unpartitioned"
                                        except Exception:
                                            partition_str = "Unknown Partition"
                                    
                                    if partition_str not in partition_groups:
                                        partition_groups[partition_str] = []
                                        partition_counts[partition_str] = 0
                                    
                                    partition_counts[partition_str] += 1
                                    
                                    if len(partition_groups[partition_str]) < 5:
                                        partition_groups[partition_str].append({
                                            "path": data_file.file_path,
                                            "format": str(data_file.file_format),
                                            "recordCount": data_file.record_count,
                                            "fileSizeInBytes": data_file.file_size_in_bytes
                                        })
                                
                                # Convert groups to list (limit to 5 partitions)
                                manifest_data["partitions"] = []
                                sorted_partitions = sorted(partition_groups.keys())
                                manifest_data["totalPartitionCount"] = len(sorted_partitions)
                                
                                for i, p_name in enumerate(sorted_partitions):
                                    if i >= 5:
                                        break
                                    p_files = partition_groups[p_name]
                                    manifest_data["partitions"].append({
                                        "name": p_name,
                                        "fileCount": partition_counts[p_name],
                                        "dataFiles": p_files
                                    })
                                
                        except Exception as e:
                            print(f"Error reading manifest entries for {manifest.manifest_path}: {e}")
                            
                        snapshot_data["manifests"].append(manifest_data)
                except Exception as e:
                    print(f"Error reading manifests for snapshot {current_snapshot.snapshot_id}: {e}")
                
                snapshots.append(snapshot_data)
            
            # Get table properties
            properties = table.properties if hasattr(table, 'properties') else {}
            
            # Use metadata_log for metadataFiles if available
            final_metadata_files = metadata_files
            if hasattr(table.metadata, 'metadata_log') and table.metadata.metadata_log:
                log_files = []
                # Add historical files
                for entry in table.metadata.metadata_log:
                    log_files.append({
                        "file": entry.metadata_file,
                        "version": -1, # We might not know the version number easily from log, but we can try to parse
                        "timestamp": entry.timestamp_ms,
                        "currentSnapshotId": None,
                        "previousMetadataFile": None
                    })
                
                # Add current file
                current_file_info = {
                    "file": full_metadata_location,
                    "version": -1,
                    "timestamp": table.metadata.last_updated_ms,
                    "currentSnapshotId": str(table.metadata.current_snapshot_id) if table.metadata.current_snapshot_id else None,
                    "previousMetadataFile": None
                }
                
                # Check if current file is already in log (it usually isn't)
                if not any(f["file"] == current_file_info["file"] for f in log_files):
                    log_files.append(current_file_info)
                
                # Try to extract versions from filenames for better display
                for f in log_files:
                    try:
                        filename = f["file"].split("/")[-1]
                        if filename.startswith("v") and ".metadata.json" in filename:
                            version_part = filename.split(".")[0][1:]
                            f["version"] = int(version_part)
                        elif "-" in filename:
                             # Try {version}-{uuid}
                             parts = filename.split("-")
                             if parts[0].isdigit():
                                 f["version"] = int(parts[0])
                    except:
                        pass
                
                final_metadata_files = log_files

            return {
                "tableName": table_name,
                "location": actual_table_location,
                "formatVersion": table.format_version if hasattr(table, 'format_version') else metadata_dict.get("format-version", 1),
                "schema": schema_fields,
                "partitionSpec": partition_spec_fields,
                "sortOrder": sort_order_fields,
                "properties": properties,
                "currentSnapshotId": str(current_snapshot.snapshot_id) if current_snapshot else -1,
                "snapshots": snapshots,
                "dataFiles": data_files,
                "partitionStats": partition_stats,
                "metadataFiles": final_metadata_files,
            }
            
        except Exception as catalog_error:
            print(f"PyIceberg catalog/table loading failed: {catalog_error}")
            import traceback
            print(traceback.format_exc())
            # Fall through to manual parsing
            pass
        
        # Fallback: if PyIceberg table loading fails, still use manual parsing
        # but we already have metadata_dict
        
        # Extract schema using PyIceberg's Schema parser
        schema_fields = []
        current_schema_id = metadata_dict.get("current-schema-id", 0)
        
        if "schemas" in metadata_dict and isinstance(metadata_dict["schemas"], list):
            for schema_obj in metadata_dict["schemas"]:
                if schema_obj.get("schema-id") == current_schema_id:
                    if "fields" in schema_obj:
                        # Use PyIceberg Schema to parse properly
                        try:
                            from pyiceberg.schema import Schema
                            # Convert schema dict to PyIceberg Schema
                            schema_json_str = json.dumps(schema_obj)
                            # PyIceberg Schema.from_json() expects the full schema structure
                            # For now, parse fields manually but correctly
                            for field in schema_obj["fields"]:
                                field_type = field.get("type", "string")
                                # Handle nested types properly
                                if isinstance(field_type, dict):
                                    base_type = field_type.get("type", "string")
                                    if "element-id" in field_type:
                                        element_type = field_type.get("element-type", {})
                                        if isinstance(element_type, dict):
                                            element_base = element_type.get("type", "string")
                                        else:
                                            element_base = str(element_type)
                                        type_str = f"list<{element_base}>"
                                    elif "key-id" in field_type:
                                        key_type = field_type.get("key-type", {})
                                        value_type = field_type.get("value-type", {})
                                        key_str = key_type.get("type", "string") if isinstance(key_type, dict) else str(key_type)
                                        value_str = value_type.get("type", "string") if isinstance(value_type, dict) else str(value_type)
                                        type_str = f"map<{key_str},{value_str}>"
                                    else:
                                        type_str = base_type
                                else:
                                    type_str = str(field_type)
                                
                                schema_fields.append({
                                    "id": field.get("id", 0),
                                    "name": field.get("name", ""),
                                    "type": type_str,
                                    "required": field.get("required", False),
                                    "doc": field.get("doc"),
                                })
                        except Exception as schema_error:
                            print(f"PyIceberg schema parsing error: {schema_error}")
                            # Fallback to manual parsing
                            for field in schema_obj["fields"]:
                                field_type = field.get("type", "string")
                                if isinstance(field_type, dict):
                                    type_str = field_type.get("type", str(field_type))
                                else:
                                    type_str = str(field_type)
                                
                                schema_fields.append({
                                    "id": field.get("id", 0),
                                    "name": field.get("name", ""),
                                    "type": type_str,
                                    "required": field.get("required", False),
                                    "doc": field.get("doc"),
                                })
                    break
        
        # Remove excessive debug output - only log important info
        print(f"Analyzing table: {table_location}")
        print(f"Schema ID: {current_schema_id}, Snapshots: {len(metadata_dict.get('snapshots', []))}")
        
        # Schema already extracted above, continue with partition spec
        
        # Extract partition spec - Iceberg v2 uses "partition-specs" (plural) array
        partition_spec = []
        default_spec_id = metadata_dict.get("default-spec-id", 0)
        
        if "partition-specs" in metadata_dict and isinstance(metadata_dict["partition-specs"], list):
            # Find the spec with matching spec-id
            for spec_obj in metadata_dict["partition-specs"]:
                if spec_obj.get("spec-id") == default_spec_id:
                    if "fields" in spec_obj:
                        for field in spec_obj["fields"]:
                            partition_spec.append({
                                "fieldId": field.get("field-id", 0),
                                "sourceId": field.get("source-id", 0),
                                "name": field.get("name", ""),
                                "transform": field.get("transform", ""),
                            })
                    break
        # Fallback to "partition-spec" (singular)
        elif "partition-spec" in metadata_dict:
            spec = metadata_dict["partition-spec"]
            if isinstance(spec, dict) and "fields" in spec:
                for field in spec["fields"]:
                    partition_spec.append({
                        "fieldId": field.get("field-id", 0),
                        "sourceId": field.get("source-id", 0),
                        "name": field.get("name", ""),
                        "transform": field.get("transform", ""),
                    })
        
        # Extract sort order - Iceberg v2 uses "sort-orders" (plural) array
        sort_order = []
        default_sort_order_id = metadata_dict.get("default-sort-order-id", 0)
        
        if "sort-orders" in metadata_dict and isinstance(metadata_dict["sort-orders"], list):
            # Find the sort order with matching order-id
            for order_obj in metadata_dict["sort-orders"]:
                if order_obj.get("order-id") == default_sort_order_id:
                    if "fields" in order_obj:
                        for field in order_obj["fields"]:
                            sort_order.append({
                                "orderId": field.get("order-id", 0),
                                "direction": field.get("direction", "asc"),
                                "nullOrder": field.get("null-order", "nulls-first"),
                                "sortFieldId": field.get("field-id", 0),
                            })
                    break
        # Fallback to "sort-order" (singular)
        elif "sort-order" in metadata_dict:
            order = metadata_dict["sort-order"]
            if isinstance(order, dict) and "fields" in order:
                for field in order["fields"]:
                    sort_order.append({
                        "orderId": field.get("order-id", 0),
                        "direction": field.get("direction", "asc"),
                        "nullOrder": field.get("null-order", "nulls-first"),
                        "sortFieldId": field.get("field-id", 0),
                    })
        
        # Parse snapshots and get data files - process each snapshot separately for per-snapshot stats
        current_snapshot_id = metadata_dict.get("current-snapshot-id", -1)
        print(f"Processing table with current-snapshot-id: {current_snapshot_id}")
        snapshots = []
        all_data_files = []  # All data files across all snapshots
        snapshot_data_files = {}  # Data files per snapshot
        
        # Process each snapshot to get per-snapshot statistics
        if "snapshots" in metadata_dict and isinstance(metadata_dict["snapshots"], list):
            print(f"Found {len(metadata_dict['snapshots'])} snapshots")
            previous_snapshot_id = None
            
            for idx, snapshot in enumerate(metadata_dict["snapshots"]):
                # Use snapshot-id, not sequence-number
                snapshot_id = snapshot.get("snapshot-id", snapshot.get("sequence-number", 0))
                manifest_list = snapshot.get("manifest-list", "")
                parent_snapshot_id = snapshot.get("parent-snapshot-id")
                
                timestamp_ms = snapshot.get("timestamp-ms", 0)
                try:
                    if timestamp_ms and timestamp_ms > 0:
                        timestamp = datetime.fromtimestamp(timestamp_ms / 1000).isoformat()
                    else:
                        timestamp = datetime.now().isoformat()
                except (ValueError, OSError, OverflowError):
                    timestamp = datetime.now().isoformat()
                
                # Get data files for this snapshot
                snapshot_files = []
                if manifest_list:
                    try:
                        snapshot_files = get_manifest_files(bucket, normalized_path, manifest_list, project_id, token)
                        print(f"Snapshot {snapshot_id}: {len(snapshot_files)} data files")
                    except Exception as e:
                        print(f"Warning: Could not load manifest files for snapshot {snapshot_id}: {str(e)}")
                
                snapshot_data_files[snapshot_id] = snapshot_files
                all_data_files.extend(snapshot_files)
                
                # Calculate per-snapshot statistics
                snapshot_file_count = len(snapshot_files)
                snapshot_record_count = sum(f.get("recordCount", 0) for f in snapshot_files)
                snapshot_total_size = sum(f.get("fileSizeInBytes", 0) for f in snapshot_files)
                
                # Calculate delta from previous snapshot
                delta = {}
                if parent_snapshot_id and parent_snapshot_id in snapshot_data_files:
                    prev_files = snapshot_data_files[parent_snapshot_id]
                    prev_record_count = sum(f.get("recordCount", 0) for f in prev_files)
                    prev_total_size = sum(f.get("fileSizeInBytes", 0) for f in prev_files)
                    delta = {
                        "addedFiles": snapshot_file_count - len(prev_files),
                        "addedRecords": snapshot_record_count - prev_record_count,
                        "addedSize": snapshot_total_size - prev_total_size,
                    }
                else:
                    # First snapshot or no parent
                    delta = {
                        "addedFiles": snapshot_file_count,
                        "addedRecords": snapshot_record_count,
                        "addedSize": snapshot_total_size,
                    }
                
                summary = snapshot.get("summary", {})
                snapshots.append({
                    "snapshotId": str(snapshot_id),  # Convert to string to preserve precision
                    "sequenceNumber": snapshot.get("sequence-number", idx + 1),
                    "timestamp": timestamp,
                    "summary": summary,
                    "manifestList": manifest_list,
                    "parentSnapshotId": str(parent_snapshot_id) if parent_snapshot_id else None,
                    "statistics": {
                        "fileCount": snapshot_file_count,
                        "recordCount": snapshot_record_count,
                        "totalSize": snapshot_total_size,
                        "delta": delta,
                    },
                })
        
        print(f"Total data files across all snapshots: {len(all_data_files)}")
        
        # Calculate overall partition stats from all data files
        partition_stats = []
        partition_map = {}
        
        def serialize_partition(part):
            """Convert partition dict to JSON-serializable format"""
            if not part:
                return {}
            serialized = {}
            for key, value in part.items():
                if isinstance(value, datetime):
                    serialized[key] = value.isoformat()
                elif hasattr(value, 'isoformat'):  # datetime-like objects
                    serialized[key] = value.isoformat()
                else:
                    serialized[key] = value
            return serialized
        
        for file in all_data_files:
            partition = file.get("partition", {})
            # Serialize partition to handle datetime objects
            partition_serialized = serialize_partition(partition)
            # Create a consistent partition key
            partition_key = json.dumps(partition_serialized, sort_keys=True) if partition_serialized else "{}"
            if partition_key not in partition_map:
                partition_map[partition_key] = {
                    "partition": partition_serialized,
                    "fileCount": 0,
                    "recordCount": 0,
                    "totalSize": 0,
                }
            partition_map[partition_key]["fileCount"] += 1
            partition_map[partition_key]["recordCount"] += file.get("recordCount", 0)
            partition_map[partition_key]["totalSize"] += file.get("fileSizeInBytes", 0)
        
        partition_stats = list(partition_map.values())
        
        # Calculate overall statistics
        total_files = len(all_data_files)
        total_records = sum(f.get("recordCount", 0) for f in all_data_files)
        total_size = sum(f.get("fileSizeInBytes", 0) for f in all_data_files)
        
        # Return properly structured data
        table_name = path.split("/")[-1] if "/" in path else path
        return {
            "tableName": table_name,
            "location": f"gs://{bucket}/{path}",
            "formatVersion": metadata_dict.get("format-version", 1),
            "schema": schema_fields,
            "partitionSpec": partition_spec,
            "sortOrder": sort_order,
            "properties": metadata_dict.get("properties", {}),
            "currentSnapshotId": str(metadata_dict.get("current-snapshot-id", -1)),
            "snapshots": metadata_dict.get("snapshots", []),
            # Try to get data files from current snapshot for graph
            "dataFiles": _get_manual_data_files(bucket, normalized_path, metadata_dict, project_id, token),
            "partitionStats": [],
            "metadataFiles": [], # Could populate this if we had the list from read_iceberg_metadata_manual
            "statistics": {
                "totalFiles": total_files,
                "totalRecords": total_records,
                "totalSize": total_size,
                "totalPartitions": len(partition_stats),
            },
        }
    except Exception as e:
        print(f"PyIceberg analysis error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

def get_sample_data(
    bucket: str, 
    path: str, 
    limit: int = 100, 
    project_id: Optional[str] = None, 
    token: Optional[str] = None,
    snapshot_id: Optional[str] = None,
    manifest_path: Optional[str] = None,
    file_path: Optional[str] = None
) -> Dict[str, Any]:
    """Get sample data from the table using optimized metadata traversal"""
    empty_result = {
        "rows": [],
        "columns": [],
        "totalRows": 0,
        "filesRead": 0,
        "message": "No data found"
    }

    try:
        client = get_storage_client(project_id=project_id, token=token)
        bucket_obj = client.bucket(bucket)

        # Helper to read a specific parquet file
        def read_parquet_file(blob_path: str):
            import pandas as pd
            try:
                # Use gcsfs for efficient partial reads if available
                import gcsfs
                import pyarrow.parquet as pq
                import warnings
                
                # Suppress GCS project warning
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=UserWarning, module="gcsfs")
                    
                    # If token is not provided (using default creds), avoid passing project to prevent mismatch errors
                    fs_project = project_id if token else None
                    fs = gcsfs.GCSFileSystem(project=fs_project, token=token if token else 'google_default')
                    gcs_path = f"gs://{bucket}/{blob_path}"
                    
                    # Use ParquetFile for single file reading (safer than read_table)
                    pq_file = pq.ParquetFile(gcs_path, filesystem=fs)
                    
                    # Read only needed rows if possible (though we need to convert to pandas)
                    # reading the first row group might be enough if it has enough rows
                    # But for simplicity, let's read the file (or first few row groups)
                    
                    # If we just want head, we can read the first row group
                    if pq_file.num_row_groups > 0:
                        table = pq_file.read_row_group(0)
                        if table.num_rows < limit and pq_file.num_row_groups > 1:
                            # If first row group is small, read more or just read all
                            table = pq_file.read()
                    else:
                        table = pq_file.read()

                    # Convert to pandas with limit
                    df_head = table.slice(0, limit).to_pandas()
                
                for col in df_head.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_head[col]):
                        df_head[col] = df_head[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Add file name column
                # User wants "top folder/data/file"
                # If path is warehouse/db/table/data/file.parquet, we want table/data/file.parquet
                # We can try to find the table name in the path and take everything after it
                
                display_path = blob_path
                parts = blob_path.split("/")
                if len(parts) > 2:
                    # Try to find "data" and go one level up
                    try:
                        data_idx = parts.index("data")
                        if data_idx > 0:
                            display_path = "/".join(parts[data_idx-1:])
                    except ValueError:
                        # "data" not found, just use last 3 parts if available
                        if len(parts) >= 3:
                            display_path = "/".join(parts[-3:])
                
                # Add to dataframe
                df_head["_file_name"] = display_path
                
                rows = df_head.to_dict(orient='records')
                columns = list(df_head.columns)
                
                # Ensure _file_name is first
                if "_file_name" in columns:
                    columns.remove("_file_name")
                    columns.insert(0, "_file_name")
                
                return {
                    "rows": rows,
                    "columns": columns,
                    "totalRows": len(rows),
                    "filesRead": 1,
                    "message": None
                }
            except Exception as gcs_err:
                print(f"GCSFS sample failed, falling back to download: {gcs_err}")
                # Fallback to downloading full file
                blob = bucket_obj.blob(blob_path)
                from io import BytesIO
                content = blob.download_as_bytes()
                df = pd.read_parquet(BytesIO(content))
                df_head = df.head(limit)
                
                for col in df_head.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_head[col]):
                        df_head[col] = df_head[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                rows = df_head.to_dict(orient='records')
                columns = list(df_head.columns)
                
                return {
                    "rows": rows,
                    "columns": columns,
                    "totalRows": len(rows),
                    "filesRead": 1,
                    "message": None
                }

        # Case 1: Specific File Path provided
        if file_path:
            # Clean path
            file_path_clean = file_path.replace(f"gs://{bucket}/", "").lstrip("/")
            return read_parquet_file(file_path_clean)

        # Case 2: Specific Manifest Path provided
        if manifest_path:
            manifest_path_clean = manifest_path.replace(f"gs://{bucket}/", "").lstrip("/")
            blob = bucket_obj.blob(manifest_path_clean)
            content = blob.download_as_bytes()
            
            manifest_data = []
            if FASTAVRO_AVAILABLE:
                from io import BytesIO
                import fastavro
                manifest_bytes_io = BytesIO(content)
                manifest_data = list(fastavro.reader(manifest_bytes_io))
            
            if manifest_data:
                first_entry = manifest_data[0]
                data_file = first_entry.get("data_file") or first_entry
                f_path = data_file.get("file_path") or data_file.get("filePath")
                if f_path:
                    f_path_clean = f_path.replace(f"gs://{bucket}/", "").lstrip("/")
                    return read_parquet_file(f_path_clean)
            
            return empty_result

        # Optimized approach: Follow metadata -> snapshot -> manifest -> data file
        # This avoids listing the entire data directory which can be very slow
        
        # 1. Get metadata
        try:
            metadata_dict, _, _ = read_iceberg_metadata_manual(bucket, path, project_id, token)
            
            # 2. Get target snapshot
            target_snapshot = None
            if snapshot_id:
                snapshots = metadata_dict.get("snapshots", [])
                target_snapshot = next((s for s in snapshots if str(s.get("snapshot-id")) == str(snapshot_id)), None)
            else:
                current_snapshot_id = metadata_dict.get("current-snapshot-id")
                snapshots = metadata_dict.get("snapshots", [])
                target_snapshot = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
            
            if target_snapshot:
                manifest_list = target_snapshot.get("manifest-list")
                if manifest_list:
                    # Parse manifest list
                    manifest_list_path = manifest_list.replace(f"gs://{bucket}/", "").lstrip("/")
                    blob = bucket_obj.blob(manifest_list_path)
                    content = blob.download_as_bytes()
                    
                    manifest_list_data = []
                    if FASTAVRO_AVAILABLE:
                        from io import BytesIO
                        import fastavro
                        manifest_bytes_io = BytesIO(content)
                        manifest_list_data = list(fastavro.reader(manifest_bytes_io))
                    
                    # 3. Pick first manifest
                    if manifest_list_data:
                        first_manifest_entry = manifest_list_data[0]
                        m_path = first_manifest_entry.get("manifest_path")
                        
                        if m_path:
                            m_path_clean = m_path.replace(f"gs://{bucket}/", "").lstrip("/")
                            blob = bucket_obj.blob(m_path_clean)
                            content = blob.download_as_bytes()
                            
                            manifest_data = []
                            if FASTAVRO_AVAILABLE:
                                manifest_bytes_io = BytesIO(content)
                                manifest_data = list(fastavro.reader(manifest_bytes_io))
                            
                            # 4. Iterate through data files until limit is reached
                            all_rows = []
                            all_columns = []
                            files_read_count = 0
                            
                            # Get all data files from the manifest
                            data_files_list = []
                            if manifest_data:
                                for entry in manifest_data:
                                    d_file = entry.get("data_file") or entry
                                    f_p = d_file.get("file_path") or d_file.get("filePath")
                                    if f_p:
                                        data_files_list.append(f_p)
                            
                            # Iterate and read
                            files_attempted = 0
                            MAX_FILES_TO_ATTEMPT = 10
                            
                            for f_path in data_files_list:
                                if len(all_rows) >= limit:
                                    break
                                
                                if files_attempted >= MAX_FILES_TO_ATTEMPT:
                                    print(f"Reached maximum file attempt limit ({MAX_FILES_TO_ATTEMPT}) without satisfying row limit")
                                    break
                                    
                                files_attempted += 1
                                    
                                f_path_clean = f_path.replace(f"gs://{bucket}/", "").lstrip("/")
                                try:
                                    result = read_parquet_file(f_path_clean)
                                    if result and result.get("rows"):
                                        new_rows = result["rows"]
                                        # Calculate how many we need
                                        needed = limit - len(all_rows)
                                        all_rows.extend(new_rows[:needed])
                                        if not all_columns and result.get("columns"):
                                            all_columns = result["columns"]
                                        files_read_count += 1
                                except Exception as read_err:
                                    print(f"Error reading file {f_path_clean}: {read_err}")
                                    continue
                            
                            if all_rows:
                                return {
                                    "rows": all_rows,
                                    "columns": all_columns,
                                    "totalRows": len(all_rows),
                                    "filesRead": files_read_count,
                                    "message": None
                                }
        except Exception as e:
            print(f"Optimized sample fetch failed: {e}")
            # Fall through to fallback methods only if no specific target was requested
            if snapshot_id:
                return empty_result
        
        # Fallback 1: PyIceberg (if optimized failed AND no specific target requested)
        if PYICEBERG_AVAILABLE and not snapshot_id:
            try:
                normalized_path = path.strip("/")
                metadata_dict, metadata_location, _ = read_iceberg_metadata_manual(bucket, path, project_id, token)
                
                if not metadata_location.startswith("gs://"):
                    full_metadata_location = f"gs://{bucket}/{metadata_location}"
                else:
                    full_metadata_location = metadata_location
                
                table = StaticTable.from_metadata(full_metadata_location)
                scan = table.scan(limit=limit)
                arrow_table = scan.to_arrow()
                pydict = arrow_table.to_pydict()
                
                rows = []
                columns = []
                if pydict:
                    keys = list(pydict.keys())
                    columns = keys
                    num_rows = len(pydict[keys[0]])
                    for i in range(min(num_rows, limit)):
                        row = {}
                        for key in keys:
                            val = pydict[key][i]
                            if hasattr(val, 'isoformat'):
                                val = val.isoformat()
                            elif isinstance(val, bytes):
                                try:
                                    val = val.decode('utf-8')
                                except:
                                    val = str(val)
                            row[key] = val
                        rows.append(row)
                
                return {
                    "rows": rows,
                    "columns": columns,
                    "totalRows": len(rows),
                    "filesRead": 1,
                    "message": None
                }
            except Exception as e:
                print(f"PyIceberg sample failed: {e}")

        # Fallback 2: Manual listing (slowest) - only if no specific target
        if not snapshot_id:
            try:
                import pandas as pd
                client = get_storage_client(project_id=project_id, token=token)
                bucket_obj = client.bucket(bucket)
                normalized_path = path.strip("/")
                
                prefixes = [f"{normalized_path}/data/", f"{normalized_path}/"]
                parquet_file = None
                for prefix in prefixes:
                    blobs = bucket_obj.list_blobs(prefix=prefix, max_results=50)
                    for blob in blobs:
                        if blob.name.endswith(".parquet"):
                            parquet_file = blob
                            break
                    if parquet_file:
                        break
                
                if not parquet_file:
                    return empty_result
                
                return read_parquet_file(parquet_file.name)
                
            except ImportError:
                print("pandas/pyarrow not available for manual sample")
                return empty_result
            except Exception as e:
                print(f"Manual sample failed: {e}")
                return empty_result
            
    except Exception as e:
        print(f"Get sample data failed: {e}")
        return empty_result

def get_manual_data_files(bucket: str, path: str, metadata: Dict[str, Any], project_id: Optional[str] = None, token: Optional[str] = None) -> List[Dict[str, Any]]:
    """Helper to get data files from current snapshot using manual parsing"""
    try:
        current_snapshot_id = metadata.get("current-snapshot-id")
        if not current_snapshot_id:
            return []
            
        snapshots = metadata.get("snapshots", [])
        current_snapshot = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
        
        if not current_snapshot:
            return []
            
        manifest_list = current_snapshot.get("manifest-list")
        if not manifest_list:
            return []
            
        # Get data files with a limit to avoid performance issues
        # We'll fetch up to 100 files which should be enough for the graph
        files = get_manifest_files(bucket, path, manifest_list, project_id, token)
        return files[:100]
    except Exception as e:
        print(f"Error getting manual data files: {e}")
        return []


def compare_snapshots(bucket: str, path: str, snapshot_id_1: str, snapshot_id_2: str, project_id: Optional[str] = None, token: Optional[str] = None) -> Dict[str, Any]:
    """Compare two snapshots and return the differences"""
    try:
        # Get metadata
        metadata_dict, _, _ = read_iceberg_metadata_manual(bucket, path, project_id, token)
        
        snapshots = metadata_dict.get("snapshots", [])
        
        # Find the snapshots
        snap1 = None
        snap2 = None
        
        # Handle empty snapshot_id_1 (start of history)
        if not snapshot_id_1:
            snap1 = {
                "snapshot-id": 0,
                "timestamp-ms": 0,
                "summary": {},
                "manifest-list": ""
            }
        
        for snap in snapshots:
            sid = str(snap.get("snapshot-id"))
            if sid == snapshot_id_1:
                snap1 = snap
            if sid == snapshot_id_2:
                snap2 = snap
        
        if not snap2:
            raise ValueError(f"Snapshot {snapshot_id_2} not found")
        
        if not snap1 and snapshot_id_1:
             raise ValueError(f"Snapshot {snapshot_id_1} not found")

        # Get data files for both snapshots
        files1 = []
        if snapshot_id_1 and snap1.get("manifest-list"):
             files1 = get_manifest_files(bucket, path, snap1["manifest-list"], project_id, token)
        
        files2 = []
        if snap2.get("manifest-list"):
             files2 = get_manifest_files(bucket, path, snap2["manifest-list"], project_id, token)
             
        # Create maps for easy comparison
        files1_map = {f["filePath"]: f for f in files1}
        files2_map = {f["filePath"]: f for f in files2}
        
        added_files = []
        removed_files = []
        modified_files = []
        
        # Find added and modified files
        for path2, file2 in files2_map.items():
            if path2 not in files1_map:
                added_files.append(file2)
            else:
                # Check if modified (e.g. different size or record count, though usually files are immutable)
                file1 = files1_map[path2]
                if file1["fileSizeInBytes"] != file2["fileSizeInBytes"] or file1["recordCount"] != file2["recordCount"]:
                    modified_files.append({
                        "filePath": path2,
                        "before": file1,
                        "after": file2,
                        "changes": {
                            "sizeDelta": file2["fileSizeInBytes"] - file1["fileSizeInBytes"],
                            "recordDelta": file2["recordCount"] - file1["recordCount"]
                        }
                    })
        
        # Find removed files
        for path1, file1 in files1_map.items():
            if path1 not in files2_map:
                removed_files.append(file1)
                
        # Calculate statistics
        stats1 = {
            "fileCount": len(files1),
            "recordCount": sum(f["recordCount"] for f in files1),
            "totalSize": sum(f["fileSizeInBytes"] for f in files1)
        }
        
        stats2 = {
            "fileCount": len(files2),
            "recordCount": sum(f["recordCount"] for f in files2),
            "totalSize": sum(f["fileSizeInBytes"] for f in files2)
        }
        
        delta = {
            "files": stats2["fileCount"] - stats1["fileCount"],
            "records": stats2["recordCount"] - stats1["recordCount"],
            "size": stats2["totalSize"] - stats1["totalSize"]
        }
        
        # Format snapshots for response
        def format_snapshot(s):
            if not s: return None
            ts = s.get("timestamp-ms", 0)
            return {
                "snapshotId": str(s.get("snapshot-id", "")),
                "timestamp": datetime.fromtimestamp(ts / 1000).isoformat() if ts > 0 else datetime.min.isoformat(),
                "summary": s.get("summary", {}),
                "manifestList": s.get("manifest-list", "")
            }

        return {
            "snapshot1": format_snapshot(snap1),
            "snapshot2": format_snapshot(snap2),
            "addedFiles": added_files,
            "removedFiles": removed_files,
            "modifiedFiles": modified_files,
            "statistics": {
                "snapshot1": stats1,
                "snapshot2": stats2,
                "delta": delta
            },
            "summary": {
                "addedCount": len(added_files),
                "removedCount": len(removed_files),
                "modifiedCount": len(modified_files)
            }
        }

    except Exception as e:
        print(f"Compare snapshots failed: {e}")
        import traceback
        print(traceback.format_exc())
        raise e
