"""
FastAPI backend for Iceberg Explorer
Handles GCS bucket access andpip install -r backend/requirements.txt Iceberg table analysis
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional
import os
from google.cloud import storage
from google.cloud import resourcemanager_v3
import json
from datetime import datetime

# Try to import PyIceberg for proper metadata parsing
try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.table import Table
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

app = FastAPI(title="Iceberg Explorer API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_storage_client(project_id: Optional[str] = None):
    """Get GCS storage client with credentials"""
    # Try to use credentials from environment or default
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and os.path.exists(credentials_path):
        if project_id:
            return storage.Client.from_service_account_json(credentials_path, project=project_id)
        return storage.Client.from_service_account_json(credentials_path)
    if project_id:
        return storage.Client(project=project_id)
    return storage.Client()


def get_resource_manager_client():
    """Get Resource Manager client for listing projects"""
    try:
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path and os.path.exists(credentials_path):
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            return resourcemanager_v3.ProjectsClient(credentials=credentials)
        return resourcemanager_v3.ProjectsClient()
    except Exception:
        # If Resource Manager API is not available, return None
        return None


@app.get("/")
async def root():
    return {"message": "Iceberg Explorer API", "version": "1.0.0"}


@app.get("/projects")
async def list_projects():
    """List all GCP projects accessible with current credentials"""
    projects = []
    errors = []
    
    # Try Resource Manager API first
    try:
        client = get_resource_manager_client()
        if client is not None:
            # Use search_projects instead of list_projects to avoid parent requirement
            # search_projects can list all accessible projects without requiring a parent
            try:
                # Try search_projects first (doesn't require parent)
                request = resourcemanager_v3.SearchProjectsRequest(query="")
                page_result = client.search_projects(request=request)
            except Exception as search_error:
                # If search_projects fails, try list_projects with organizations/-
                try:
                    request = resourcemanager_v3.ListProjectsRequest(parent="organizations/-")
                    page_result = client.list_projects(request=request)
                except Exception:
                    # If both fail, raise the original search error
                    raise search_error
            
            for project in page_result:
                try:
                    project_state = "UNKNOWN"
                    if hasattr(project, 'state'):
                        if hasattr(project.state, 'name'):
                            project_state = project.state.name
                        elif isinstance(project.state, int):
                            # State is an enum value
                            from google.cloud.resourcemanager_v3 import Project
                            try:
                                project_state = Project.State(project.state).name
                            except:
                                project_state = str(project.state)
                    
                    projects.append({
                        "id": project.project_id,
                        "name": project.display_name or project.project_id,
                        "state": project_state,
                    })
                except Exception as e:
                    errors.append(f"Error processing project: {str(e)}")
                    continue
            
            # Return active projects first, but include all if requested
            if projects:
                # Filter to active projects for display
                active_projects = [p for p in projects if p.get("state") == "ACTIVE"]
                if active_projects:
                    return {
                        "projects": active_projects,
                        "total_found": len(projects),
                        "active_count": len(active_projects),
                        "errors": errors if errors else None
                    }
                # If no active projects, return all projects
                return {
                    "projects": projects,
                    "total_found": len(projects),
                    "active_count": 0,
                    "errors": errors if errors else None
                }
    except Exception as e:
        error_msg = f"Resource Manager API error: {str(e)}"
        errors.append(error_msg)
        print(error_msg)
        import traceback
        print(traceback.format_exc())
    
    # Try using gcloud CLI as fallback
    try:
        import subprocess
        result = subprocess.run(
            ["gcloud", "projects", "list", "--format=json"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 and result.stdout:
            import json as json_lib
            gcloud_projects = json_lib.loads(result.stdout)
            for proj in gcloud_projects:
                project_id = proj.get("projectId") or proj.get("project_id")
                if project_id and not any(p["id"] == project_id for p in projects):
                    projects.append({
                        "id": project_id,
                        "name": proj.get("name", project_id),
                        "state": "ACTIVE" if proj.get("lifecycleState") == "ACTIVE" else "UNKNOWN",
                    })
    except Exception as e:
        # gcloud not available or failed, continue
        pass
    
    # Fallback: Try to get project from service account JSON file
    try:
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path and os.path.exists(credentials_path):
            import json as json_lib
            with open(credentials_path, 'r') as f:
                creds_data = json_lib.load(f)
                project_id = creds_data.get("project_id")
                if project_id and not any(p["id"] == project_id for p in projects):
                    projects.append({
                        "id": project_id,
                        "name": project_id,
                        "state": "UNKNOWN",
                    })
    except Exception:
        pass
    
    # Fallback: Try to get project from default storage client
    try:
        storage_client = get_storage_client()
        default_project = storage_client.project
        if default_project and not any(p["id"] == default_project for p in projects):
            projects.append({
                "id": default_project,
                "name": default_project,
                "state": "UNKNOWN",
            })
    except Exception:
        pass
    
    if projects:
        active_projects = [p for p in projects if p.get("state") == "ACTIVE"]
        return {
            "projects": active_projects if active_projects else projects,
            "total_found": len(projects),
            "active_count": len(active_projects),
            "errors": errors if errors else None
        }
    
    # If still no projects, return empty list with helpful message
    error_detail = "No projects found. "
    if errors:
        error_detail += f"Errors: {'; '.join(errors)}. "
    error_detail += "Make sure:\n"
    error_detail += "1. Resource Manager API is enabled\n"
    error_detail += "2. Service account has 'resourcemanager.projects.list' permission\n"
    error_detail += "3. Or set project_id in your service account JSON file"
    
    raise HTTPException(status_code=500, detail=error_detail)


@app.get("/buckets")
async def list_buckets(project_id: Optional[str] = None):
    """List all GCS buckets accessible with current credentials, optionally filtered by project"""
    try:
        client = get_storage_client(project_id=project_id)
        buckets = []
        for bucket in client.list_buckets():
            # If project_id is specified, only return buckets from that project
            if project_id:
                if bucket.project_number:
                    # Get project ID from bucket metadata if available
                    try:
                        bucket.reload()
                        if bucket.project_number:
                            # We can't easily get project_id from bucket, so we'll list all
                            # and filter by checking if bucket is accessible in the specified project
                            buckets.append(bucket.name)
                    except Exception:
                        # If we can't reload, just include it
                        buckets.append(bucket.name)
                else:
                    buckets.append(bucket.name)
            else:
                buckets.append(bucket.name)
        
        return {"buckets": buckets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list buckets: {str(e)}")


@app.get("/discover")
async def discover_iceberg_tables(bucket: str, project_id: Optional[str] = None):
    """Recursively scan a bucket for all Iceberg tables by finding *.metadata.json files"""
    try:
        client = get_storage_client(project_id=project_id)
        bucket_obj = client.bucket(bucket)
        
        tables = []
        seen_table_paths = set()
        
        # Recursively search for all metadata.json files
        # Look for files ending with .metadata.json
        blobs = bucket_obj.list_blobs()
        
        for blob in blobs:
            blob_name = blob.name
            
            # Look for Iceberg metadata files
            if blob_name.endswith(".metadata.json") and "metadata" in blob_name:
                # Extract table path (everything before /metadata/)
                parts = blob_name.split("/metadata/")
                if len(parts) > 0:
                    table_path = parts[0]
                    
                    if table_path not in seen_table_paths:
                        seen_table_paths.add(table_path)
                        table_name = table_path.split("/")[-1] if "/" in table_path else table_path
                        
                        table_info = {
                            "name": table_name,
                            "location": f"gs://{bucket}/{table_path}",
                            "bucket": bucket,
                            "path": table_path,
                        }
                        if project_id:
                            table_info["projectId"] = project_id
                        
                        tables.append(table_info)
        
        # Sort by path
        tables.sort(key=lambda x: x["path"])
        
        return {
            "tables": tables,
            "count": len(tables),
        }
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Failed to discover Iceberg tables: {str(e)}\n{traceback.format_exc()}"
        )


@app.get("/browse")
async def browse_bucket(bucket: str, path: str = "", project_id: Optional[str] = None):
    """Browse a GCS bucket and find Iceberg tables"""
    try:
        client = get_storage_client(project_id=project_id)
        bucket_obj = client.bucket(bucket)
        
        folders = set()
        items = []  # All items: folders that might be Iceberg tables, regular folders, etc.
        iceberg_tables = {}  # Map of folder path to table info
        
        # List objects with the given prefix
        prefix = path + "/" if path else ""
        
        # Use list_blobs with delimiter to get folder structure
        blobs_iterator = bucket_obj.list_blobs(prefix=prefix, delimiter="/")
        
        # Consume the iterator - this populates the prefixes attribute
        blobs_list = list(blobs_iterator)
        
        # First, scan all blobs to find Iceberg tables at any level
        seen_table_paths = set()
        for blob in blobs_list:
            blob_name = blob.name
            
            # Look for Iceberg metadata files
            if "metadata" in blob_name and blob_name.endswith(".metadata.json"):
                # Extract table path (everything before /metadata/)
                parts = blob_name.split("/metadata/")
                if len(parts) > 0:
                    table_path = parts[0]
                    # Remove prefix if present to get relative path
                    if prefix and table_path.startswith(prefix):
                        relative_table_path = table_path[len(prefix):].lstrip("/")
                    else:
                        relative_table_path = table_path
                    
                    if table_path not in seen_table_paths:
                        seen_table_paths.add(table_path)
                        table_name = relative_table_path.split("/")[-1] if "/" in relative_table_path else relative_table_path
                        # Store both full path and relative path for matching
                        table_info = {
                            "name": table_name,
                            "location": f"gs://{bucket}/{table_path}",
                            "bucket": bucket,
                            "path": table_path,
                        }
                        if project_id:
                            table_info["projectId"] = project_id
                        iceberg_tables[table_path] = table_info
                        # Also store by relative path for easier matching
                        if relative_table_path != table_path:
                            iceberg_tables[relative_table_path] = iceberg_tables[table_path]
        
        # Get folders from prefixes (these are "folders" in GCS)
        try:
            if hasattr(blobs_iterator, 'prefixes'):
                for prefix_item in blobs_iterator.prefixes:
                    # Remove the base prefix to get just the folder name
                    if prefix and prefix_item.startswith(prefix):
                        folder_name = prefix_item[len(prefix):].rstrip("/")
                    else:
                        folder_name = prefix_item.rstrip("/")
                    
                    # Only get immediate children (first level)
                    if folder_name:
                        parts = folder_name.split("/")
                        if parts and parts[0]:
                            immediate_folder = parts[0]
                            folders.add(immediate_folder)
                            
                            # Check if this folder is an Iceberg table
                            full_folder_path = f"{path}/{immediate_folder}" if path else immediate_folder
                            # Check if this path matches any Iceberg table
                            matching_table = None
                            for table_path, table_info in iceberg_tables.items():
                                # Check if folder path matches table path exactly, or table is in this folder
                                if table_path == full_folder_path or table_path.startswith(full_folder_path + "/"):
                                    # If exact match, this folder IS the table
                                    if table_path == full_folder_path:
                                        matching_table = table_info
                                        break
                            
                            if matching_table:
                                items.append({
                                    "name": immediate_folder,
                                    "type": "iceberg_table",
                                    "path": full_folder_path,
                                    "table": matching_table,
                                })
                            else:
                                items.append({
                                    "name": immediate_folder,
                                    "type": "folder",
                                    "path": full_folder_path,
                                })
        except AttributeError:
            pass
        
        # Also infer folders from blob paths
        for blob in blobs_list:
            blob_name = blob.name
            
            # Remove prefix to get relative path
            if prefix and blob_name.startswith(prefix):
                relative_path = blob_name[len(prefix):]
            else:
                relative_path = blob_name
            
            # Extract immediate folder if this blob is in a subfolder
            if "/" in relative_path:
                immediate_folder = relative_path.split("/")[0]
                if immediate_folder and immediate_folder not in [item["name"] for item in items]:
                    folders.add(immediate_folder)
                    full_folder_path = f"{path}/{immediate_folder}" if path else immediate_folder
                    
                    # Check if this folder is an Iceberg table
                    matching_table = None
                    for table_path, table_info in iceberg_tables.items():
                        # Check if folder path matches table path exactly
                        if table_path == full_folder_path or table_path.startswith(full_folder_path + "/"):
                            if table_path == full_folder_path:
                                matching_table = table_info
                                break
                    
                    if matching_table:
                        items.append({
                            "name": immediate_folder,
                            "type": "iceberg_table",
                            "path": full_folder_path,
                            "table": matching_table,
                        })
                    else:
                        items.append({
                            "name": immediate_folder,
                            "type": "folder",
                            "path": full_folder_path,
                        })
        
        # Sort items: Iceberg tables first, then folders
        items.sort(key=lambda x: (x["type"] != "iceberg_table", x["name"].lower()))
        
        # Extract just table info for backward compatibility
        tables = [item["table"] for item in items if item["type"] == "iceberg_table"]
        
        return {
            "folders": sorted(list(folders)),
            "tables": tables,
            "items": items,  # New: all items with type information
        }
    except Exception as e:
        import traceback
        error_detail = f"Failed to browse bucket: {str(e)}"
        error_detail += f"\nTraceback: {traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


def analyze_with_pyiceberg_metadata(bucket: str, path: str, project_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Use PyIceberg's Table API to properly load and analyze Iceberg table"""
    if not PYICEBERG_AVAILABLE:
        return None
    
    try:
        normalized_path = path.strip("/")
        table_location = f"gs://{bucket}/{normalized_path}"
        
        # First, find the metadata file to get the exact table location
        metadata_dict = read_iceberg_metadata_manual(bucket, path, project_id)
        actual_table_location = metadata_dict.get("location", table_location)
        
        # Use PyIceberg's filesystem catalog to load the table
        # PyIceberg can work with GCS through fsspec/gcsfs
        try:
            # Create a filesystem catalog pointing to the table's warehouse location
            # The warehouse is the parent directory of the table
            warehouse_path = "/".join(actual_table_location.split("/")[:-1]) if "/" in actual_table_location else actual_table_location
            
            # Try different catalog type names
            # PyIceberg 0.6.0 uses "filesystem" as the catalog type
            try:
                catalog = load_catalog(
                    "filesystem",
                    **{
                        "type": "filesystem",
                        "warehouse": warehouse_path,
                    }
                )
            except Exception as e1:
                # Try alternative catalog type
                try:
                    catalog = load_catalog(
                        "gcs",
                        **{
                            "type": "filesystem",
                            "warehouse": warehouse_path,
                        }
                    )
                except Exception as e2:
                    raise Exception(f"Failed to load catalog with both 'filesystem' and 'gcs' types: {e1}, {e2}")
            
            # Extract namespace and table name from path
            # For filesystem catalog, we need to construct the identifier
            # The table name is the last part of the path
            table_name = normalized_path.split("/")[-1] if "/" in normalized_path else normalized_path
            namespace = ".".join(normalized_path.split("/")[:-1]) if "/" in normalized_path else "default"
            
            # Try to load the table
            try:
                table_identifier = f"{namespace}.{table_name}" if namespace != "default" else table_name
                table = catalog.load_table(table_identifier)
            except Exception:
                # If that doesn't work, try just the table name
                table = catalog.load_table(table_name)
            
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
                for data_file in task:
                    # Extract partition information
                    partition = {}
                    if hasattr(data_file, 'partition') and data_file.partition:
                        partition = dict(data_file.partition)
                    
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
                snapshots.append({
                    "snapshotId": str(current_snapshot.snapshot_id),  # Convert to string to preserve precision
                    "timestamp": datetime.fromtimestamp(current_snapshot.timestamp_ms / 1000).isoformat() if current_snapshot.timestamp_ms else None,
                    "summary": current_snapshot.summary if hasattr(current_snapshot, 'summary') else {},
                    "manifestList": current_snapshot.manifest_list if hasattr(current_snapshot, 'manifest_list') else "",
                })
            
            # Get table properties
            properties = table.properties() if hasattr(table, 'properties') else {}
            
            return {
                "tableName": table_name,
                "location": actual_table_location,
                "formatVersion": table.format_version() if hasattr(table, 'format_version') else metadata_dict.get("format-version", 1),
                "schema": schema_fields,
                "partitionSpec": partition_spec_fields,
                "sortOrder": sort_order_fields,
                "properties": properties,
                "currentSnapshotId": str(current_snapshot.snapshot_id) if current_snapshot else -1,
                "snapshots": snapshots,
                "dataFiles": data_files,
                "partitionStats": partition_stats,
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
                        snapshot_files = get_manifest_files(bucket, normalized_path, manifest_list, project_id)
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
            "currentSnapshotId": str(current_snapshot_id) if current_snapshot_id != -1 else -1,
            "snapshots": snapshots,
            "dataFiles": all_data_files,
            "partitionStats": partition_stats,
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


def read_iceberg_metadata_manual(bucket: str, path: str, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Manually read Iceberg metadata from GCS
    
    According to Apache Iceberg spec:
    - Metadata files are in metadata/ directory
    - Named as v{version}.metadata.json (e.g., v1.metadata.json, v2.metadata.json)
    - Or sometimes as {version}-{hash}.metadata.json
    - The latest version file contains the current table state
    """
    try:
        client = get_storage_client(project_id=project_id)
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
            
            raise Exception(error_msg)
        
        # Get the latest metadata file (highest version number)
        # Iceberg metadata files are versioned: 00000-*.metadata.json, 00001-*.metadata.json, etc.
        def extract_version(blob_name: str) -> int:
            try:
                filename = blob_name.split("/")[-1]  # Get just the filename
                
                # Pattern 1: {zero-padded-number}-{hash}.metadata.json (e.g., 00002-abc123.metadata.json)
                # This is the most common format in Iceberg v2
                if "-" in filename and ".metadata.json" in filename:
                    version_str = filename.split("-")[0]
                    # Handle zero-padded numbers like "00002" -> 2
                    if version_str.isdigit():
                        return int(version_str)
                
                # Pattern 2: v{number}.metadata.json (older format)
                if filename.startswith("v") and ".metadata.json" in filename:
                    version_part = filename[1:].split(".metadata.json")[0]
                    version_str = version_part.split("-")[0]
                    if version_str.isdigit():
                        return int(version_str)
                
                # Pattern 3: Just a number at the start
                if filename[0].isdigit():
                    version_str = filename.split(".")[0].split("-")[0]
                    if version_str.isdigit():
                        return int(version_str)
                
                # If no version pattern found, return -1 (will be sorted last)
                return -1
            except (ValueError, IndexError, AttributeError):
                return -1
        
        if not metadata_json_files:
            raise Exception(f"No metadata files found. Expected files like '00000-*.metadata.json' in '{metadata_dir}' directory")
        
        # Sort by version (highest first) and get the latest
        # Also sort by name as secondary key to ensure consistent ordering
        metadata_json_files.sort(key=lambda x: (extract_version(x.name), x.name), reverse=True)
        
        # Verify we have files and log all versions found
        if metadata_json_files:
            print(f"Found {len(metadata_json_files)} metadata files:")
            for f in metadata_json_files[:5]:  # Show first 5
                print(f"  - {f.name} (version: {extract_version(f.name)})")
        
        # Strategy: Use the HIGHEST VERSION file (already sorted), but verify it has data
        # If the highest version is empty, try the next highest version with data
        latest_metadata = None
        best_metadata = None
        best_version = -1
        
        # Start with the highest version file (first in sorted list)
        for metadata_file in metadata_json_files:
            try:
                content = metadata_file.download_as_text()
                test_metadata = json.loads(content)
                test_snapshot_id = test_metadata.get("current-snapshot-id", -1)
                test_version = extract_version(metadata_file.name)
                
                # Use the highest version file that has actual snapshots
                # Priority: version number first, then check if it has data
                if test_version > best_version:
                    if test_snapshot_id != -1:  # Only use files with actual data
                        best_metadata = test_metadata
                        best_version = test_version
                        latest_metadata = metadata_file
                        print(f"  Candidate: {metadata_file.name} (version={test_version}, snapshot-id={test_snapshot_id})")
            except Exception as e:
                print(f"  Error reading {metadata_file.name}: {str(e)}")
                continue
        
        # If we found a file with data, use it
        if latest_metadata and best_metadata:
            current_snapshot_id = best_metadata.get("current-snapshot-id", -1)
            print(f"Selected metadata file: {latest_metadata.name} (version: {best_version}, current-snapshot-id: {current_snapshot_id})")
            return best_metadata
        
        # Fallback: use highest version file even if empty
        if metadata_json_files:
            latest_metadata = metadata_json_files[0]
            latest_version = extract_version(latest_metadata.name)
            print(f"Using highest version file (may be empty): {latest_metadata.name} (version: {latest_version})")
            
            try:
                metadata_content = latest_metadata.download_as_text()
                metadata = json.loads(metadata_content)
                current_snapshot_id = metadata.get("current-snapshot-id", -1)
                snapshots_count = len(metadata.get("snapshots", []))
                print(f"Metadata file check: current-snapshot-id={current_snapshot_id}, snapshots={snapshots_count}")
                return metadata
            except Exception as e:
                raise Exception(f"Failed to parse metadata file {latest_metadata.name}: {str(e)}")
        
        raise Exception("No valid metadata files found")
        
    except Exception as e:
        import traceback
        error_detail = f"Failed to read metadata: {str(e)}"
        error_detail += f"\nPath: {path}"
        error_detail += f"\nBucket: {bucket}"
        error_detail += f"\nTraceback: {traceback.format_exc()}"
        raise Exception(error_detail)


def get_manifest_files(bucket: str, path: str, manifest_list_path: str, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get data files from manifest list using PyIceberg or fastavro for Avro parsing"""
    try:
        client = get_storage_client(project_id=project_id)
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


def analyze_table_with_pyiceberg(bucket: str, path: str, project_id: Optional[str] = None) -> Dict[str, Any]:
    """Analyze Iceberg table using PyIceberg library for accurate metadata"""
    try:
        normalized_path = path.strip("/")
        table_location = f"gs://{bucket}/{normalized_path}"
        
        # Create a REST catalog pointing to GCS
        # For GCS, we can use a filesystem-based approach
        catalog = load_catalog(
            "gcs_catalog",
            **{
                "type": "rest",
                "uri": table_location,
                "warehouse": f"gs://{bucket}/{normalized_path}",
            }
        )
        
        # Try to load the table
        # Note: This might not work directly with GCS paths, so we'll fall back to manual parsing
        # But we can use PyIceberg to parse the metadata file properly
        return None
    except Exception as e:
        print(f"PyIceberg catalog approach failed: {str(e)}")
        return None


@app.get("/analyze")
async def analyze_table(bucket: str, path: str, project_id: Optional[str] = None):
    """Analyze an Iceberg table and return comprehensive metadata"""
    try:
        # Normalize path
        normalized_path = path.strip("/")
        
        # Try PyIceberg first if available
        if PYICEBERG_AVAILABLE:
            try:
                # Use PyIceberg to parse metadata properly
                result = analyze_with_pyiceberg_metadata(bucket, normalized_path, project_id)
                if result:
                    return result
            except Exception as e:
                print(f"PyIceberg analysis failed, falling back to manual: {str(e)}")
        
        # Fall back to manual metadata reading
        try:
            metadata = read_iceberg_metadata_manual(bucket, normalized_path, project_id=project_id)
        except Exception as e:
            # Provide more detailed error information
            error_msg = str(e)
            import traceback
            raise HTTPException(
                status_code=404,
                detail=f"Failed to read Iceberg metadata:\n{error_msg}\n\n"
                       f"Bucket: {bucket}\n"
                       f"Path: {normalized_path}\n"
                       f"Project: {project_id or 'default'}\n\n"
                       f"Please verify:\n"
                       f"1. The path is correct\n"
                       f"2. The table has a metadata/ directory\n"
                       f"3. There are .metadata.json files in the metadata directory"
            )
        
        # Extract schema - Iceberg v2 uses "schemas" (plural) array
        schema_fields = []
        current_schema_id = metadata.get("current-schema-id", 0)
        
        # Try "schemas" (plural) first (Iceberg v2)
        if "schemas" in metadata and isinstance(metadata["schemas"], list):
            # Find the schema with matching schema-id
            for schema_obj in metadata["schemas"]:
                if schema_obj.get("schema-id") == current_schema_id:
                    if "fields" in schema_obj:
                        for field in schema_obj["fields"]:
                            # Handle type - can be string or dict
                            field_type = field.get("type", "string")
                            if isinstance(field_type, dict):
                                type_str = field_type.get("type", str(field_type))
                                if "element-id" in field_type:
                                    type_str = f"list<{type_str}>"
                                elif "key-id" in field_type:
                                    type_str = f"map<{type_str}>"
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
        # Fallback to "schema" (singular) for older format
        elif "schema" in metadata:
            schema = metadata["schema"]
            fields_list = None
            
            if isinstance(schema, dict) and "fields" in schema:
                fields_list = schema["fields"]
            elif isinstance(schema, list):
                fields_list = schema
            
            if fields_list:
                for field in fields_list:
                    field_type = field.get("type", {})
                    if isinstance(field_type, dict):
                        type_str = field_type.get("type", str(field_type))
                        if "element-id" in field_type:
                            type_str = f"list<{type_str}>"
                        elif "key-id" in field_type:
                            type_str = f"map<{type_str}>"
                    else:
                        type_str = str(field_type)
                    
                    schema_fields.append({
                        "id": field.get("id", 0),
                        "name": field.get("name", ""),
                        "type": type_str,
                        "required": not field.get("optional", True),
                        "doc": field.get("doc"),
                    })
        
        # Extract partition spec - Iceberg v2 uses "partition-specs" (plural) array
        partition_spec = []
        default_spec_id = metadata.get("default-spec-id", 0)
        
        if "partition-specs" in metadata and isinstance(metadata["partition-specs"], list):
            # Find the spec with matching spec-id
            for spec_obj in metadata["partition-specs"]:
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
        elif "partition-spec" in metadata:
            spec = metadata["partition-spec"]
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
        default_sort_order_id = metadata.get("default-sort-order-id", 0)
        
        if "sort-orders" in metadata and isinstance(metadata["sort-orders"], list):
            # Find the sort order with matching order-id
            for order_obj in metadata["sort-orders"]:
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
        elif "sort-order" in metadata:
            order = metadata["sort-order"]
            if isinstance(order, dict) and "fields" in order:
                for field in order["fields"]:
                    sort_order.append({
                        "orderId": field.get("order-id", 0),
                        "direction": field.get("direction", "asc"),
                        "nullOrder": field.get("null-order", "nulls-first"),
                        "sortFieldId": field.get("field-id", 0),
                    })
        
        # Get current snapshot and process all snapshots with per-snapshot stats
        current_snapshot_id = metadata.get("current-snapshot-id", -1)
        print(f"Processing table with current-snapshot-id: {current_snapshot_id}")
        snapshots = []
        all_data_files = []
        snapshot_data_files = {}
        
        if "snapshots" in metadata and isinstance(metadata["snapshots"], list):
            print(f"Found {len(metadata['snapshots'])} snapshots")
            for idx, snapshot in enumerate(metadata["snapshots"]):
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
                        snapshot_files = get_manifest_files(bucket, normalized_path, manifest_list, project_id=project_id)
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
        for file in all_data_files:
            partition = file.get("partition", {})
            partition_key = json.dumps(partition, sort_keys=True) if partition else "{}"
            if partition_key not in partition_map:
                partition_map[partition_key] = {
                    "partition": partition,
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
        
        # Extract table name from path
        table_name = normalized_path.split("/")[-1] if "/" in normalized_path else normalized_path
        
        return {
            "tableName": table_name,
            "location": f"gs://{bucket}/{normalized_path}",
            "formatVersion": metadata.get("format-version", 1),
            "schema": schema_fields,
            "partitionSpec": partition_spec,
            "sortOrder": sort_order,
            "properties": metadata.get("properties", {}),
            "currentSnapshotId": str(current_snapshot_id) if current_snapshot_id != -1 else -1,
            "snapshots": snapshots,
            "dataFiles": all_data_files,
            "partitionStats": partition_stats,
            "statistics": {
                "totalFiles": total_files,
                "totalRecords": total_records,
                "totalSize": total_size,
                "totalPartitions": len(partition_stats),
            },
        }
    except HTTPException:
        # Re-raise HTTP exceptions (like the 404 from metadata reading)
        raise
    except Exception as e:
        import traceback
        error_detail = f"Failed to analyze table: {str(e)}"
        error_detail += f"\n\nBucket: {bucket}"
        error_detail += f"\nPath: {path}"
        error_detail += f"\nNormalized Path: {normalized_path if 'normalized_path' in locals() else 'N/A'}"
        error_detail += f"\nProject: {project_id or 'default'}"
        error_detail += f"\n\nTraceback:\n{traceback.format_exc()}"
        raise HTTPException(
            status_code=500,
            detail=error_detail
        )


@app.get("/analyze/snapshot")
async def get_snapshot_data(bucket: str, path: str, snapshot_id: int, project_id: Optional[str] = None):
    """Get data files and statistics for a specific snapshot"""
    try:
        normalized_path = path.strip("/")
        
        # Get the full table analysis first
        table_data = await analyze_table(bucket, normalized_path, project_id)
        
        # Find the requested snapshot
        snapshot = None
        for snap in table_data.get("snapshots", []):
            if snap.get("snapshotId") == snapshot_id:
                snapshot = snap
                break
        
        if not snapshot:
            raise HTTPException(
                status_code=404,
                detail=f"Snapshot {snapshot_id} not found"
            )
        
        # Get data files for this snapshot
        manifest_list = snapshot.get("manifestList", "")
        snapshot_files = []
        if manifest_list:
            try:
                snapshot_files = get_manifest_files(bucket, normalized_path, manifest_list, project_id)
            except Exception as e:
                print(f"Warning: Could not load manifest files for snapshot {snapshot_id}: {str(e)}")
        
        # Calculate partition stats for this snapshot
        partition_stats = []
        partition_map = {}
        for file in snapshot_files:
            partition = file.get("partition", {})
            partition_key = json.dumps(partition, sort_keys=True) if partition else "{}"
            if partition_key not in partition_map:
                partition_map[partition_key] = {
                    "partition": partition,
                    "fileCount": 0,
                    "recordCount": 0,
                    "totalSize": 0,
                }
            partition_map[partition_key]["fileCount"] += 1
            partition_map[partition_key]["recordCount"] += file.get("recordCount", 0)
            partition_map[partition_key]["totalSize"] += file.get("fileSizeInBytes", 0)
        
        partition_stats = list(partition_map.values())
        
        return {
            "snapshot": snapshot,
            "dataFiles": snapshot_files,
            "partitionStats": partition_stats,
            "statistics": snapshot.get("statistics", {}),
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get snapshot data: {str(e)}\n\n{traceback.format_exc()}"
        )


@app.get("/sample")
async def get_sample_data(
    bucket: str,
    path: str,
    limit: int = 100,
    snapshot_id: Optional[str] = None,
    project_id: Optional[str] = None
):
    """Get sample data from the table"""
    try:
        normalized_path = path.strip("/")
        storage_client = get_storage_client(project_id)
        bucket_obj = storage_client.bucket(bucket)
        
        # Get table metadata to find data files
        table_data = await analyze_table(bucket, normalized_path, project_id)
        
        # Get data files for the specified snapshot or current snapshot
        data_files = []
        if snapshot_id:
            # Convert string ID to int for comparison
            try:
                snapshot_id_int = int(snapshot_id)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid snapshot ID: {snapshot_id}"
                )
            
            # Get files for specific snapshot
            snapshot = None
            for snap in table_data.get("snapshots", []):
                snap_id = snap.get("snapshotId")
                # Handle both int and string snapshot IDs
                if isinstance(snap_id, str):
                    snap_id = int(snap_id)
                if snap_id == snapshot_id_int:
                    snapshot = snap
                    break
            if snapshot:
                manifest_list = snapshot.get("manifestList", "")
                if manifest_list:
                    data_files = get_manifest_files(bucket, normalized_path, manifest_list, project_id)
        else:
            # Use current snapshot files
            data_files = table_data.get("dataFiles", [])
        
        if not data_files:
            return {
                "rows": [],
                "columns": [],
                "totalRows": 0,
                "message": "No data files found"
            }
        
        # Read sample from first few Parquet files
        import pyarrow.parquet as pq
        import pyarrow as pa
        import pandas as pd
        from io import BytesIO
        
        sample_rows = []
        columns = []
        files_read = 0
        max_files_to_read = 3  # Read from first 3 files
        
        for data_file in data_files[:max_files_to_read]:
            if files_read >= max_files_to_read:
                break
            
            file_path = data_file.get("filePath", "")
            if not file_path or not file_path.endswith(".parquet"):
                continue
            
            try:
                # Download Parquet file
                # Handle both relative and absolute paths
                if file_path.startswith("gs://") or file_path.startswith("/"):
                    # Absolute path - extract just the path part
                    if file_path.startswith(f"gs://{bucket}/"):
                        full_path = file_path.replace(f"gs://{bucket}/", "")
                    elif file_path.startswith("/"):
                        full_path = file_path.lstrip("/")
                    else:
                        full_path = file_path
                elif file_path.startswith(normalized_path):
                    # Already includes the table path
                    full_path = file_path
                else:
                    # Relative path - prepend table path
                    full_path = f"{normalized_path}/{file_path}".replace("//", "/")
                
                blob = bucket_obj.blob(full_path)
                parquet_bytes = blob.download_as_bytes()
                
                # Read Parquet file
                parquet_file = pq.ParquetFile(BytesIO(parquet_bytes))
                table = parquet_file.read()
                
                # Get column names
                if not columns:
                    columns = [col for col in table.column_names]
                
                # Convert to list of dicts
                df = table.to_pandas()
                rows = df.head(limit - len(sample_rows)).to_dict('records')
                
                # Convert to JSON-serializable format
                for row in rows:
                    serialized_row = {}
                    for key, value in row.items():
                        if pd.isna(value):
                            serialized_row[key] = None
                        elif isinstance(value, (pd.Timestamp, datetime)):
                            serialized_row[key] = value.isoformat()
                        elif isinstance(value, (int, float, str, bool)) or value is None:
                            serialized_row[key] = value
                        else:
                            serialized_row[key] = str(value)
                    sample_rows.append(serialized_row)
                
                files_read += 1
                
                if len(sample_rows) >= limit:
                    break
                    
            except Exception as e:
                print(f"Warning: Could not read Parquet file {file_path}: {str(e)}")
                continue
        
        return {
            "rows": sample_rows,
            "columns": columns,
            "totalRows": len(sample_rows),
            "filesRead": files_read
        }
        
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get sample data: {str(e)}\n\n{traceback.format_exc()}"
        )


@app.get("/snapshot/compare")
async def compare_snapshots(
    bucket: str,
    path: str,
    snapshot_id_1: str,
    snapshot_id_2: str,
    project_id: Optional[str] = None
):
    """Compare two snapshots to see what changed"""
    try:
        normalized_path = path.strip("/")
        
        # Convert string IDs to int for comparison (handles large integers)
        try:
            snapshot_id_1_int = int(snapshot_id_1)
            snapshot_id_2_int = int(snapshot_id_2)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid snapshot IDs: {snapshot_id_1}, {snapshot_id_2}"
            )
        
        # Get full table analysis
        table_data = await analyze_table(bucket, normalized_path, project_id)
        
        # Find both snapshots - compare as integers
        snapshot1 = None
        snapshot2 = None
        for snap in table_data.get("snapshots", []):
            snap_id = snap.get("snapshotId")
            # Handle both int and string snapshot IDs
            if isinstance(snap_id, str):
                snap_id = int(snap_id)
            if snap_id == snapshot_id_1_int:
                snapshot1 = snap
            if snap_id == snapshot_id_2_int:
                snapshot2 = snap
        
        if not snapshot1:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id_1} not found")
        if not snapshot2:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id_2} not found")
        
        # Get data files for both snapshots
        files1 = []
        files2 = []
        
        if snapshot1.get("manifestList"):
            files1 = get_manifest_files(bucket, normalized_path, snapshot1["manifestList"], project_id)
        if snapshot2.get("manifestList"):
            files2 = get_manifest_files(bucket, normalized_path, snapshot2["manifestList"], project_id)
        
        # Create file path sets for comparison
        file_paths1 = {f.get("filePath") for f in files1 if f.get("filePath")}
        file_paths2 = {f.get("filePath") for f in files2 if f.get("filePath")}
        
        # Find added, removed, and modified files
        added_files = [f for f in files2 if f.get("filePath") not in file_paths1]
        removed_files = [f for f in files1 if f.get("filePath") not in file_paths2]
        
        # Files that exist in both (potentially modified)
        common_paths = file_paths1 & file_paths2
        modified_files = []
        for path in common_paths:
            file1 = next((f for f in files1 if f.get("filePath") == path), None)
            file2 = next((f for f in files2 if f.get("filePath") == path), None)
            if file1 and file2:
                # Check if file changed (size or record count)
                if (file1.get("fileSizeInBytes") != file2.get("fileSizeInBytes") or
                    file1.get("recordCount") != file2.get("recordCount")):
                    modified_files.append({
                        "filePath": path,
                        "before": file1,
                        "after": file2,
                        "changes": {
                            "sizeDelta": file2.get("fileSizeInBytes", 0) - file1.get("fileSizeInBytes", 0),
                            "recordDelta": file2.get("recordCount", 0) - file1.get("recordCount", 0),
                        }
                    })
        
        # Calculate statistics delta
        stats1 = snapshot1.get("statistics", {})
        stats2 = snapshot2.get("statistics", {})
        
        return {
            "snapshot1": snapshot1,
            "snapshot2": snapshot2,
            "addedFiles": added_files,
            "removedFiles": removed_files,
            "modifiedFiles": modified_files,
            "statistics": {
                "snapshot1": stats1,
                "snapshot2": stats2,
                "delta": {
                    "files": stats2.get("fileCount", 0) - stats1.get("fileCount", 0),
                    "records": stats2.get("recordCount", 0) - stats1.get("recordCount", 0),
                    "size": stats2.get("totalSize", 0) - stats1.get("totalSize", 0),
                }
            },
            "summary": {
                "addedCount": len(added_files),
                "removedCount": len(removed_files),
                "modifiedCount": len(modified_files),
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compare snapshots: {str(e)}\n\n{traceback.format_exc()}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

