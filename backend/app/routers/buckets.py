from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any, Optional, Set
from ..core.security import get_current_user_token
from ..services.gcs import get_storage_client

router = APIRouter()

from google.api_core.exceptions import Forbidden, Unauthorized

@router.get("/buckets")
async def list_buckets(project_id: Optional[str] = None, token: Optional[str] = Depends(get_current_user_token)):
    """List all GCS buckets accessible with current credentials, optionally filtered by project"""
    client = get_storage_client(project_id=project_id, token=token)
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


@router.get("/discover")
async def discover_iceberg_tables(bucket: str, project_id: Optional[str] = None, token: Optional[str] = Depends(get_current_user_token)):
    """Recursively scan a bucket for all Iceberg tables by finding *.metadata.json files"""
    try:
        client = get_storage_client(project_id=project_id, token=token)
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


@router.get("/browse")
async def browse_bucket(bucket: str, path: str = "", project_id: Optional[str] = None, token: Optional[str] = Depends(get_current_user_token)):
    """Browse a GCS bucket and find Iceberg tables"""
    try:
        client = get_storage_client(project_id=project_id, token=token)
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
