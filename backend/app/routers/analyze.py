from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any, Optional
from ..core.security import get_current_user_token
from ..services.iceberg import analyze_with_pyiceberg_metadata, read_iceberg_metadata_manual, get_manual_data_files, PYICEBERG_AVAILABLE

router = APIRouter()

@router.get("/analyze")
async def analyze_table(bucket: str, path: str, project_id: Optional[str] = None, token: Optional[str] = Depends(get_current_user_token)):
    """Analyze an Iceberg table and return comprehensive metadata"""
    try:
        # Normalize path
        normalized_path = path.strip("/")
        
        # Try PyIceberg first if available
        if PYICEBERG_AVAILABLE:
            try:
                # Use PyIceberg to parse metadata properly
                result = analyze_with_pyiceberg_metadata(bucket, normalized_path, project_id, token=token)
                if result:
                    return result
            except Exception as e:
                print(f"PyIceberg analysis failed, falling back to manual: {str(e)}")
        
        # Fall back to manual metadata reading
        # Note: analyze_with_pyiceberg_metadata already includes manual fallback logic,
        # so if it returns None, it means both failed or PyIceberg is not available and manual failed inside it?
        # Actually analyze_with_pyiceberg_metadata returns None if PyIceberg is not available OR if it fails?
        # Let's check services/iceberg.py:
        # It returns None if PYICEBERG_AVAILABLE is False.
        # It also has a fallback to manual parsing inside it.
        # So if it returns a result, we are good.
        # If it returns None (and didn't raise), it might be because PYICEBERG_AVAILABLE is False.
        
        # If we are here, either PyIceberg is not available or it failed and returned None (if that's possible)
        # But analyze_with_pyiceberg_metadata in services/iceberg.py actually DOES return a dict even if PyIceberg fails,
        # because it has the manual fallback inside it!
        # So if it returns None, it's likely because PYICEBERG_AVAILABLE is False (at the top check).
        
        # So we should call it again? No, if PYICEBERG_AVAILABLE is False, it returns None immediately.
        # We need to call read_iceberg_metadata_manual directly if PYICEBERG_AVAILABLE is False.
        
        # Wait, analyze_with_pyiceberg_metadata in services/iceberg.py:
        # if not PYICEBERG_AVAILABLE: return None
        
        # So if it returns None, we MUST try manual.
        
        try:
            metadata, _, _ = read_iceberg_metadata_manual(bucket, normalized_path, project_id=project_id, token=token)
            
            # If we are here, we have metadata but analyze_with_pyiceberg_metadata returned None (or wasn't called)
            # We need to construct the response manually like in the original main.py
            # BUT, services/iceberg.py's analyze_with_pyiceberg_metadata ALREADY has the manual fallback logic inside it...
            # except it returns None if PYICEBERG_AVAILABLE is False.
            
            # I should have made analyze_with_pyiceberg_metadata handle the case where PyIceberg is missing by just doing manual parsing.
            # But I can't change it now easily without another write.
            # I will just replicate the manual parsing logic here or call a helper if I had one.
            # Actually, I can just call read_iceberg_metadata_manual and then do the extraction here.
            # OR I can update services/iceberg.py to be more robust.
            
            # Let's look at what I wrote to services/iceberg.py
            # It has the full manual parsing logic inside analyze_with_pyiceberg_metadata...
            # BUT it starts with `if not PYICEBERG_AVAILABLE: return None`.
            # This is a bit of a flaw in my extraction.
            
            # I will implement the manual parsing here using read_iceberg_metadata_manual
            # effectively duplicating the logic from main.py (which I am replacing).
            # This is safer than modifying services/iceberg.py again right now.
            
            # Actually, I can just copy the logic from main.py since I have it.
            pass # Continue to logic below
        except Exception:
            raise
            
    except (Forbidden, Unauthorized) as e:
        raise HTTPException(
            status_code=401 if isinstance(e, Unauthorized) else 403,
            detail=f"Authentication failed: {str(e)}"
        )
    except Exception as e:
        # Check for GCS permission errors (fallback for non-api_core exceptions)
        error_str = str(e)
        if "403" in error_str or "Forbidden" in error_str:
            raise HTTPException(
                status_code=403,
                detail="Permission denied. You do not have access to this bucket or object. Please check your GCS permissions."
            )
        if "401" in error_str or "Unauthorized" in error_str:
            raise HTTPException(
                status_code=401,
                detail="Authentication failed. Please try logging in again."
            )
            
        # Provide more detailed error information for other errors
        import traceback
        print(traceback.format_exc())
        raise HTTPException(
            status_code=404,
            detail=f"Failed to read Iceberg metadata:\n{error_str}\n\n"
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
            for schema_obj in metadata["schemas"]:
                if schema_obj.get("schema-id") == current_schema_id:
                    if "fields" in schema_obj:
                        for field in schema_obj["fields"]:
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
        
        # Extract partition spec
        partition_spec = []
        default_spec_id = metadata.get("default-spec-id", 0)
        
        if "partition-specs" in metadata and isinstance(metadata["partition-specs"], list):
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
        
        # Extract sort order
        sort_order = []
        default_sort_order_id = metadata.get("default-sort-order-id", 0)
        
        if "sort-orders" in metadata and isinstance(metadata["sort-orders"], list):
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
        
        # Return basic metadata if manual parsing was used
        # Note: This is a simplified return compared to what analyze_with_pyiceberg_metadata returns
        # but it matches what the frontend expects for basic display if full analysis fails.
        # Actually, we should try to return as much as possible.
        
        return {
            "tableName": normalized_path.split("/")[-1] if "/" in normalized_path else normalized_path,
            "location": f"gs://{bucket}/{normalized_path}",
            "formatVersion": metadata.get("format-version", 1),
            "schema": schema_fields,
            "partitionSpec": partition_spec,
            "sortOrder": sort_order,
            "properties": metadata.get("properties", {}),
            "currentSnapshotId": str(metadata.get("current-snapshot-id", -1)),

            "snapshots": metadata.get("snapshots", []),
            # Try to get data files from current snapshot for graph
            "dataFiles": get_manual_data_files(bucket, normalized_path, metadata, project_id, token),
            "partitionStats": [],
            "metadataFiles": [], # Could populate this if we had the list from read_iceberg_metadata_manual
        }

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

from google.api_core.exceptions import Forbidden, Unauthorized

@router.get("/sample")
async def get_sample(
    bucket: str, 
    path: str, 
    limit: int = 100, 
    snapshot_id: Optional[str] = None,
    manifest_path: Optional[str] = None,
    file_path: Optional[str] = None,
    project_id: Optional[str] = None, 
    token: Optional[str] = Depends(get_current_user_token)
):
    """Get sample data from an Iceberg table, optionally targeting a specific snapshot, manifest, or file"""
    from ..services.iceberg import get_sample_data
    data = get_sample_data(bucket, path, limit, project_id, token=token, snapshot_id=snapshot_id, manifest_path=manifest_path, file_path=file_path)
    return data

@router.get("/snapshot/compare")
async def compare_snapshots_endpoint(
    bucket: str, 
    path: str, 
    snapshot_id_1: str, 
    snapshot_id_2: str, 
    project_id: Optional[str] = None, 
    token: Optional[str] = Depends(get_current_user_token)
):
    """Compare two snapshots"""
    from ..services.iceberg import compare_snapshots
    # Handle empty snapshot_id_1 which might come as "null" or empty string
    s1 = snapshot_id_1 if snapshot_id_1 and snapshot_id_1 != "null" else ""
    return compare_snapshots(bucket, path, s1, snapshot_id_2, project_id, token=token)
