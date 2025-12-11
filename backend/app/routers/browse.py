from fastapi import APIRouter, HTTPException, Depends
from typing import Optional, List, Dict, Any
from google.cloud import storage
from ..core.security import get_current_user_token

router = APIRouter()

def get_storage_client(token: Optional[str] = None, project_id: Optional[str] = None):
    try:
        from google.oauth2.credentials import Credentials
        if token:
            creds = Credentials(token=token)
            return storage.Client(credentials=creds, project=project_id)
        return storage.Client(project=project_id)
    except Exception as e:
        print(f"Error creating storage client: {e}")
        return None

@router.get("/browse")
async def browse_bucket(
    bucket: str,
    path: str = "",
    project_id: str = None,
    token: Optional[str] = Depends(get_current_user_token)
):
    """List contents of a GCS bucket path."""
    try:
        client = get_storage_client(token=token, project_id=project_id)
        if not client:
            raise HTTPException(status_code=500, detail="Failed to initialize storage client")

        bucket_obj = client.bucket(bucket)
        
        # Ensure path ends with / if not empty
        prefix = path
        if prefix and not prefix.endswith("/"):
            prefix += "/"
            
        blobs = list(client.list_blobs(bucket_obj, prefix=prefix, delimiter="/"))
        
        folders = list(blobs.prefixes) if blobs.prefixes else []
        items = []
        
        # Process folders (prefixes)
        for folder in folders:
            folder_name = folder.rstrip("/").split("/")[-1]
            full_path = folder.rstrip("/")
            
            # Check if it's an Iceberg table (has metadata folder)
            is_iceberg = False
            try:
                metadata_prefix = f"{folder}metadata/"
                metadata_blobs = list(client.list_blobs(bucket_obj, prefix=metadata_prefix, max_results=1))
                if metadata_blobs:
                    is_iceberg = True
            except:
                pass

            item = {
                "name": folder_name,
                "type": "iceberg_table" if is_iceberg else "folder",
                "path": full_path
            }
            
            if is_iceberg:
                item["table"] = {
                    "name": folder_name,
                    "location": f"gs://{bucket}/{full_path}",
                    "bucket": bucket,
                    "path": full_path,
                    "projectId": project_id
                }
            
            items.append(item)

        # Process files (blobs)
        for blob in blobs:
            if blob.name == prefix:
                continue
            name = blob.name.split("/")[-1]
            if not name:
                continue
                
            items.append({
                "name": name,
                "type": "file",
                "path": blob.name,
                "size": blob.size,
                "contentType": blob.content_type,
                "timeCreated": blob.time_created.isoformat() if blob.time_created else None
            })

        return {
            "folders": [f.rstrip("/").split("/")[-1] for f in folders],
            "items": items
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error browsing bucket: {str(e)}")
