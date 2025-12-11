from fastapi import APIRouter, HTTPException, Depends
from typing import Optional, List
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

@router.get("/discover")
async def discover_tables(
    bucket: str,
    project_id: str = None,
    token: Optional[str] = Depends(get_current_user_token)
):
    """Discover Iceberg tables in a bucket."""
    try:
        client = get_storage_client(token=token, project_id=project_id)
        if not client:
            raise HTTPException(status_code=500, detail="Failed to initialize storage client")

        bucket_obj = client.bucket(bucket)
        tables = []
        
        # List all objects looking for 'metadata/*.json'
        # This is a simple heuristic. For large buckets, this might be slow.
        # A better approach would be to crawl folders.
        
        # For now, let's just look at top level folders and one level deep
        # or use a recursive function with depth limit
        
        async def check_folder(prefix=""):
            blobs = client.list_blobs(bucket_obj, prefix=prefix, delimiter="/")
            folders = list(blobs.prefixes)
            
            for folder in folders:
                # Check for metadata
                metadata_prefix = f"{folder}metadata/"
                metadata_blobs = list(client.list_blobs(bucket_obj, prefix=metadata_prefix, max_results=1))
                
                if metadata_blobs:
                    folder_path = folder.rstrip("/")
                    folder_name = folder_path.split("/")[-1]
                    tables.append({
                        "name": folder_name,
                        "location": f"gs://{bucket}/{folder_path}",
                        "bucket": bucket,
                        "path": folder_path,
                        "projectId": project_id
                    })
                else:
                    # Recurse if not a table (limit depth if needed, but here we just go 1 level deeper for now to avoid infinite loops in huge buckets)
                    # Actually, let's just do 2 levels for safety
                    if prefix.count("/") < 2:
                        await check_folder(folder)

        # Since we can't easily do async recursion with sync GCS client, we'll just do iterative or limited depth
        # Let's just list all blobs with 'metadata/' in name? No, that's too many.
        # Let's stick to the browsing approach or just return what we find in the current path if we were browsing.
        # But /discover implies finding them.
        
        # Let's try a smarter approach: list blobs that look like metadata files
        # match_glob is supported in newer library versions, but maybe not here.
        
        # Fallback: Just check top level folders for now
        blobs = client.list_blobs(bucket_obj, prefix="", delimiter="/")
        folders = list(blobs.prefixes)
        
        for folder in folders:
             metadata_prefix = f"{folder}metadata/"
             if list(client.list_blobs(bucket_obj, prefix=metadata_prefix, max_results=1)):
                 folder_path = folder.rstrip("/")
                 tables.append({
                     "name": folder_path.split("/")[-1],
                     "location": f"gs://{bucket}/{folder_path}",
                     "bucket": bucket,
                     "path": folder_path,
                     "projectId": project_id
                 })
        
        return {"tables": tables}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error discovering tables: {str(e)}")
