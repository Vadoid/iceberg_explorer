from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
from google.cloud import bigquery
from ..core.security import get_current_user_token

router = APIRouter()

def get_bigquery_client(token: Optional[str] = None, project_id: Optional[str] = None):
    """Get a BigQuery client with user credentials if available."""
    try:
        from google.oauth2.credentials import Credentials
        
        if token:
            creds = Credentials(token=token)
            return bigquery.Client(credentials=creds, project=project_id)
        
        # Fallback to ADC
        return bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Error creating BigQuery client: {e}")
        return None

@router.get("/bigquery/datasets")
async def list_datasets(
    project_id: str,
    token: Optional[str] = Depends(get_current_user_token)
):
    """List datasets in a project."""
    try:
        client = get_bigquery_client(token=token, project_id=project_id)
        if not client:
            raise HTTPException(status_code=500, detail="Failed to initialize BigQuery client")
            
        datasets = list(client.list_datasets())
        return {
            "datasets": [
                {
                    "dataset_id": d.dataset_id,
                    "project": d.project,
                    "full_dataset_id": d.full_dataset_id,
                    "labels": d.labels
                }
                for d in datasets
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing datasets: {str(e)}")

@router.get("/bigquery/tables")
async def list_tables(
    project_id: str,
    dataset_id: str,
    token: Optional[str] = Depends(get_current_user_token)
):
    """List tables in a dataset."""
    try:
        client = get_bigquery_client(token=token, project_id=project_id)
        if not client:
            raise HTTPException(status_code=500, detail="Failed to initialize BigQuery client")
            
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref))
        
        return {
            "tables": [
                {
                    "table_id": t.table_id,
                    "table_type": t.table_type,
                    "full_table_id": t.full_table_id,
                    "created": t.created.isoformat() if t.created else None,
                    "expires": t.expires.isoformat() if t.expires else None
                }
                for t in tables
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing tables: {str(e)}")

@router.get("/bigquery/search-iceberg")
async def search_iceberg_tables(
    project_id: str,
    token: Optional[str] = Depends(get_current_user_token)
):
    """Search for Iceberg tables across all datasets in the project."""
    try:
        client = get_bigquery_client(token=token, project_id=project_id)
        if not client:
            raise HTTPException(status_code=500, detail="Failed to initialize BigQuery client")

        found_tables = []
        datasets = list(client.list_datasets())

        # TODO: Consider parallelizing this if performance becomes an issue
        for dataset in datasets:
            try:
                dataset_ref = client.dataset(dataset.dataset_id, project=project_id)
                # Use iterator to handle potential partial results or specific iteration errors
                tables = client.list_tables(dataset_ref)
                
                for table_item in tables:
                    try:
                        if table_item.table_type == 'EXTERNAL':
                            # Fetch full table details to check external configuration
                            table = client.get_table(table_item.reference)
                            ext_config = table.external_data_configuration
                            
                            # Robustly check for source_format and source_uris
                            source_format = None
                            source_uris = []
                            
                            if ext_config:
                                # Handle both object and dict-like access for safety
                                if hasattr(ext_config, 'source_format'):
                                    source_format = ext_config.source_format
                                elif isinstance(ext_config, dict):
                                    source_format = ext_config.get('source_format')
                                    
                                if hasattr(ext_config, 'source_uris'):
                                    source_uris = ext_config.source_uris
                                elif isinstance(ext_config, dict):
                                    source_uris = ext_config.get('source_uris', [])

                            if source_format == 'ICEBERG':
                                found_tables.append({
                                    "dataset_id": dataset.dataset_id,
                                    "table_id": table.table_id,
                                    "full_table_id": f"{project_id}.{dataset.dataset_id}.{table.table_id}",
                                    "location": source_uris[0] if source_uris else None,
                                    "created": table.created.isoformat() if table.created else None
                                })
                    except Exception as e:
                        # Log error but continue scanning other tables
                        print(f"Warning: Error inspecting table {table_item.table_id}: {e}")
                        continue
            except Exception as e:
                # Log error but continue scanning other datasets
                print(f"Warning: Error scanning dataset {dataset.dataset_id}: {e}")
                continue

        return {"tables": found_tables}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching Iceberg tables: {str(e)}")
