from typing import Optional
import os
from google.cloud import storage
from google.cloud import resourcemanager_v3

def get_storage_client(project_id: Optional[str] = None, token: Optional[str] = None):
    """Get GCS storage client with credentials
    
    Credentials are resolved in this order:
    1. Bearer Token (if provided) -> User-Centric
    2. GOOGLE_APPLICATION_CREDENTIALS environment variable
    3. Application Default Credentials (ADC)
    """
    # 1. Try Bearer Token (User-Centric)
    if token:
        try:
            from google.oauth2.credentials import Credentials
            creds = Credentials(token=token)
            if project_id:
                return storage.Client(credentials=creds, project=project_id)
            return storage.Client(credentials=creds)
        except Exception as e:
            print(f"Error creating client from token: {e}")
            pass

    # 2. Try environment variable
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and os.path.exists(credentials_path):
        if project_id:
            return storage.Client.from_service_account_json(credentials_path, project=project_id)
        return storage.Client.from_service_account_json(credentials_path)
    
    # 3. Use Application Default Credentials (ADC)
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
