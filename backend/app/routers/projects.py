from fastapi import APIRouter, HTTPException, Depends, Security
from typing import List, Dict, Any, Optional
import os
from google.cloud import resourcemanager_v3
from ..core.security import get_current_user_token
from ..services.gcs import get_resource_manager_client, get_storage_client

router = APIRouter()

@router.get("/projects")
async def list_projects(token: Optional[str] = Depends(get_current_user_token)):
    """List all GCP projects accessible with current credentials"""
    projects = []
    errors = []
    
    # Try Resource Manager API first
    try:
        client = get_resource_manager_client(token=token)
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
        storage_client = get_storage_client(token=token)
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
