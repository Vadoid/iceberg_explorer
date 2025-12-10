from typing import Optional
from fastapi import Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer(auto_error=False)

async def get_current_user_token(credentials: Optional[HTTPAuthorizationCredentials] = Security(security)) -> Optional[str]:
    if credentials:
        return credentials.credentials
    return None
