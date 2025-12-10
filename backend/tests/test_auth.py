import pytest
from unittest.mock import patch, MagicMock
from app.services.gcs import get_storage_client
import os

def test_get_storage_client_with_token():
    """Test that get_storage_client uses the provided token"""
    token = "fake-token"
    project_id = "test-project"
    
    with patch("app.services.gcs.storage.Client") as mock_client:
        with patch("google.oauth2.credentials.Credentials") as mock_creds:
            get_storage_client(project_id=project_id, token=token)
            
            # Verify Credentials was initialized with the token
            mock_creds.assert_called_with(token=token)
            
            # Verify Client was initialized with credentials and project
            mock_client.assert_called_with(credentials=mock_creds.return_value, project=project_id)

def test_get_storage_client_no_token_adc():
    """Test that get_storage_client falls back to ADC when no token is provided"""
    project_id = "test-project"
    
    # Ensure no env var is set
    with patch.dict(os.environ, {}, clear=True):
        with patch("app.services.gcs.storage.Client") as mock_client:
            get_storage_client(project_id=project_id)
            
            # Verify Client was initialized with just project (ADC behavior)
            mock_client.assert_called_with(project=project_id)

def test_get_storage_client_env_var():
    """Test that get_storage_client uses GOOGLE_APPLICATION_CREDENTIALS if set"""
    project_id = "test-project"
    fake_path = "/tmp/fake-creds.json"
    
    with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": fake_path}):
        with patch("os.path.exists", return_value=True):
            with patch("app.services.gcs.storage.Client.from_service_account_json") as mock_from_json:
                get_storage_client(project_id=project_id)
                
                # Verify from_service_account_json was called
                mock_from_json.assert_called_with(fake_path, project=project_id)
