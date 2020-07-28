"""
Test file to test gcs.py
"""
import pytest
from merge_automation.cloud.scripts.main.cloud import Cloud
from merge_automation.cloud.scripts.main.cloud_factory import gcs

def test_invalid_path():
    """
    this function will test whether exception
    is thrown if we pass invalid path
    :return: None
    """
    invalid_path="/vault/abc.json"

    with pytest.raises(StorageCredentialNotFound):
        get_secrets(invalid_path)

        
def test_invalidJson():
     """
    this function will test whether exception
    is thrown if we pass a file which is not JSON 
    :return: None
    """
    ivalid_json="/vault/secrets/gcp_credentials.csv"

    with pytest.raises(json.JSONDecodeError):
        get_secrets(path)

