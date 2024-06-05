# Some Useful Tips regarding the Airflow Secret management
- secrets tagging
 ```
 pip install google-cloud-secret-manager
 from google.cloud import secretmanager
 def fetch_secrets(secret_name):
    """Fetches secret name from secret manager.

    Args:
        secret_name (str): Name of secret being pulled.

    Returns:
        dict: decoded payload of secret response.
    """
    _, project_id = default()
    _client = secretmanager.SecretManagerServiceClient()
    secret_version_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = _client.access_secret_version(name=secret_version_name)
    secret_payload = response.payload.data.decode("UTF-8")
    return secret_payload

 ```

- get secrets from SM
```
from google.cloud import secretmanager

SMTP_PWD = (
    f'projects/234342/secrets/airflow-config-smtp-conn/versions/latest'
)
secret_manager = secretmanager.SecretManagerServiceClient()
smtp_pwd_response = secret_manager.access_secret_version(
    request={'name': SMTP_PWD}
)
smtp_pwd_get = smtp_pwd_response.payload.data.decode('UTF-8')
```
# Airflow SM config
```
google-cloud-platform://?extra__google_cloud_platform__key_secret_name=suneb&extra__google_cloud_platform__project=coregcpproj&extra__google_cloud_platform__key_secret_project_id=hostprojectname&extra__google_cloud_platform__num_retries=0
```
