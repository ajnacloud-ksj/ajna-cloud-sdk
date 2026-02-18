# Ajna Backend SDK

Shared Python library for Ajna cloud backend services. Installable as a pip package.

## Components

| Module | Description |
|--------|-------------|
| `ajna_sdk.ibex` | IbexDB client with caching, Lambda invocation, batch ops |
| `ajna_sdk.auth` | Auth provider (Local/Cognito/Test) with decorators |
| `ajna_sdk.logger` | Structured JSON logging with request tracking |
| `ajna_sdk.http` | Lambda HTTP response utilities with CORS |

## Installation

```bash
# From GitHub (private repo)
pip install git+https://github.com/ajnacloud-ksj/ajna-backend-sdk.git

# Local development
pip install -e /path/to/ajna-backend-sdk
```

## Quick Start

```python
from ajna_sdk.ibex import OptimizedIbexClient, TenantManager
from ajna_sdk.auth import require_auth, get_user_id
from ajna_sdk.logger import logger, log_handler
from ajna_sdk.http import respond

# IbexDB
client = OptimizedIbexClient(
    api_url="https://smartlink.ajna.cloud/ibexdb",
    api_key="your-key",
    tenant_id="greenbox",
    namespace="telemetry"
)
result = client.query("devices", limit=10)

# Auth decorator
@log_handler
@require_auth
def my_handler(event, context):
    user_id = get_user_id(event)
    return respond(200, {"user": user_id})
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `IBEX_API_URL` | IbexDB API endpoint | — |
| `IBEX_API_KEY` | IbexDB API key | — |
| `TENANT_ID` | Database tenant ID | `default` |
| `DB_NAMESPACE` | Database namespace | `default` |
| `IBEX_LAMBDA_NAME` | Lambda function name for direct invocation | — |
| `AUTH_MODE` | Auth mode: `local`, `cognito`, `test` | `local` |
| `COGNITO_USER_POOL_ID` | Cognito User Pool ID | — |
| `COGNITO_CLIENT_ID` | Cognito App Client ID | — |
| `COGNITO_REGION` | Cognito region | `us-east-1` |
| `ENVIRONMENT` | Environment: `development`, `staging`, `production` | `development` |
| `LOG_LEVEL` | Logging level | `DEBUG` |
