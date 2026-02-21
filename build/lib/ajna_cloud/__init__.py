"""
Ajna Cloud SDK

Shared library for Ajna Cloud serverless backends:
- IbexDB client with caching and Lambda invocation
- Authentication provider (Local / Cognito / Test)
- Structured JSON logging
- Lambda HTTP response utilities
"""

__version__ = "0.2.1"

from ajna_cloud.ibex import OptimizedIbexClient, TenantManager
from ajna_cloud.auth import AuthFactory, require_auth, require_admin, require_roles, get_user_id
from ajna_cloud.logger import Logger, logger, log_handler
from ajna_cloud.http import respond, get_cors_headers

__all__ = [
    # IbexDB
    "OptimizedIbexClient",
    "TenantManager",
    # Auth
    "AuthFactory",
    "require_auth",
    "require_admin",
    "require_roles",
    "get_user_id",
    # Logging
    "Logger",
    "logger",
    "log_handler",
    # HTTP
    "respond",
    "get_cors_headers",
]
