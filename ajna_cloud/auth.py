"""
Authentication Provider

Supports:
- Local mode (development, no auth required)
- Cognito mode (production JWT verification)
- Test mode (fixed test user)

Install: pip install ajna-backend-sdk
"""

import os
import json
import logging
import time
from typing import Any, Callable, Dict, Optional
from functools import wraps

logger = logging.getLogger(__name__)


# ─── Providers ──────────────────────────────────────────────────────────────────

class AuthProvider:
    """Base auth provider interface"""

    def authenticate(self, event: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def get_user_id(self, event: Dict[str, Any]) -> Optional[str]:
        raise NotImplementedError


class LocalAuthProvider(AuthProvider):
    """Development auth — no authentication required"""

    def authenticate(self, event: Dict[str, Any]) -> Dict[str, Any]:
        headers = event.get('headers', {}) or {}
        user_id = (
            headers.get('X-User-ID') or headers.get('x-user-id') or 'local-dev-user'
        )
        return {
            'user_id': user_id,
            'email': f'{user_id}@localhost',
            'role': 'admin',
            'auth_mode': 'local'
        }

    def get_user_id(self, event: Dict[str, Any]) -> Optional[str]:
        headers = event.get('headers', {}) or {}
        return headers.get('X-User-ID') or headers.get('x-user-id') or 'local-dev-user'


class CognitoAuthProvider(AuthProvider):
    """AWS Cognito JWT verification"""

    def __init__(self, user_pool_id: str, client_id: str, region: str = 'us-east-1'):
        self.user_pool_id = user_pool_id
        self.client_id = client_id
        self.region = region
        self._jwks = None
        self._jwks_url = (
            f"https://cognito-idp.{region}.amazonaws.com/"
            f"{user_pool_id}/.well-known/jwks.json"
        )

    def _get_jwks(self) -> Dict:
        if self._jwks is None:
            import requests
            response = requests.get(self._jwks_url, timeout=5)
            response.raise_for_status()
            self._jwks = response.json()
        return self._jwks

    def authenticate(self, event: Dict[str, Any]) -> Dict[str, Any]:
        headers = event.get('headers', {}) or {}
        auth_header = headers.get('Authorization') or headers.get('authorization', '')

        if not auth_header:
            raise AuthError(401, "Missing Authorization header")

        token = auth_header.replace('Bearer ', '').strip()
        if not token:
            raise AuthError(401, "Missing token")

        try:
            from jose import jwt, JWTError

            jwks = self._get_jwks()
            unverified_header = jwt.get_unverified_header(token)
            kid = unverified_header.get('kid')

            key = None
            for k in jwks.get('keys', []):
                if k.get('kid') == kid:
                    key = k
                    break

            if not key:
                raise AuthError(401, "Token key not found in JWKS")

            claims = jwt.decode(
                token, key,
                algorithms=['RS256'],
                audience=self.client_id,
                issuer=f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}"
            )

            # Determine role: prefer custom:role attribute, then fall back to Cognito group membership
            groups = claims.get('cognito:groups', []) or []
            role = claims.get('custom:role') or ('admin' if 'admin' in groups else 'user')

            # Determine scopes (OAuth2.0)
            scope_str = claims.get('scope', '')
            scopes = scope_str.split(' ') if scope_str else []

            # Determine enterprise tenant context
            tenant_id = claims.get('custom:tenant_id') or claims.get('tenant_id')

            return {
                'user_id': claims.get('sub'),
                'email': claims.get('email'),
                'role': role,
                'groups': groups,
                'scopes': scopes,
                'tenant_id': tenant_id,
                'auth_mode': 'cognito',
                'claims': claims
            }

        except ImportError:
            logger.warning("python-jose not installed, using API Gateway claims")
            return self._extract_api_gateway_claims(event)
        except AuthError:
            raise
        except Exception as e:
            logger.error(f"JWT verification failed: {e}")
            raise AuthError(401, f"Invalid token: {e}")

    def _extract_api_gateway_claims(self, event: Dict[str, Any]) -> Dict[str, Any]:
        claims = (
            event.get('requestContext', {})
            .get('authorizer', {})
            .get('claims', {})
        )
        if not claims:
            raise AuthError(401, "No claims found in request context")

        # Determine role: prefer custom:role attribute, then fall back to Cognito group membership
        groups = claims.get('cognito:groups')
        if isinstance(groups, str):
            # API Gateway sometimes stringifies arrays
            import ast
            try:
                groups = ast.literal_eval(groups)
            except:
                groups = [groups]
        groups = groups or []
        role = claims.get('custom:role') or ('admin' if 'admin' in groups else 'user')

        # Determine scopes (OAuth2.0)
        scope_str = claims.get('scope', '')
        scopes = scope_str.split(' ') if scope_str else []

        # Determine enterprise tenant context
        tenant_id = claims.get('custom:tenant_id') or claims.get('tenant_id')

        return {
            'user_id': claims.get('sub'),
            'email': claims.get('email'),
            'role': role,
            'groups': groups,
            'scopes': scopes,
            'tenant_id': tenant_id,
            'auth_mode': 'cognito',
            'claims': claims
        }

    def get_user_id(self, event: Dict[str, Any]) -> Optional[str]:
        claims = (
            event.get('requestContext', {})
            .get('authorizer', {})
            .get('claims', {})
        )
        if 'sub' in claims:
            return claims['sub']
        headers = event.get('headers', {}) or {}
        return headers.get('X-User-ID') or headers.get('x-user-id')


class TestAuthProvider(AuthProvider):
    """Test auth with a fixed test user"""

    def authenticate(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'user_id': 'test-user-001',
            'email': 'test@example.com',
            'role': 'admin',
            'auth_mode': 'test'
        }

    def get_user_id(self, event: Dict[str, Any]) -> Optional[str]:
        return 'test-user-001'


# ─── Auth Error ─────────────────────────────────────────────────────────────────

class AuthError(Exception):
    """Authentication error with HTTP status code"""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


# ─── Factory ────────────────────────────────────────────────────────────────────

class AuthFactory:
    """Factory for creating auth providers based on AUTH_MODE"""

    _provider: Optional[AuthProvider] = None

    @classmethod
    def get_provider(cls) -> AuthProvider:
        if cls._provider is None:
            mode = os.environ.get('AUTH_MODE', 'local').lower()
            cls._provider = cls._create_provider(mode)
        return cls._provider

    @classmethod
    def _create_provider(cls, mode: str) -> AuthProvider:
        if mode == 'cognito':
            user_pool_id = os.environ.get('COGNITO_USER_POOL_ID')
            client_id = os.environ.get('COGNITO_CLIENT_ID')
            region = os.environ.get('COGNITO_REGION', 'us-east-1')

            if not user_pool_id or not client_id:
                logger.warning("Cognito config missing, falling back to local auth")
                return LocalAuthProvider()

            return CognitoAuthProvider(user_pool_id, client_id, region)
        elif mode == 'test':
            return TestAuthProvider()
        else:
            return LocalAuthProvider()

    @classmethod
    def reset(cls):
        """Reset cached provider (useful for testing)"""
        cls._provider = None


# ─── Decorators ─────────────────────────────────────────────────────────────────

def _get_respond():
    """Lazy import of respond to avoid circular deps"""
    try:
        from ajna_cloud.http import respond
        return respond
    except ImportError:
        # Minimal fallback
        import json as _json
        def respond(status_code, body):
            return {
                'statusCode': status_code,
                'headers': {'Content-Type': 'application/json'},
                'body': _json.dumps(body, default=str) if body else ''
            }
        return respond


def require_auth(func: Callable) -> Callable:
    """Decorator that enforces authentication. Injects user info into context['auth']."""
    @wraps(func)
    def wrapper(event: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        provider = AuthFactory.get_provider()
        respond = _get_respond()

        try:
            user_info = provider.authenticate(event)
            context['auth'] = user_info
            return func(event, context)
        except AuthError as e:
            return respond(e.status_code, {'error': e.message})
        except Exception as e:
            logger.error(f"Auth error: {e}")
            return respond(401, {'error': 'Authentication failed'})

    return wrapper


def require_roles(allowed_roles: list) -> Callable:
    """
    Decorator that requires the user to have at least one of the allowed roles.
    Matches against the user's role or their Cognito groups.
    Usage: @require_roles(['admin', 'editor'])
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(event: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
            provider = AuthFactory.get_provider()
            respond = _get_respond()

            try:
                user_info = provider.authenticate(event)
                context['auth'] = user_info

                user_role = user_info.get('role', '')
                user_groups = user_info.get('groups', [])
                
                # Check if the user's explicit role OR any of their Cognito groups are in the allowed list
                has_access = (user_role in allowed_roles) or any(group in allowed_roles for group in user_groups)

                if not has_access:
                    msg = f"Forbidden: Requires one of {allowed_roles}"
                    return respond(403, {'error': msg})

                return func(event, context)
            except AuthError as e:
                return respond(e.status_code, {'error': e.message})
            except Exception as e:
                logger.error(f"Auth error: {e}")
                return respond(401, {'error': 'Authentication failed'})

        return wrapper
    return decorator

# Maintain backward compatibility for existing code that uses @require_admin
require_admin = require_roles(['admin', 'SystemAdmin'])


def require_scopes(required_scopes: list) -> Callable:
    """
    Decorator that requires the user to have ALL of the specified OAuth2 scopes.
    Usage: @require_scopes(['read:devices', 'write:devices'])
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(event: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
            provider = AuthFactory.get_provider()
            respond = _get_respond()

            try:
                user_info = provider.authenticate(event)
                context['auth'] = user_info

                user_scopes = user_info.get('scopes', [])
                
                # Check if the user has ALL required scopes
                has_access = all(scope in user_scopes for scope in required_scopes)

                if not has_access:
                    msg = f"Forbidden: Requires scopes {required_scopes}"
                    return respond(403, {'error': msg})

                return func(event, context)
            except AuthError as e:
                return respond(e.status_code, {'error': e.message})
            except Exception as e:
                logger.error(f"Auth error: {e}")
                return respond(401, {'error': 'Authentication failed'})

        return wrapper
    return decorator


def get_user_id(event: Dict[str, Any]) -> Optional[str]:
    """Convenience function to extract user ID from event"""
    provider = AuthFactory.get_provider()
    return provider.get_user_id(event)
