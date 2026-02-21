"""
HTTP Utilities for Lambda Handlers

Provides:
- Lambda response builder with CORS headers
- User ID extraction from events
- Request body parsing
- Query parameter extraction

Install: pip install ajna-backend-sdk
"""

import json
from typing import Any, Dict, Optional


def get_cors_headers(event: Dict[str, Any] = None) -> Dict[str, str]:
    """Return CORS headers for Lambda responses"""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Credentials": "true"
    }


def respond(
    status_code: int,
    body: Any,
    is_base64: bool = False,
    event: Dict = None
) -> Dict[str, Any]:
    """
    Create a Lambda-compatible response with CORS headers.

    Args:
        status_code: HTTP status code
        body: Response body (dict, list, string, or None)
        is_base64: Whether body is base64 encoded
        event: Original event (for future CORS origin detection)
    """
    headers = get_cors_headers(event)

    if is_base64:
        headers['Content-Type'] = 'application/octet-stream'
    else:
        headers['Content-Type'] = 'application/json'

    if body is None:
        formatted_body = ''
    elif isinstance(body, (dict, list)):
        formatted_body = json.dumps(body, default=str)
    else:
        formatted_body = body

    return {
        "statusCode": status_code,
        "headers": headers,
        "body": formatted_body,
        "isBase64Encoded": is_base64
    }


def get_user_id(event: Dict[str, Any]) -> Optional[str]:
    """Extract user ID from event (Cognito claims → headers → None)"""
    # Try Cognito claims
    try:
        claims = (
            event.get('requestContext', {})
            .get('authorizer', {})
            .get('claims', {})
        )
        if 'sub' in claims:
            return claims['sub']
    except Exception:
        pass

    # Try headers
    try:
        headers = event.get('headers', {}) or {}
        user_id = headers.get('X-User-ID') or headers.get('x-user-id')
        if user_id:
            return user_id
    except Exception:
        pass

    return None


def parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    """Parse JSON body from Lambda event"""
    body = event.get('body')
    if not body:
        return {}
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body


def get_query_params(event: Dict[str, Any]) -> Dict[str, str]:
    """Get query string parameters from Lambda event"""
    return event.get('queryStringParameters') or {}
