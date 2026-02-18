"""
Structured Logging System

Provides:
- JSON structured logging for production
- Human-readable logging for development
- Request/response logging middleware
- log_handler decorator for Lambda handlers
- Sensitive data masking

Install: pip install ajna-backend-sdk
"""

import os
import json
import logging
import sys
import time
import traceback
import uuid
from datetime import datetime
from typing import Any, Dict
from functools import wraps


# ─── JSON Formatter ────────────────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging in production"""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        for field in ('user_id', 'request_id', 'tenant_id', 'correlation_id'):
            if hasattr(record, field):
                log_obj[field] = getattr(record, field)

        if record.exc_info and record.exc_info[0]:
            log_obj['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }

        if hasattr(record, 'extra_data'):
            log_obj['data'] = record.extra_data

        return json.dumps(log_obj)


# ─── Logger ────────────────────────────────────────────────────────────────────

class Logger:
    """Enhanced singleton logger with structured logging and context"""

    _instance = None
    _logger = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._logger is None:
            self._setup_logger()

    def _setup_logger(self):
        env = os.environ.get('ENVIRONMENT', 'development')
        log_level = os.environ.get('LOG_LEVEL', 'INFO' if env == 'production' else 'DEBUG')

        self._logger = logging.getLogger('ajna-sdk')
        self._logger.setLevel(getattr(logging, log_level))
        self._logger.handlers = []

        handler = logging.StreamHandler(sys.stdout)

        if env == 'production':
            handler.setFormatter(JSONFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))

        self._logger.addHandler(handler)
        self._logger.propagate = False

    def _mask_sensitive_data(self, data: Any) -> Any:
        if isinstance(data, dict):
            masked = {}
            sensitive_keys = [
                'password', 'token', 'api_key', 'secret', 'authorization',
                'x-api-key', 'credit_card', 'ssn'
            ]
            for key, value in data.items():
                if any(s in key.lower() for s in sensitive_keys):
                    if isinstance(value, str) and len(value) > 4:
                        masked[key] = value[:2] + '*' * (len(value) - 4) + value[-2:]
                    else:
                        masked[key] = '***'
                elif isinstance(value, (dict, list)):
                    masked[key] = self._mask_sensitive_data(value)
                else:
                    masked[key] = value
            return masked
        elif isinstance(data, list):
            return [self._mask_sensitive_data(item) for item in data]
        return data

    def log(self, level: str, message: str, **kwargs):
        extra = {}
        for field in ('user_id', 'request_id', 'tenant_id', 'correlation_id'):
            if field in kwargs:
                extra[field] = kwargs.pop(field)

        if kwargs:
            if os.environ.get('MASK_SENSITIVE_DATA', 'true').lower() == 'true':
                kwargs = self._mask_sensitive_data(kwargs)
            extra['extra_data'] = kwargs

        log_method = getattr(self._logger, level.lower())
        log_method(message, extra=extra)

    def debug(self, message: str, **kwargs):
        self.log('debug', message, **kwargs)

    def info(self, message: str, **kwargs):
        self.log('info', message, **kwargs)

    def warning(self, message: str, **kwargs):
        self.log('warning', message, **kwargs)

    def error(self, message: str, **kwargs):
        self.log('error', message, **kwargs)

    def critical(self, message: str, **kwargs):
        self.log('critical', message, **kwargs)

    def exception(self, message: str, **kwargs):
        self._logger.exception(message, extra=kwargs)


# ─── Request Logger ────────────────────────────────────────────────────────────

class RequestLogger:
    """Middleware for logging API requests and responses"""

    def __init__(self, logger_instance: Logger):
        self.logger = logger_instance

    def log_request(self, event: Dict[str, Any], context: Dict[str, Any]) -> str:
        request_id = str(uuid.uuid4())

        method = event.get('httpMethod', 'UNKNOWN')
        path = event.get('path', '')
        headers = event.get('headers', {}) or {}

        user_id = headers.get('X-User-ID') or headers.get('x-user-id')
        tenant_id = headers.get('X-Tenant-Id') or headers.get('x-tenant-id', 'default')

        self.logger.info(
            f"Request: {method} {path}",
            request_id=request_id,
            user_id=user_id,
            tenant_id=tenant_id,
            method=method,
            path=path,
        )

        context['request_id'] = request_id
        return request_id

    def log_response(self, request_id: str, response: Dict[str, Any], duration_ms: float):
        status_code = response.get('statusCode', 0)
        log_data = {
            'request_id': request_id,
            'status_code': status_code,
            'duration_ms': round(duration_ms, 2)
        }
        if status_code < 400:
            self.logger.info(f"Response: {status_code}", **log_data)
        elif status_code < 500:
            self.logger.warning(f"Client error: {status_code}", **log_data)
        else:
            self.logger.error(f"Server error: {status_code}", **log_data)


# ─── Handler Decorator ─────────────────────────────────────────────────────────

def log_handler(func):
    """Decorator to log handler execution with timing and error handling"""
    @wraps(func)
    def wrapper(event, context):
        _logger = Logger()
        request_logger = RequestLogger(_logger)

        start_time = time.time()
        request_id = request_logger.log_request(event, context)

        try:
            response = func(event, context)
            duration_ms = (time.time() - start_time) * 1000
            request_logger.log_response(request_id, response, duration_ms)
            return response
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            _logger.exception(
                f"Handler error: {str(e)}",
                request_id=request_id,
                handler=func.__name__,
                duration_ms=duration_ms
            )
            from ajna_sdk.http import respond
            return respond(500, {
                'error': 'Internal server error',
                'request_id': request_id
            })

    return wrapper


# Singleton instance
logger = Logger()
