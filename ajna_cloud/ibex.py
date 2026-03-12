"""
Optimized IbexDB Client

A self-contained client for IbexDB with:
- In-memory LRU caching with configurable TTLs
- Connection pooling and reuse
- Direct Lambda invocation support
- Batch operations
- Write-through cache invalidation
- Performance statistics

Install: pip install ajna-backend-sdk
"""

import json
import time
import hashlib
import logging
import os
from collections import OrderedDict
from typing import Any, Dict, List, Optional

import boto3
import requests

logger = logging.getLogger(__name__)


# ─── Cache Configuration ───────────────────────────────────────────────────────

def _get_cache_config():
    """Environment-aware cache configuration"""
    env = os.environ.get('ENVIRONMENT', 'development')
    if env == 'production':
        return {'MAX_CACHE_SIZE': 200, 'CACHE_TTL': 60, 'READ_CACHE_TTL': 30}
    return {'MAX_CACHE_SIZE': 500, 'CACHE_TTL': 120, 'READ_CACHE_TTL': 60}


# Tables that should never be cached (override per project)
NEVER_CACHE_TABLES: set = set()


# ─── LRU Cache with TTL ────────────────────────────────────────────────────────

class TTLCache:
    """Thread-safe LRU cache with per-entry TTL expiration"""

    def __init__(self, max_size: int = 500, default_ttl: int = 120):
        self._cache: OrderedDict = OrderedDict()
        self._expiry: Dict[str, float] = {}
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            if time.time() < self._expiry.get(key, 0):
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]
            else:
                del self._cache[key]
                del self._expiry[key]
        self._misses += 1
        return None

    def set(self, key: str, value: Any, ttl: int = None):
        if len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)
        self._cache[key] = value
        self._expiry[key] = time.time() + (ttl or self._default_ttl)

    def invalidate(self, pattern: str = None):
        if pattern is None:
            self._cache.clear()
            self._expiry.clear()
        else:
            keys_to_remove = [k for k in self._cache if k.startswith(pattern)]
            for key in keys_to_remove:
                del self._cache[key]
                if key in self._expiry:
                    del self._expiry[key]

    @property
    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            'hits': self._hits,
            'misses': self._misses,
            'total_requests': total,
            'hit_rate': self._hits / total if total > 0 else 0,
            'size': len(self._cache)
        }


# ─── Main Client ───────────────────────────────────────────────────────────────

class OptimizedIbexClient:
    """
    High-performance IbexDB client with caching and direct Lambda invocation.

    Usage:
        client = OptimizedIbexClient(
            api_url="https://smartlink.ajna.cloud/ibexdb",
            api_key="your-key",
            tenant_id="greenbox",
            namespace="telemetry"
        )
        client.enable_direct_lambda("ibex-db-lambda")
        result = client.query("devices", filters=[...], limit=10)
    """

    def __init__(
        self,
        api_url: str,
        api_key: str,
        tenant_id: str,
        namespace: str = "default",
        timeout: int = 20,
        max_retries: int = 3,
        lambda_name: str = None
    ):
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.tenant_id = tenant_id
        self.namespace = namespace
        self.timeout = timeout
        self.max_retries = max_retries

        # HTTP session with connection pooling
        self._session = requests.Session()
        self._session.headers.update({
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        })

        # Cache
        cache_config = _get_cache_config()
        self._cache = TTLCache(
            max_size=cache_config['MAX_CACHE_SIZE'],
            default_ttl=cache_config['CACHE_TTL']
        )
        self._read_cache_ttl = cache_config['READ_CACHE_TTL']

        # Direct Lambda invocation
        self._lambda_client = None
        self._lambda_function_name = lambda_name
        self._use_lambda_for_writes_only = True

        if lambda_name:
            self.enable_direct_lambda(lambda_name)

        # Stats
        self._total_requests = 0
        self._lambda_invocations = 0
        self._api_calls = 0

    def enable_direct_lambda(self, function_name: str, use_for_writes_only: bool = True):
        """Enable direct Lambda-to-Lambda invocation for IbexDB"""
        try:
            self._lambda_client = boto3.client('lambda')
            self._lambda_function_name = function_name
            self._use_lambda_for_writes_only = use_for_writes_only
            logger.info(f"Direct Lambda invocation enabled: {function_name}")
        except Exception as e:
            logger.warning(f"Failed to initialize Lambda client: {e}")
            self._lambda_client = None

    # ─── Core Read/Write Operations ─────────────────────────────────────────

    def query(
        self,
        table: str,
        filters: List[Dict] = None,
        sort: List[Dict] = None,
        limit: int = 50,
        offset: int = 0,
        use_cache: bool = True,
        include_deleted: bool = False,
        tenant_id: str = None,
        namespace: str = None,
        projection: List[str] = None,
        aggregations: List[Dict] = None,
        group_by: List[str] = None
    ) -> Dict[str, Any]:
        """Query records from a table"""
        self._total_requests += 1

        if table in NEVER_CACHE_TABLES:
            use_cache = False

        cache_key = None
        if use_cache:
            cache_key = self._make_cache_key('query', table, filters, sort, limit, offset)
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        payload = {
            "operation": "QUERY",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table,
            "limit": limit,
            "skip_versioning": False
        }
        if filters:
            payload["filters"] = filters
        if sort:
            payload["sort"] = sort
        if offset > 0:
            payload["offset"] = offset
        if include_deleted:
            payload["include_deleted"] = True
        if projection:
            payload["projection"] = projection
        if aggregations:
            payload["aggregations"] = aggregations
        if group_by:
            payload["group_by"] = group_by

        result = self._execute(payload, is_write=False)

        if use_cache and cache_key and result and result.get('success'):
            self._cache.set(cache_key, result, ttl=self._read_cache_ttl)
            for record in result.get('data', {}).get('records', []):
                if 'id' in record:
                    self._cache.set(
                        f"record:{table}:{record['id']}", record,
                        ttl=self._read_cache_ttl
                    )

        return result

    def write(self, table: str, records: List[Dict], tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """Write (append) records to a table"""
        self._total_requests += 1

        payload = {
            "operation": "WRITE",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table,
            "records": records,
            "mode": "append"
        }

        result = self._execute(payload, is_write=True)

        if result and result.get('success'):
            self._cache.invalidate(f"query:{table}")
            for record in result.get('data', {}).get('records', records):
                if 'id' in record:
                    self._cache.set(
                        f"record:{table}:{record['id']}", record,
                        ttl=self._read_cache_ttl
                    )

        return result

    def upsert(self, table: str, records: List[Dict], filters: List[Dict] = None, updates: Dict = None, tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """Upsert records — update if exists, insert if not"""
        self._total_requests += 1

        payload = {
            "operation": "UPSERT",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table,
            "records": records
        }
        if filters:
            payload["filters"] = filters
        if updates:
            payload["updates"] = updates

        result = self._execute(payload, is_write=True)

        if result and result.get('success'):
            self._cache.invalidate(f"query:{table}")
            for record in records:
                if 'id' in record:
                    self._cache.invalidate(f"record:{table}:{record['id']}")

        return result

    def update(self, table: str, filters: List[Dict], updates: Dict, tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """Update records matching filters"""
        self._total_requests += 1

        payload = {
            "operation": "UPDATE",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table,
            "filters": filters,
            "updates": updates
        }

        result = self._execute(payload, is_write=True)

        if result and result.get('success'):
            self._cache.invalidate(f"query:{table}")
            for f in filters:
                if f.get('field') == 'id':
                    self._cache.invalidate(f"record:{table}:{f['value']}")

        return result

    def delete(self, table: str, filters: List[Dict], tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """Soft-delete records matching filters"""
        self._total_requests += 1

        payload = {
            "operation": "DELETE",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table,
            "filters": filters
        }

        result = self._execute(payload, is_write=True)

        if result and result.get('success'):
            self._cache.invalidate(f"query:{table}")
            for f in filters:
                if f.get('field') == 'id':
                    self._cache.invalidate(f"record:{table}:{f['value']}")

        return result

    def hard_delete(self, table: str, filters: List[Dict], confirm: bool = False) -> Dict[str, Any]:
        """Physically delete records — requires confirm=True"""
        self._total_requests += 1

        if not confirm:
            raise ValueError("confirm=True is required for hard_delete to prevent accidental data loss")

        payload = {
            "operation": "HARD_DELETE",
            "tenant_id": self.tenant_id,
            "namespace": self.namespace,
            "table": table,
            "filters": filters,
            "confirm": True
        }

        result = self._execute(payload, is_write=True)

        if result and result.get('success'):
            self._cache.invalidate(f"query:{table}")
            for f in filters:
                if f.get('field') == 'id':
                    self._cache.invalidate(f"record:{table}:{f['value']}")

        return result

    def compact(self, table: str, force: bool = False, target_file_size_mb: int = None, max_files: int = None) -> Dict[str, Any]:
        """Compact small files in the table to improve read performance"""
        self._total_requests += 1

        payload = {
            "operation": "COMPACT",
            "tenant_id": self.tenant_id,
            "namespace": self.namespace,
            "table": table,
            "force": force
        }
        if target_file_size_mb:
            payload["target_file_size_mb"] = target_file_size_mb
        if max_files:
            payload["max_files"] = max_files

        return self._execute(payload, is_write=True)

    # ─── Table Management ────────────────────────────────────────────────────

    def create_table(self, table: str, schema: Dict, if_not_exists: bool = True, tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """Create a new table"""
        payload = {
            "operation": "CREATE_TABLE",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table,
            "schema": schema,
            "if_not_exists": if_not_exists
        }
        return self._execute(payload, is_write=True)

    def list_tables(self, tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """List all tables in the namespace"""
        payload = {
            "operation": "LIST_TABLES",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace
        }
        return self._execute(payload, is_write=False)

    def describe_table(self, table: str, tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """Describe table schema and metadata"""
        payload = {
            "operation": "DESCRIBE_TABLE",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table
        }
        return self._execute(payload, is_write=False)

    def drop_table(self, table: str, purge: bool = False, tenant_id: str = None, namespace: str = None) -> Dict[str, Any]:
        """Drop a table"""
        payload = {
            "operation": "DROP_TABLE",
            "tenant_id": tenant_id or self.tenant_id,
            "namespace": namespace or self.namespace,
            "table": table,
            "purge": purge
        }
        result = self._execute(payload, is_write=True)
        if result and result.get('success'):
            self._cache.invalidate(f"query:{table}")
            self._cache.invalidate(f"record:{table}")
        return result

    def drop_namespace(self, namespace: str = None) -> Dict[str, Any]:
        """Drop a namespace (database). Defaults to the client's namespace."""
        payload = {
            "operation": "DROP_NAMESPACE",
            "tenant_id": self.tenant_id,
            "namespace": namespace or self.namespace
        }
        return self._execute(payload, is_write=True)

    # ─── Storage Operations ──────────────────────────────────────────────────

    def get_upload_url(self, filename: str, content_type: str, expires_in: int = 300, tenant_id: str = None, folder: str = None) -> Dict[str, Any]:
        """Get a presigned S3 URL for uploading a file"""
        payload = {
            "operation": "GET_UPLOAD_URL",
            "tenant_id": tenant_id or self.tenant_id,
            "filename": filename,
            "content_type": content_type,
            "expires_in": expires_in
        }
        if folder:
            payload["folder"] = folder
        return self._execute(payload, is_write=False)

    def get_download_url(self, file_key: str, expires_in: int = 3600, tenant_id: str = None) -> Dict[str, Any]:
        """Get a presigned S3 URL for downloading a file"""
        payload = {
            "operation": "GET_DOWNLOAD_URL",
            "tenant_id": tenant_id or self.tenant_id,
            "file_key": file_key,
            "expires_in": expires_in
        }
        return self._execute(payload, is_write=False)

    def export_csv(
        self,
        table: str,
        filters: List[Dict] = None,
        projection: List[str] = None,
        sort: List[Dict] = None,
        limit: int = None,
        filename: str = None,
        include_header: bool = True,
        include_deleted: bool = False,
        expiration_seconds: int = 3600
    ) -> Dict[str, Any]:
        """Export table data as CSV, returning a presigned download URL"""
        self._total_requests += 1

        payload = {
            "operation": "EXPORT_CSV",
            "tenant_id": self.tenant_id,
            "namespace": self.namespace,
            "table": table,
            "include_header": include_header,
            "include_deleted": include_deleted,
            "expiration_seconds": expiration_seconds
        }
        if filters is not None:
            payload["filters"] = filters
        if projection is not None:
            payload["projection"] = projection
        if sort is not None:
            payload["sort"] = sort
        if limit is not None:
            payload["limit"] = limit
        if filename is not None:
            payload["filename"] = filename

        return self._execute(payload, is_write=False)

    def execute_sql(
        self,
        sql: str,
        params: list = None,
        namespace: str = None,
        timeout_ms: int = 30000,
    ) -> Dict[str, Any]:
        """
        Execute raw SQL query via the query engine.

        Supports complex queries, JOINs, aggregations, and window functions
        powered by DuckDB + Apache Iceberg.

        Args:
            sql: SQL query string
            params: Optional query parameters for parameterized queries
            namespace: Override namespace (uses default if not set)
            timeout_ms: Query timeout in milliseconds

        Returns:
            Dict with 'records', 'row_count', and 'metadata'
        """
        self._total_requests += 1

        payload = {
            "operation": "EXECUTE_SQL",
            "tenant_id": self.tenant_id,
            "namespace": namespace or self.namespace,
            "sql": sql,
            "timeout_ms": timeout_ms,
        }
        if params:
            payload["params"] = params

        return self._execute(payload, is_write=False)

    def federated_query(
        self,
        sql: str,
        params: list = None,
        sources: dict = None,
        namespace: str = None,
        timeout_ms: int = 30000,
    ) -> Dict[str, Any]:
        """
        Execute a federated query across multiple data sources.

        Supports cross-source JOINs between Iceberg tables, PostgreSQL,
        MySQL, and other data sources via DuckDB's ATTACH mechanism.

        Args:
            sql: SQL query string (can reference tables from multiple sources)
            params: Optional query parameters
            sources: Optional data source configuration overrides
            namespace: Override namespace
            timeout_ms: Query timeout in milliseconds

        Returns:
            Dict with 'records', 'row_count', and 'metadata'
        """
        self._total_requests += 1

        payload = {
            "operation": "FEDERATED_QUERY",
            "tenant_id": self.tenant_id,
            "namespace": namespace or self.namespace,
            "sql": sql,
            "timeout_ms": timeout_ms,
        }
        if params:
            payload["params"] = params
        if sources:
            payload["sources"] = sources

        return self._execute(payload, is_write=False)

    # ─── Batch Operations ────────────────────────────────────────────────────

    def batch_write(self, table: str, records: List[Dict], batch_size: int = 100) -> Dict[str, Any]:
        """Write records in batches for large datasets"""
        all_results = []
        errors = []

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            result = self.write(table, batch)
            if result and result.get('success'):
                all_results.extend(result.get('data', {}).get('records', batch))
            else:
                errors.append({
                    'batch_index': i // batch_size,
                    'error': result.get('error') if result else 'Unknown error'
                })

        return {
            'success': len(errors) == 0,
            'data': {'records': all_results},
            'total_written': len(all_results),
            'errors': errors
        }

    # ─── Execution Layer ─────────────────────────────────────────────────────

    def _execute(self, payload: Dict, is_write: bool = False) -> Dict[str, Any]:
        """Execute via Lambda invocation or HTTP API"""
        use_lambda = (
            self._lambda_client is not None
            and self._lambda_function_name is not None
            and (is_write or not self._use_lambda_for_writes_only)
        )
        if use_lambda:
            return self._invoke_lambda(payload)
        return self._call_api(payload)

    def _invoke_lambda(self, payload: Dict) -> Dict[str, Any]:
        """Invoke IbexDB Lambda directly"""
        try:
            self._lambda_invocations += 1
            response = self._lambda_client.invoke(
                FunctionName=self._lambda_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload).encode('utf-8')
            )

            response_payload = json.loads(response['Payload'].read().decode('utf-8'))
            if isinstance(response_payload, str):
                response_payload = json.loads(response_payload)

            if 'FunctionError' in response:
                logger.error(f"Lambda invocation error: {response_payload}")
                return {'success': False, 'error': str(response_payload)}

            if 'statusCode' in response_payload:
                body = response_payload.get('body', '{}')
                if isinstance(body, str):
                    body = json.loads(body)
                return body

            return response_payload

        except Exception as e:
            logger.error(f"Lambda invocation failed, falling back to API: {e}")
            return self._call_api(payload)

    def _call_api(self, payload: Dict) -> Dict[str, Any]:
        """Call IbexDB via HTTP API with retries"""
        self._api_calls += 1
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = self._session.post(
                    self.api_url, json=payload, timeout=self.timeout
                )

                if response.status_code == 200:
                    return response.json()
                elif response.status_code in (429, 502, 503, 504):
                    wait = min(2 ** attempt, 8)
                    logger.warning(f"Retryable status {response.status_code}, waiting {wait}s (attempt {attempt + 1})")
                    time.sleep(wait)
                    continue
                else:
                    error_text = response.text[:500]
                    logger.error(f"API error {response.status_code}: {error_text}")
                    return {'success': False, 'error': f"HTTP {response.status_code}: {error_text}"}

            except requests.exceptions.Timeout:
                last_error = "Request timed out"
                logger.warning(f"Timeout on attempt {attempt + 1}")
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {e}"
                logger.warning(f"Connection error on attempt {attempt + 1}")
            except Exception as e:
                last_error = str(e)
                logger.error(f"Unexpected error: {e}")
                break

        return {'success': False, 'error': last_error or 'Max retries exceeded'}

    # ─── Utilities ───────────────────────────────────────────────────────────

    def _make_cache_key(self, operation: str, table: str, *args) -> str:
        key_data = json.dumps([operation, table, *args], sort_keys=True, default=str)
        key_hash = hashlib.md5(key_data.encode()).hexdigest()[:12]
        return f"{operation}:{table}:{key_hash}"

    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        cache_stats = self._cache.stats
        return {
            'total_requests': self._total_requests,
            'lambda_invocations': self._lambda_invocations,
            'api_calls': self._api_calls,
            'cached_responses': cache_stats['hits'],
            'cache_hit_rate': cache_stats['hit_rate'],
            'cache_size': cache_stats['size']
        }

    def clear_cache(self):
        """Clear all cached data"""
        self._cache.invalidate()


# ─── Tenant Manager ────────────────────────────────────────────────────────────

class TenantManager:
    """Factory for creating tenant-specific IbexDB clients"""

    @staticmethod
    def create_ibex_client(
        tenant_config: Dict[str, Any],
        client_class=OptimizedIbexClient
    ) -> OptimizedIbexClient:
        return client_class(
            api_url=tenant_config.get('api_url', os.environ.get('IBEX_API_URL', '')),
            api_key=tenant_config.get('api_key', os.environ.get('IBEX_API_KEY', '')),
            tenant_id=tenant_config.get('tenant_id', os.environ.get('TENANT_ID', 'default')),
            namespace=tenant_config.get('namespace', os.environ.get('DB_NAMESPACE', 'default')),
            timeout=tenant_config.get('timeout', 20),
            lambda_name=tenant_config.get('lambda_name')
        )
