# app/utils/cache_service.py
"""
Enhanced Redis cache service with intelligent caching strategies
and performance optimizations for the Big Data Platform.
"""

import asyncio
import json
import pickle
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
from functools import wraps
import hashlib
import redis.asyncio as redis
from loguru import logger
from config.settings import settings


class AsyncRedisCache:
    """
    Asynchronous Redis cache service with intelligent caching strategies.
    """

    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.is_connected = False
        self.connection_pool = None

        # Cache configuration
        self.default_ttl = 300  # 5 minutes
        self.max_retries = 3
        self.retry_delay = 1.0

        # Cache key prefixes for different data types
        self.prefixes = {
            'overview': 'bigdata:overview',
            'cluster': 'bigdata:cluster',
            'metrics': 'bigdata:metrics',
            'tasks': 'bigdata:tasks',
            'health': 'bigdata:health',
            'storage': 'bigdata:storage'
        }

        # TTL configurations for different data types
        self.ttl_config = {
            'overview_full': 60,  # Full overview data - 1 minute
            'overview_stats': 120,  # Statistics - 2 minutes
            'cluster_info': 30,  # Cluster information - 30 seconds
            'cluster_metrics': 15,  # Real-time metrics - 15 seconds
            'task_list': 60,  # Task list - 1 minute
            'system_health': 45,  # System health - 45 seconds
            'storage_info': 300,  # Storage info - 5 minutes
            'database_stats': 1800,  # Database statistics - 30 minutes
            'business_systems': 3600,  # Business systems - 1 hour
        }

    async def initialize(self):
        """Initialize Redis connection with connection pooling."""
        try:
            # Parse Redis URL
            redis_url = settings.REDIS_URL
            logger.info(f"Initializing Redis connection: {redis_url}")

            # Create connection pool
            self.connection_pool = redis.ConnectionPool.from_url(
                redis_url,
                max_connections=20,
                retry_on_timeout=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                health_check_interval=30,
                encoding='utf-8',
                decode_responses=False  # We handle encoding ourselves
            )

            # Create Redis client
            self.redis_client = redis.Redis(
                connection_pool=self.connection_pool,
                socket_connect_timeout=5,
                socket_timeout=5
            )

            # Test connection
            await self.redis_client.ping()
            self.is_connected = True

            logger.info("✅ Redis cache service initialized successfully")

        except Exception as e:
            logger.error(f"❌ Redis initialization failed: {e}")
            self.is_connected = False
            self.redis_client = None

    async def health_check(self) -> bool:
        """Check if Redis is healthy and responsive."""
        if not self.redis_client:
            return False

        try:
            result = await self.redis_client.ping()
            self.is_connected = result
            return result
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            self.is_connected = False
            return False

    def _generate_cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate a unique cache key based on prefix and parameters."""
        # Create a hash of all parameters for consistent key generation
        key_components = [str(arg) for arg in args]
        key_components.extend([f"{k}:{v}" for k, v in sorted(kwargs.items())])

        if key_components:
            # Create hash for long or complex keys
            key_suffix = hashlib.md5(
                ":".join(key_components).encode('utf-8')
            ).hexdigest()[:12]
            return f"{prefix}:{key_suffix}"
        else:
            return prefix

    def _serialize_data(self, data: Any) -> bytes:
        """Serialize data for Redis storage."""
        try:
            # Try JSON first for simple data types
            if isinstance(data, (dict, list, str, int, float, bool, type(None))):
                return json.dumps(data, default=str, ensure_ascii=False).encode('utf-8')
            else:
                # Use pickle for complex objects
                return pickle.dumps(data)
        except Exception as e:
            logger.warning(f"Serialization failed, using pickle: {e}")
            return pickle.dumps(data)

    def _deserialize_data(self, data: bytes) -> Any:
        """Deserialize data from Redis."""
        try:
            # Try JSON first
            return json.loads(data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            try:
                # Fallback to pickle
                return pickle.loads(data)
            except Exception as e:
                logger.error(f"Failed to deserialize data: {e}")
                return None

    async def get(self, key: str, default: Any = None) -> Any:
        """Get data from cache with automatic deserialization."""
        if not await self.health_check():
            return default

        try:
            data = await self.redis_client.get(key)
            if data is None:
                return default

            return self._deserialize_data(data)

        except Exception as e:
            logger.warning(f"Cache get failed for key {key}: {e}")
            return default

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set data in cache with automatic serialization."""
        if not await self.health_check():
            return False

        try:
            serialized_data = self._serialize_data(value)
            ttl = ttl or self.default_ttl

            await self.redis_client.setex(key, ttl, serialized_data)
            return True

        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete a single key from cache."""
        if not await self.health_check():
            return False

        try:
            result = await self.redis_client.delete(key)
            return result > 0
        except Exception as e:
            logger.warning(f"Cache delete failed for key {key}: {e}")
            return False

    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching a pattern."""
        if not await self.health_check():
            return 0

        try:
            keys = await self.redis_client.keys(pattern)
            if keys:
                result = await self.redis_client.delete(*keys)
                logger.info(f"Deleted {result} cache keys matching pattern: {pattern}")
                return result
            return 0
        except Exception as e:
            logger.warning(f"Cache pattern delete failed for pattern {pattern}: {e}")
            return 0

    async def exists(self, key: str) -> bool:
        """Check if a key exists in cache."""
        if not await self.health_check():
            return False

        try:
            result = await self.redis_client.exists(key)
            return result > 0
        except Exception as e:
            logger.warning(f"Cache exists check failed for key {key}: {e}")
            return False

    async def get_ttl(self, key: str) -> int:
        """Get TTL (time to live) for a key."""
        if not await self.health_check():
            return -1

        try:
            return await self.redis_client.ttl(key)
        except Exception as e:
            logger.warning(f"Cache TTL check failed for key {key}: {e}")
            return -1

    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """Increment a numeric value in cache."""
        if not await self.health_check():
            return None

        try:
            return await self.redis_client.incr(key, amount)
        except Exception as e:
            logger.warning(f"Cache increment failed for key {key}: {e}")
            return None

    async def get_cache_info(self) -> Dict[str, Any]:
        """Get cache statistics and information."""
        if not await self.health_check():
            return {"status": "disconnected", "error": "Redis not available"}

        try:
            info = await self.redis_client.info()
            memory_info = await self.redis_client.info('memory')

            return {
                "status": "connected",
                "redis_version": info.get('redis_version', 'unknown'),
                "used_memory": memory_info.get('used_memory_human', 'unknown'),
                "connected_clients": info.get('connected_clients', 0),
                "total_commands_processed": info.get('total_commands_processed', 0),
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "uptime_in_seconds": info.get('uptime_in_seconds', 0),
            }
        except Exception as e:
            logger.warning(f"Failed to get cache info: {e}")
            return {"status": "error", "error": str(e)}

    # High-level caching methods for specific data types

    async def cache_overview_data(self, data: Any, cache_type: str = 'overview_full') -> bool:
        """Cache overview data with appropriate TTL."""
        key = self._generate_cache_key(self.prefixes['overview'], cache_type)
        ttl = self.ttl_config.get(cache_type, self.default_ttl)
        return await self.set(key, data, ttl)

    async def get_overview_data(self, cache_type: str = 'overview_full') -> Any:
        """Get cached overview data."""
        key = self._generate_cache_key(self.prefixes['overview'], cache_type)
        return await self.get(key)

    async def cache_cluster_metrics(self, cluster_type: str, data: Any) -> bool:
        """Cache cluster metrics data."""
        key = self._generate_cache_key(self.prefixes['cluster'], cluster_type, 'metrics')
        ttl = self.ttl_config['cluster_metrics']
        return await self.set(key, data, ttl)

    async def get_cluster_metrics(self, cluster_type: str) -> Any:
        """Get cached cluster metrics."""
        key = self._generate_cache_key(self.prefixes['cluster'], cluster_type, 'metrics')
        return await self.get(key)

    async def cache_task_data(self, task_type: str, data: Any) -> bool:
        """Cache task-related data."""
        key = self._generate_cache_key(self.prefixes['tasks'], task_type)
        ttl = self.ttl_config['task_list']
        return await self.set(key, data, ttl)

    async def get_task_data(self, task_type: str) -> Any:
        """Get cached task data."""
        key = self._generate_cache_key(self.prefixes['tasks'], task_type)
        return await self.get(key)

    async def invalidate_overview_cache(self) -> int:
        """Invalidate all overview-related cache."""
        pattern = f"{self.prefixes['overview']}:*"
        return await self.delete_pattern(pattern)

    async def invalidate_cluster_cache(self, cluster_type: str = None) -> int:
        """Invalidate cluster cache - all clusters or specific cluster."""
        if cluster_type:
            pattern = f"{self.prefixes['cluster']}:*{cluster_type}*"
        else:
            pattern = f"{self.prefixes['cluster']}:*"
        return await self.delete_pattern(pattern)

    async def invalidate_all_cache(self) -> bool:
        """Invalidate entire cache (use with caution)."""
        if not await self.health_check():
            return False

        try:
            await self.redis_client.flushdb()
            logger.info("All cache invalidated")
            return True
        except Exception as e:
            logger.error(f"Failed to invalidate all cache: {e}")
            return False


def cache_with_key(
        cache_type: str,
        ttl: Optional[int] = None,
        key_func: Optional[Callable] = None
):
    """
    Decorator for caching function results with custom key generation.

    Args:
        cache_type: Type of cache (used for TTL configuration)
        ttl: Custom TTL in seconds (overrides default)
        key_func: Custom function to generate cache key
    """

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                func_name = f"{func.__module__}.{func.__name__}"
                cache_key = cache_service._generate_cache_key(
                    f"func:{func_name}", *args, **kwargs
                )

            # Try to get from cache
            cached_result = await cache_service.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit: {cache_key}")
                return cached_result

            # Execute function
            result = await func(*args, **kwargs)

            # Cache result
            cache_ttl = ttl or cache_service.ttl_config.get(cache_type, cache_service.default_ttl)
            await cache_service.set(cache_key, result, cache_ttl)

            logger.debug(f"Cache set: {cache_key} (TTL: {cache_ttl}s)")
            return result

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # For sync functions, run in event loop
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, we can't use this decorator
                # Return the original function
                return func(*args, **kwargs)
            else:
                return loop.run_until_complete(async_wrapper(*args, **kwargs))

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


# Global cache service instance
cache_service = AsyncRedisCache()


async def initialize_cache():
    """Initialize the global cache service."""
    await cache_service.initialize()
    return cache_service


# Convenience functions for backward compatibility
async def get_cache(key: str, default: Any = None) -> Any:
    """Get data from cache."""
    return await cache_service.get(key, default)


async def set_cache(key: str, value: Any, ttl: Optional[int] = None) -> bool:
    """Set data in cache."""
    return await cache_service.set(key, value, ttl)


async def delete_cache(key: str) -> bool:
    """Delete data from cache."""
    return await cache_service.delete(key)


async def clear_cache_pattern(pattern: str) -> int:
    """Clear cache by pattern."""
    return await cache_service.delete_pattern(pattern)