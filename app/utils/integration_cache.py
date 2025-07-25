# app/utils/integration_cache.py
"""
数据集成模块的多层缓存架构
支持高并发访问的缓存策略
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from functools import wraps
import hashlib
from app.utils.cache_service import cache_service
from loguru import logger


class IntegrationCacheManager:
    """数据集成专用缓存管理器 - 多层缓存架构"""

    def __init__(self):
        # 内存缓存 - 最快访问
        self.memory_cache: Dict[str, Dict] = {}
        self.memory_cache_ttl = 60  # 1分钟

        # Redis缓存 - 中等速度，跨进程共享
        self.redis_cache_ttl = 300  # 5分钟

        # 数据库缓存 - 持久化，最长TTL
        self.db_cache_ttl = 3600  # 1小时

        # 缓存键前缀
        self.prefix = "integration"

        # 并发控制
        self.collecting_locks: Dict[str, asyncio.Lock] = {}

        # 缓存统计
        self.stats = {
            'memory_hits': 0,
            'redis_hits': 0,
            'cache_misses': 0,
            'total_requests': 0
        }

    async def get_cached_data(self,
                              cache_key: str,
                              data_fetcher: Callable,
                              ttl_config: Dict[str, int] = None) -> Any:
        """
        多层缓存获取数据
        1. 内存缓存 (最快)
        2. Redis缓存 (中等)
        3. 执行数据获取函数 (最慢)
        """
        self.stats['total_requests'] += 1

        # 1. 检查内存缓存
        if self._is_memory_cache_valid(cache_key):
            self.stats['memory_hits'] += 1
            logger.debug(f"Memory cache hit: {cache_key}")
            return self.memory_cache[cache_key]['data']

        # 2. 检查Redis缓存
        redis_key = f"{self.prefix}:{cache_key}"
        cached_data = await cache_service.get(redis_key)
        if cached_data:
            self.stats['redis_hits'] += 1
            logger.debug(f"Redis cache hit: {cache_key}")

            # 更新内存缓存
            self._set_memory_cache(cache_key, cached_data)
            return cached_data

        # 3. 缓存未命中，使用锁防止缓存击穿
        if cache_key not in self.collecting_locks:
            self.collecting_locks[cache_key] = asyncio.Lock()

        async with self.collecting_locks[cache_key]:
            # 双重检查 - 可能其他协程已经获取了数据
            cached_data = await cache_service.get(redis_key)
            if cached_data:
                self._set_memory_cache(cache_key, cached_data)
                return cached_data

            # 执行数据获取
            self.stats['cache_misses'] += 1
            logger.debug(f"Cache miss, fetching data: {cache_key}")

            try:
                data = await data_fetcher()

                # 设置多层缓存
                await self._set_all_cache_layers(cache_key, data, ttl_config)

                return data

            except Exception as e:
                logger.error(f"Data fetching failed for {cache_key}: {e}")
                raise

    def _is_memory_cache_valid(self, cache_key: str) -> bool:
        """检查内存缓存是否有效"""
        if cache_key not in self.memory_cache:
            return False

        cache_time = self.memory_cache[cache_key]['timestamp']
        return (time.time() - cache_time) < self.memory_cache_ttl

    def _set_memory_cache(self, cache_key: str, data: Any) -> None:
        """设置内存缓存"""
        self.memory_cache[cache_key] = {
            'data': data,
            'timestamp': time.time()
        }

    async def _set_all_cache_layers(self,
                                    cache_key: str,
                                    data: Any,
                                    ttl_config: Dict[str, int] = None) -> None:
        """设置所有缓存层"""
        # 设置内存缓存
        self._set_memory_cache(cache_key, data)

        # 设置Redis缓存
        redis_key = f"{self.prefix}:{cache_key}"
        redis_ttl = ttl_config.get('redis', self.redis_cache_ttl) if ttl_config else self.redis_cache_ttl
        await cache_service.set(redis_key, data, redis_ttl)

    async def invalidate_cache(self, pattern: str = None) -> None:
        """清除缓存"""
        if pattern:
            # 清除匹配模式的缓存
            keys_to_remove = [k for k in self.memory_cache.keys() if pattern in k]
            for key in keys_to_remove:
                del self.memory_cache[key]

            # 清除Redis缓存
            await cache_service.delete_pattern(f"{self.prefix}:*{pattern}*")
        else:
            # 清除所有缓存
            self.memory_cache.clear()
            await cache_service.delete_pattern(f"{self.prefix}:*")

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total = self.stats['total_requests']
        if total == 0:
            return self.stats

        return {
            **self.stats,
            'memory_hit_rate': (self.stats['memory_hits'] / total) * 100,
            'redis_hit_rate': (self.stats['redis_hits'] / total) * 100,
            'cache_miss_rate': (self.stats['cache_misses'] / total) * 100,
            'memory_cache_size': len(self.memory_cache)
        }


# 全局实例
integration_cache = IntegrationCacheManager()


# 装饰器：自动缓存数据源相关操作
def cache_data_source_operation(cache_type: str, ttl: int = 300):
    """缓存数据源操作的装饰器"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 生成缓存键
            func_name = func.__name__
            args_str = "_".join(str(arg) for arg in args if isinstance(arg, (str, int)))
            kwargs_str = "_".join(f"{k}_{v}" for k, v in kwargs.items() if isinstance(v, (str, int)))

            cache_key = f"{cache_type}_{func_name}_{args_str}_{kwargs_str}"
            cache_key = hashlib.md5(cache_key.encode()).hexdigest()[:16]

            # 使用缓存管理器
            async def data_fetcher():
                return await func(*args, **kwargs)

            return await integration_cache.get_cached_data(
                cache_key,
                data_fetcher,
                {'redis': ttl}
            )

        return wrapper

    return decorator


# 连接状态缓存装饰器
def cache_connection_status(ttl: int = 60):
    """缓存连接状态的装饰器"""
    return cache_data_source_operation("connection", ttl)


# 表结构缓存装饰器
def cache_table_schema(ttl: int = 1800):  # 30分钟
    """缓存表结构的装饰器"""
    return cache_data_source_operation("schema", ttl)


# 数据预览缓存装饰器
def cache_data_preview(ttl: int = 300):  # 5分钟
    """缓存数据预览的装饰器"""
    return cache_data_source_operation("preview", ttl)