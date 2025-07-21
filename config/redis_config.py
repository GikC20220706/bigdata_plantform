# config/redis_config.py
import redis
import json
import pickle
from typing import Any, Optional, Union
from functools import wraps
import asyncio
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RedisCache:
    def __init__(self, host='localhost', port=6379, db=0, decode_responses=False):
        """
        Redis缓存配置

        Args:
            host: Redis服务器地址
            port: Redis端口
            db: Redis数据库编号
            decode_responses: 是否自动解码响应
        """
        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            # 测试连接
            self.redis_client.ping()
            logger.info(f"Redis连接成功: {host}:{port}")
        except Exception as e:
            logger.error(f"Redis连接失败: {e}")
            self.redis_client = None

    def is_available(self) -> bool:
        """检查Redis是否可用"""
        if not self.redis_client:
            return False
        try:
            self.redis_client.ping()
            return True
        except:
            return False

    def set(self, key: str, value: Any, expire: int = 300) -> bool:
        """
        设置缓存

        Args:
            key: 缓存键
            value: 缓存值
            expire: 过期时间(秒)
        """
        if not self.is_available():
            return False

        try:
            # 序列化数据
            if isinstance(value, (dict, list)):
                serialized_value = json.dumps(value, ensure_ascii=False, default=str)
            else:
                serialized_value = pickle.dumps(value)

            self.redis_client.setex(key, expire, serialized_value)
            return True
        except Exception as e:
            logger.error(f"设置缓存失败 {key}: {e}")
            return False

    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if not self.is_available():
            return None

        try:
            value = self.redis_client.get(key)
            if value is None:
                return None

            # 尝试JSON反序列化
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # 如果JSON失败，尝试pickle
                try:
                    return pickle.loads(value)
                except:
                    return value
        except Exception as e:
            logger.error(f"获取缓存失败 {key}: {e}")
            return None

    def delete(self, key: str) -> bool:
        """删除缓存"""
        if not self.is_available():
            return False

        try:
            self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error(f"删除缓存失败 {key}: {e}")
            return False

    def clear_pattern(self, pattern: str) -> int:
        """根据模式清除缓存"""
        if not self.is_available():
            return 0

        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                return self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"清除缓存模式失败 {pattern}: {e}")
            return 0


# 全局缓存实例
cache = None


def init_cache(host='localhost', port=6379, db=0):
    """初始化缓存"""
    global cache
    cache = RedisCache(host=host, port=port, db=db)
    return cache


def cache_key(*args, **kwargs) -> str:
    """生成缓存键"""
    key_parts = []
    key_parts.extend(str(arg) for arg in args)
    key_parts.extend(f"{k}:{v}" for k, v in sorted(kwargs.items()))
    return ":".join(key_parts)


def cached(expire: int = 300, key_prefix: str = ""):
    """
    缓存装饰器

    Args:
        expire: 过期时间(秒)
        key_prefix: 缓存键前缀
    """

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not cache or not cache.is_available():
                return await func(*args, **kwargs)

            # 生成缓存键
            func_name = f"{func.__module__}.{func.__name__}"
            cache_key_str = cache_key(key_prefix, func_name, *args, **kwargs)

            # 尝试从缓存获取
            cached_result = cache.get(cache_key_str)
            if cached_result is not None:
                logger.debug(f"缓存命中: {cache_key_str}")
                return cached_result

            # 执行函数并缓存结果
            result = await func(*args, **kwargs)
            cache.set(cache_key_str, result, expire)
            logger.debug(f"缓存设置: {cache_key_str}")
            return result

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not cache or not cache.is_available():
                return func(*args, **kwargs)

            # 生成缓存键
            func_name = f"{func.__module__}.{func.__name__}"
            cache_key_str = cache_key(key_prefix, func_name, *args, **kwargs)

            # 尝试从缓存获取
            cached_result = cache.get(cache_key_str)
            if cached_result is not None:
                logger.debug(f"缓存命中: {cache_key_str}")
                return cached_result

            # 执行函数并缓存结果
            result = func(*args, **kwargs)
            cache.set(cache_key_str, result, expire)
            logger.debug(f"缓存设置: {cache_key_str}")
            return result

        # 根据函数类型返回对应的包装器
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


# 缓存失效管理
class CacheManager:
    def __init__(self, cache_instance: RedisCache):
        self.cache = cache_instance

    def invalidate_overview_cache(self):
        """清除数据总览相关缓存"""
        patterns = [
            "overview:*",
            "cluster:stats:*",
            "tasks:today:*",
            "system:status:*"
        ]
        for pattern in patterns:
            self.cache.clear_pattern(pattern)
        logger.info("数据总览缓存已清除")

    def invalidate_cluster_cache(self):
        """清除集群监控相关缓存"""
        patterns = [
            "cluster:*",
            "nodes:*",
            "metrics:*"
        ]
        for pattern in patterns:
            self.cache.clear_pattern(pattern)
        logger.info("集群监控缓存已清除")

    def invalidate_all_cache(self):
        """清除所有缓存"""
        if self.cache.is_available():
            try:
                self.cache.redis_client.flushdb()
                logger.info("所有缓存已清除")
            except Exception as e:
                logger.error(f"清除所有缓存失败: {e}")


# 全局缓存管理器
cache_manager = None


def init_cache_manager():
    """初始化缓存管理器"""
    global cache_manager
    if cache:
        cache_manager = CacheManager(cache)
    return cache_manager