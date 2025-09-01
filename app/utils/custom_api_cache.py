"""
自定义API缓存管理器 - 支持多层缓存策略
"""

import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List
from loguru import logger
import asyncio
import aioredis
from app.utils.cache_service import cache_service
from config.settings import settings


class CustomAPICache:
    """自定义API缓存管理器"""

    def __init__(self):
        self.memory_cache: Dict[str, Dict] = {}
        self.cache_stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "deletes": 0,
            "errors": 0
        }

    async def get_cached_result(
            self,
            cache_key: str,
            ttl: int
    ) -> Optional[Dict[str, Any]]:
        """获取缓存的查询结果"""
        try:
            # 1. 先检查内存缓存
            if cache_key in self.memory_cache:
                cache_entry = self.memory_cache[cache_key]
                if self._is_cache_valid(cache_entry, ttl):
                    self.cache_stats["hits"] += 1
                    logger.debug(f"内存缓存命中: {cache_key}")
                    return cache_entry["data"]
                else:
                    # 过期删除
                    del self.memory_cache[cache_key]

            # 2. 检查Redis缓存
            redis_key = f"custom_api:{cache_key}"
            cached_data = await cache_service.get(redis_key)

            if cached_data:
                try:
                    result_data = json.loads(cached_data)

                    # 更新到内存缓存
                    self.memory_cache[cache_key] = {
                        "data": result_data,
                        "cached_at": datetime.now(),
                        "access_count": 1
                    }

                    self.cache_stats["hits"] += 1
                    logger.debug(f"Redis缓存命中: {cache_key}")
                    return result_data

                except json.JSONDecodeError:
                    logger.warning(f"Redis缓存数据格式错误: {cache_key}")
                    await cache_service.delete(redis_key)

            # 3. 缓存未命中
            self.cache_stats["misses"] += 1
            return None

        except Exception as e:
            self.cache_stats["errors"] += 1
            logger.error(f"获取缓存失败 {cache_key}: {e}")
            return None

    async def cache_result(
            self,
            cache_key: str,
            result_data: Dict[str, Any],
            ttl: int
    ) -> bool:
        """缓存查询结果"""
        try:
            # 1. 缓存到内存（短期）
            self.memory_cache[cache_key] = {
                "data": result_data,
                "cached_at": datetime.now(),
                "access_count": 0
            }

            # 清理过期的内存缓存
            await self._cleanup_memory_cache()

            # 2. 缓存到Redis（长期）
            if ttl > 60:  # 只有TTL大于1分钟才缓存到Redis
                redis_key = f"custom_api:{cache_key}"
                serialized_data = json.dumps(result_data, default=str, ensure_ascii=False)

                success = await cache_service.set(
                    key=redis_key,
                    value=serialized_data,
                    expire=ttl
                )

                if success:
                    logger.debug(f"结果已缓存到Redis: {cache_key} (TTL: {ttl}s)")

            self.cache_stats["sets"] += 1
            return True

        except Exception as e:
            self.cache_stats["errors"] += 1
            logger.error(f"缓存结果失败 {cache_key}: {e}")
            return False

    async def clear_api_cache(self, api_name: str) -> int:
        """清理指定API的所有缓存"""
        try:
            cleared_count = 0

            # 1. 清理内存缓存
            keys_to_remove = []
            for key in self.memory_cache.keys():
                if api_name in key:
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del self.memory_cache[key]
                cleared_count += 1

            # 2. 清理Redis缓存
            pattern = f"custom_api:*api_{api_name}*"
            redis_keys = await cache_service.get_keys_by_pattern(pattern)

            for key in redis_keys:
                await cache_service.delete(key)
                cleared_count += 1

            logger.info(f"清理API缓存: {api_name}, 共清理 {cleared_count} 个缓存项")
            self.cache_stats["deletes"] += cleared_count

            return cleared_count

        except Exception as e:
            logger.error(f"清理API缓存失败 {api_name}: {e}")
            return 0

    async def clear_all_cache(self) -> Dict[str, int]:
        """清理所有自定义API缓存"""
        try:
            # 清理内存缓存
            memory_count = len(self.memory_cache)
            self.memory_cache.clear()

            # 清理Redis缓存
            pattern = "custom_api:*"
            redis_keys = await cache_service.get_keys_by_pattern(pattern)
            redis_count = 0

            for key in redis_keys:
                await cache_service.delete(key)
                redis_count += 1

            result = {
                "memory_cleared": memory_count,
                "redis_cleared": redis_count,
                "total_cleared": memory_count + redis_count
            }

            logger.info(f"清理所有API缓存: {result}")
            self.cache_stats["deletes"] += result["total_cleared"]

            return result

        except Exception as e:
            logger.error(f"清理所有缓存失败: {e}")
            return {"memory_cleared": 0, "redis_cleared": 0, "total_cleared": 0}

    def _is_cache_valid(self, cache_entry: Dict, ttl: int) -> bool:
        """检查缓存是否有效"""
        cached_at = cache_entry.get("cached_at")
        if not cached_at:
            return False

        elapsed = (datetime.now() - cached_at).total_seconds()
        return elapsed < ttl

    async def _cleanup_memory_cache(self):
        """清理过期的内存缓存"""
        try:
            # 限制内存缓存数量，最多保留1000个
            if len(self.memory_cache) > 1000:
                # 按访问时间排序，删除最老的
                sorted_items = sorted(
                    self.memory_cache.items(),
                    key=lambda x: x[1].get("cached_at", datetime.min)
                )

                # 删除最老的200个
                for key, _ in sorted_items[:200]:
                    del self.memory_cache[key]

                logger.debug(f"清理过期内存缓存，剩余: {len(self.memory_cache)} 个")

        except Exception as e:
            logger.error(f"清理内存缓存失败: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_requests = self.cache_stats["hits"] + self.cache_stats["misses"]
        hit_rate = (self.cache_stats["hits"] / total_requests * 100) if total_requests > 0 else 0

        return {
            "statistics": self.cache_stats,
            "hit_rate": round(hit_rate, 2),
            "memory_cache_size": len(self.memory_cache),
            "last_updated": datetime.now()
        }


# 创建全局缓存管理器实例
custom_api_cache = CustomAPICache()