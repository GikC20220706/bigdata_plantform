"""
自定义API频率限制器 - 基于滑动窗口算法
"""

import time
from collections import defaultdict, deque
from typing import Dict, Tuple
from loguru import logger
import asyncio


class CustomAPIRateLimiter:
    """自定义API频率限制器"""

    def __init__(self):
        # 存储格式: {(api_id, client_ip): deque(timestamps)}
        self.request_windows: Dict[Tuple[int, str], deque] = defaultdict(deque)
        self.blocked_clients: Dict[Tuple[int, str], float] = {}  # 临时封禁

    async def check_rate_limit(
            self,
            api_id: int,
            client_ip: str,
            limit: int,
            window_seconds: int = 60
    ) -> bool:
        """
        检查是否超过频率限制

        Args:
            api_id: API ID
            client_ip: 客户端IP
            limit: 频率限制（次数/分钟）
            window_seconds: 时间窗口（秒）

        Returns:
            bool: True表示允许请求，False表示被限流
        """
        try:
            current_time = time.time()
            key = (api_id, client_ip)

            # 1. 检查是否在临时封禁列表中
            if key in self.blocked_clients:
                if current_time < self.blocked_clients[key]:
                    logger.warning(f"客户端仍在封禁中: API {api_id}, IP {client_ip}")
                    return False
                else:
                    # 解除封禁
                    del self.blocked_clients[key]
                    logger.info(f"解除客户端封禁: API {api_id}, IP {client_ip}")

            # 2. 获取请求时间窗口
            request_window = self.request_windows[key]

            # 3. 清理过期的请求记录
            cutoff_time = current_time - window_seconds
            while request_window and request_window[0] < cutoff_time:
                request_window.popleft()

            # 4. 检查当前窗口内的请求数量
            if len(request_window) >= limit:
                # 超过限制，检查是否需要临时封禁
                if len(request_window) >= limit * 1.5:  # 超过限制50%时临时封禁
                    self.blocked_clients[key] = current_time + 300  # 封禁5分钟
                    logger.warning(f"临时封禁客户端5分钟: API {api_id}, IP {client_ip}")

                logger.warning(
                    f"频率限制触发: API {api_id}, IP {client_ip}, "
                    f"当前请求数: {len(request_window)}, 限制: {limit}"
                )
                return False

            # 5. 记录当前请求
            request_window.append(current_time)

            # 6. 定期清理old数据（异步）
            if len(self.request_windows) > 10000:  # 当存储的客户端过多时清理
                asyncio.create_task(self._cleanup_old_records())

            return True

        except Exception as e:
            logger.error(f"频率限制检查失败: {e}")
            return True  # 出错时允许请求，避免影响正常服务

    async def _cleanup_old_records(self):
        """清理过期的请求记录"""
        try:
            current_time = time.time()
            cutoff_time = current_time - 3600  # 清理1小时前的记录

            keys_to_remove = []

            # 清理请求窗口
            for key, window in self.request_windows.items():
                while window and window[0] < cutoff_time:
                    window.popleft()

                # 如果窗口为空，标记删除
                if not window:
                    keys_to_remove.append(key)

            # 删除空窗口
            for key in keys_to_remove:
                del self.request_windows[key]

            # 清理过期封禁
            expired_blocks = [
                key for key, unblock_time in self.blocked_clients.items()
                if current_time >= unblock_time
            ]

            for key in expired_blocks:
                del self.blocked_clients[key]

            logger.debug(
                f"清理频率限制记录: 删除{len(keys_to_remove)}个窗口, "
                f"解除{len(expired_blocks)}个封禁"
            )

        except Exception as e:
            logger.error(f"清理频率限制记录失败: {e}")

    def get_rate_limit_stats(self) -> Dict:
        """获取频率限制统计信息"""
        current_time = time.time()

        active_clients = len(self.request_windows)
        blocked_clients = len(self.blocked_clients)

        # 统计活跃请求数
        total_requests = sum(len(window) for window in self.request_windows.values())

        return {
            "active_clients": active_clients,
            "blocked_clients": blocked_clients,
            "total_recent_requests": total_requests,
            "memory_usage_kb": (
                                       len(str(self.request_windows)) + len(str(self.blocked_clients))
                               ) / 1024,
            "last_updated": current_time
        }

    async def reset_client_limit(self, api_id: int, client_ip: str) -> bool:
        """重置指定客户端的频率限制"""
        try:
            key = (api_id, client_ip)

            # 清理请求窗口
            if key in self.request_windows:
                del self.request_windows[key]

            # 解除封禁
            if key in self.blocked_clients:
                del self.blocked_clients[key]

            logger.info(f"重置客户端频率限制: API {api_id}, IP {client_ip}")
            return True

        except Exception as e:
            logger.error(f"重置客户端频率限制失败: {e}")
            return False


# 创建全局频率限制器实例
rate_limiter = CustomAPIRateLimiter()