# app/utils/file_handler.py
"""
文件处理工具类
提供文件上传、验证、存储、删除等功能
"""

import os
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from fastapi import UploadFile, HTTPException
from loguru import logger

from config.settings import settings


class FileHandler:
    """文件处理工具类"""

    # 资源类型与允许的文件扩展名映射
    ALLOWED_EXTENSIONS: Dict[str, List[str]] = {
        "JOB": [".sh", ".py", ".sql", ".bash"],  # 作业脚本
        "FUNC": [".jar", ".zip"],  # 函数JAR包
        "LIB": [".jar", ".zip", ".tar.gz", ".whl"],  # 依赖库
        "EXCEL": [".xlsx", ".xls", ".csv"]  # Excel文件
    }

    # 资源类型与最大文件大小映射（MB）
    MAX_FILE_SIZE: Dict[str, int] = {
        "JOB": 10,  # 脚本文件最大10MB
        "FUNC": 100,  # 函数JAR包最大100MB
        "LIB": 500,  # 依赖库最大200MB
        "EXCEL": 50  # Excel文件最大50MB
    }

    # 资源类型与存储子目录映射
    STORAGE_DIRS: Dict[str, str] = {
        "JOB": "scripts",
        "FUNC": "jars/functions",
        "LIB": "jars/libs",
        "EXCEL": "excel"
    }

    def __init__(self, base_upload_dir: str = None):
        """
        初始化文件处理器

        Args:
            base_upload_dir: 基础上传目录，默认从配置读取
        """
        self.base_upload_dir = base_upload_dir or settings.UPLOAD_DIR
        self.resource_base_dir = os.path.join(self.base_upload_dir, "resources")
        self._ensure_directories()

    def _ensure_directories(self):
        """确保所有必要的目录存在"""
        try:
            # 创建基础目录
            Path(self.resource_base_dir).mkdir(parents=True, exist_ok=True)

            # 创建各资源类型的子目录
            for subdir in self.STORAGE_DIRS.values():
                dir_path = os.path.join(self.resource_base_dir, subdir)
                Path(dir_path).mkdir(parents=True, exist_ok=True)
                logger.debug(f"确保目录存在: {dir_path}")

            logger.info(f"资源存储目录初始化完成: {self.resource_base_dir}")
        except Exception as e:
            logger.error(f"创建资源目录失败: {e}")
            raise

    def validate_file(
            self,
            file: UploadFile,
            resource_type: str
    ) -> Tuple[bool, Optional[str]]:
        """
        验证文件是否符合要求

        Args:
            file: 上传的文件对象
            resource_type: 资源类型

        Returns:
            (是否通过验证, 错误消息)
        """
        # 1. 检查资源类型是否有效
        if resource_type not in self.ALLOWED_EXTENSIONS:
            return False, f"不支持的资源类型: {resource_type}"

        # 2. 检查文件名
        if not file.filename:
            return False, "文件名不能为空"

        # 3. 检查文件扩展名
        file_ext = os.path.splitext(file.filename)[1].lower()
        allowed_exts = self.ALLOWED_EXTENSIONS[resource_type]

        if file_ext not in allowed_exts:
            return False, f"资源类型 {resource_type} 不支持 {file_ext} 格式，允许的格式: {', '.join(allowed_exts)}"

        # 4. 检查文件大小（如果能获取到）
        if hasattr(file, 'size') and file.size:
            max_size_bytes = self.MAX_FILE_SIZE[resource_type] * 1024 * 1024
            if file.size > max_size_bytes:
                max_size_mb = self.MAX_FILE_SIZE[resource_type]
                actual_size_mb = file.size / (1024 * 1024)
                return False, f"文件大小 {actual_size_mb:.2f}MB 超过限制 {max_size_mb}MB"

        # 5. 检查文件名是否包含非法字符
        illegal_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
        for char in illegal_chars:
            if char in file.filename:
                return False, f"文件名包含非法字符: {char}"

        return True, None

    def generate_unique_filename(
            self,
            original_filename: str,
            resource_type: str
    ) -> str:
        """
        生成唯一的文件名

        格式: 原文件名_时间戳.扩展名
        例如: data_etl_20240101_123045.sh

        Args:
            original_filename: 原始文件名
            resource_type: 资源类型

        Returns:
            唯一的文件名
        """
        # 获取文件名和扩展名
        name_without_ext, ext = os.path.splitext(original_filename)

        # 生成时间戳
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 清理文件名（移除特殊字符）
        clean_name = "".join(c for c in name_without_ext if c.isalnum() or c in ('_', '-'))

        # 生成唯一文件名
        unique_filename = f"{clean_name}_{timestamp}{ext}"

        return unique_filename

    def get_storage_path(
            self,
            filename: str,
            resource_type: str
    ) -> str:
        """
        获取文件的完整存储路径

        Args:
            filename: 文件名
            resource_type: 资源类型

        Returns:
            完整的文件存储路径
        """
        # 🆕 添加：防止路径遍历攻击
        # 1. 移除文件名中的路径分隔符
        safe_filename = os.path.basename(filename)

        # 2. 再次检查是否包含 .. 等危险字符
        if ".." in safe_filename or "/" in safe_filename or "\\" in safe_filename:
            raise ValueError(f"非法的文件名: {filename}")

        # 3. 构建路径
        subdir = self.STORAGE_DIRS[resource_type]
        file_path = os.path.join(self.resource_base_dir, subdir, safe_filename)

        # 🆕 添加：验证最终路径在允许的目录内
        real_base_dir = os.path.realpath(self.resource_base_dir)
        real_file_path = os.path.realpath(file_path)

        if not real_file_path.startswith(real_base_dir):
            raise ValueError(f"路径遍历攻击检测: {filename}")

        return file_path

    async def save_upload_file(
            self,
            upload_file: UploadFile,
            resource_type: str
    ) -> Dict[str, any]:
        """
        保存上传的文件

        Args:
            upload_file: FastAPI的UploadFile对象
            resource_type: 资源类型

        Returns:
            包含文件信息的字典
            {
                "success": True/False,
                "file_name": "唯一文件名",
                "original_filename": "原始文件名",
                "file_path": "完整存储路径",
                "file_size": 文件大小（字节）,
                "file_extension": "扩展名",
                "md5_hash": "MD5值",
                "error": "错误信息（如果失败）"
            }
        """
        try:
            # 1. 验证文件
            is_valid, error_msg = self.validate_file(upload_file, resource_type)
            if not is_valid:
                logger.warning(f"文件验证失败: {error_msg}")
                return {
                    "success": False,
                    "error": error_msg
                }

            # 2. 生成唯一文件名
            unique_filename = self.generate_unique_filename(
                upload_file.filename,
                resource_type
            )

            # 3. 获取存储路径
            file_path = self.get_storage_path(unique_filename, resource_type)

            # 4. 读取文件内容（用于计算MD5和获取实际大小）
            file_content = await upload_file.read()
            file_size = len(file_content)

            # 再次验证文件大小
            max_size_bytes = self.MAX_FILE_SIZE[resource_type] * 1024 * 1024
            if file_size > max_size_bytes:
                max_size_mb = self.MAX_FILE_SIZE[resource_type]
                actual_size_mb = file_size / (1024 * 1024)
                return {
                    "success": False,
                    "error": f"文件大小 {actual_size_mb:.2f}MB 超过限制 {max_size_mb}MB"
                }

            # 5. 计算MD5
            md5_hash = hashlib.md5(file_content).hexdigest()

            # 6. 保存文件
            with open(file_path, "wb") as f:
                f.write(file_content)

            # 7. 获取文件扩展名
            file_extension = os.path.splitext(upload_file.filename)[1].lower()

            logger.info(
                f"文件保存成功: {unique_filename}, "
                f"大小: {file_size} bytes, MD5: {md5_hash}"
            )

            return {
                "success": True,
                "file_name": unique_filename,
                "original_filename": upload_file.filename,
                "file_path": file_path,
                "file_size": file_size,
                "file_extension": file_extension,
                "md5_hash": md5_hash
            }

        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            return {
                "success": False,
                "error": f"文件保存失败: {str(e)}"
            }

    def delete_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
        """
        删除文件

        Args:
            file_path: 文件路径

        Returns:
            (是否成功, 错误消息)
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"文件不存在，无需删除: {file_path}")
                return True, None

            os.remove(file_path)
            logger.info(f"文件删除成功: {file_path}")
            return True, None

        except Exception as e:
            error_msg = f"删除文件失败: {str(e)}"
            logger.error(f"{error_msg}, 文件路径: {file_path}")
            return False, error_msg

    def check_file_exists(self, file_path: str) -> bool:
        """
        检查文件是否存在

        Args:
            file_path: 文件路径

        Returns:
            文件是否存在
        """
        return os.path.exists(file_path) and os.path.isfile(file_path)

    def get_file_info(self, file_path: str) -> Optional[Dict[str, any]]:
        """
        获取文件信息

        Args:
            file_path: 文件路径

        Returns:
            文件信息字典，如果文件不存在返回None
        """
        try:
            if not self.check_file_exists(file_path):
                return None

            stat = os.stat(file_path)

            return {
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "file_size": stat.st_size,
                "created_time": datetime.fromtimestamp(stat.st_ctime),
                "modified_time": datetime.fromtimestamp(stat.st_mtime),
                "is_readable": os.access(file_path, os.R_OK),
                "is_writable": os.access(file_path, os.W_OK)
            }

        except Exception as e:
            logger.error(f"获取文件信息失败: {e}, 文件路径: {file_path}")
            return None

    def check_md5_duplicate(
            self,
            md5_hash: str,
            resource_type: str
    ) -> Optional[str]:
        """
        检查是否存在相同MD5的文件（去重）

        Args:
            md5_hash: 文件的MD5值
            resource_type: 资源类型

        Returns:
            如果存在重复文件，返回文件路径；否则返回None
        """
        subdir = self.STORAGE_DIRS[resource_type]
        resource_dir = os.path.join(self.resource_base_dir, subdir)

        try:
            for filename in os.listdir(resource_dir):
                file_path = os.path.join(resource_dir, filename)

                if not os.path.isfile(file_path):
                    continue

                # 计算文件MD5
                with open(file_path, "rb") as f:
                    file_md5 = hashlib.md5(f.read()).hexdigest()

                if file_md5 == md5_hash:
                    logger.info(f"发现重复文件: {file_path}, MD5: {md5_hash}")
                    return file_path

            return None

        except Exception as e:
            logger.error(f"检查MD5重复失败: {e}")
            return None

    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """
        格式化文件大小

        Args:
            size_bytes: 文件大小（字节）

        Returns:
            格式化后的文件大小字符串
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    def get_allowed_extensions(self, resource_type: str) -> List[str]:
        """
        获取指定资源类型允许的文件扩展名列表

        Args:
            resource_type: 资源类型

        Returns:
            允许的扩展名列表
        """
        return self.ALLOWED_EXTENSIONS.get(resource_type, [])

    def get_max_file_size(self, resource_type: str) -> int:
        """
        获取指定资源类型的最大文件大小（MB）

        Args:
            resource_type: 资源类型

        Returns:
            最大文件大小（MB）
        """
        return self.MAX_FILE_SIZE.get(resource_type, 10)


# 创建全局文件处理器实例
file_handler = FileHandler()