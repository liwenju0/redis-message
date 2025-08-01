"""
精简版异步MinIO对象存储操作框架

提供核心的异步MinIO对象存储功能：
- 基础对象操作（上传、下载、删除、列表）
- 元数据管理
- 预签名URL生成
- 并发控制

作者: AI Assistant
版本: 2.1.0 (精简版)
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, BinaryIO, Dict, List, Optional, Union

from minio import Minio
from minio.error import S3Error


# ============================================================================
# 配置和数据类
# ============================================================================

@dataclass
class MinioConfig:
    """MinIO连接配置"""
    host: str
    access_key: str
    secret_key: str
    secure: bool = False
    region: Optional[str] = None
    max_workers: int = 10
    max_concurrent: int = 20
    
    @classmethod
    def from_dict(cls, config_dict: dict) -> 'MinioConfig':
        """从字典创建配置"""
        return cls(**config_dict)


@dataclass
class ObjectMetadata:
    """对象元数据"""
    content_type: Optional[str] = None
    custom_metadata: Dict[str, str] = field(default_factory=dict)
    
    def get_safe_metadata(self) -> Dict[str, str]:
        """获取ASCII安全的元数据 - 使用Unicode转义编码"""
        safe_metadata = {}
        for key, value in self.custom_metadata.items():
            # 使用backslashreplace自动处理所有Unicode字符
            str_value = str(value).encode("ascii", "backslashreplace").decode()
            safe_metadata[key] = str_value
        return safe_metadata
    
    @staticmethod
    def decode_metadata(metadata: Dict[str, str]) -> Dict[str, str]:
        """解码元数据中的Unicode转义字符"""
        decoded_metadata = {}
        for key, value in metadata.items():
            try:
                # 检查是否包含转义字符
                if '\\u' in value or '\\U' in value or '\\x' in value:
                    decoded_value = value.encode("ascii").decode("unicode-escape")
                    decoded_metadata[key] = decoded_value
                else:
                    decoded_metadata[key] = value
            except (UnicodeDecodeError, UnicodeEncodeError):
                # 如果解码失败，保持原值
                decoded_metadata[key] = value
        return decoded_metadata
    
    def to_headers(self) -> Dict[str, str]:
        """转换为HTTP头格式（已弃用，建议使用get_safe_metadata）"""
        headers = {}
        if self.content_type:
            headers['Content-Type'] = self.content_type
        
        # 使用新的安全元数据方法
        safe_metadata = self.get_safe_metadata()
        for key, value in safe_metadata.items():
            headers[f'x-amz-meta-{key.lower()}'] = value
        
        return headers


# ============================================================================
# 异常类
# ============================================================================

class MinioStorageError(Exception):
    """MinIO存储操作异常"""
    pass


# ============================================================================
# 异步连接管理器
# ============================================================================

class AsyncConnectionManager:
    """异步MinIO连接管理器"""
    
    def __init__(self, config: MinioConfig):
        self.config = config
        self._client: Optional[Minio] = None
        self._lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=config.max_workers)
    
    @asynccontextmanager
    async def get_client(self):
        """获取客户端连接"""
        client = await self._ensure_client()
        yield client
    
    async def _ensure_client(self) -> Minio:
        """确保客户端连接可用"""
        async with self._lock:
            if self._client is None:
                self._client = await self._create_client()
            return self._client
    
    async def _create_client(self) -> Minio:
        """创建MinIO客户端"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: Minio(
                self.config.host,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure,
                region=self.config.region
            )
        )
    
    async def close(self):
        """关闭连接管理器"""
        # 清理MinIO客户端引用
        async with self._lock:
            if self._client is not None:
                self._client = None
        
        # 关闭线程池
        if self._executor is not None:
            self._executor.shutdown(wait=True)


# ============================================================================
# 异步MinIO存储实现
# ============================================================================

class AsyncMinioStorage:
    """异步MinIO对象存储实现"""
    
    def __init__(self, config: MinioConfig):
        self.config = config
        self.connection_manager = AsyncConnectionManager(config)
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
    
    async def upload_object(self, bucket: str, key: str, data: Union[bytes, BinaryIO], 
                          metadata: Optional[ObjectMetadata] = None) -> bool:
        """
        异步上传对象
        
        Args:
            bucket: 存储桶名称
            key: 对象键
            data: 对象数据
            metadata: 对象元数据
            
        Returns:
            bool: 上传是否成功
        """
        async with self._semaphore:
            try:
                async with self.connection_manager.get_client() as client:
                    loop = asyncio.get_event_loop()
                    
                    def _upload():
                        # 确保存储桶存在
                        if not client.bucket_exists(bucket):
                            client.make_bucket(bucket)
                        
                        # 准备数据流
                        if isinstance(data, bytes):
                            data_stream = BytesIO(data)
                            data_size = len(data)
                        else:
                            data_stream = data
                            data_size = data.getbuffer().nbytes if hasattr(data, 'getbuffer') else -1
                        
                        # 准备元数据
                        content_type = None
                        custom_metadata = {}
                        
                        if metadata:
                            content_type = metadata.content_type
                            custom_metadata = metadata.get_safe_metadata()
                        
                        # 上传对象
                        client.put_object(
                            bucket, 
                            key, 
                            data_stream, 
                            data_size,
                            content_type=content_type,
                            metadata=custom_metadata
                        )
                        return True
                    
                    return await loop.run_in_executor(self.connection_manager._executor, _upload)
                    
            except Exception as e:
                logging.error(f"Failed to upload {bucket}/{key}: {e}")
                return False
    
    async def download_object(self, bucket: str, key: str) -> Optional[bytes]:
        """
        异步下载对象
        
        Args:
            bucket: 存储桶名称
            key: 对象键
            
        Returns:
            bytes: 对象数据，如果不存在返回None
        """
        async with self._semaphore:
            try:
                async with self.connection_manager.get_client() as client:
                    loop = asyncio.get_event_loop()
                    
                    def _download():
                        response = client.get_object(bucket, key)
                        return response.read()
                    
                    return await loop.run_in_executor(self.connection_manager._executor, _download)
                    
            except S3Error as e:
                if e.code in ["NoSuchKey", "NoSuchBucket"]:
                    return None
                logging.error(f"Failed to download {bucket}/{key}: {e}")
                return None
            except Exception as e:
                logging.error(f"Failed to download {bucket}/{key}: {e}")
                return None
    
    async def delete_object(self, bucket: str, key: str) -> bool:
        """
        异步删除对象
        
        Args:
            bucket: 存储桶名称
            key: 对象键
            
        Returns:
            bool: 删除是否成功
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    self.connection_manager._executor,
                    client.remove_object,
                    bucket, key
                )
                return True
        except Exception as e:
            logging.error(f"Failed to delete {bucket}/{key}: {e}")
            return False
    
    async def object_exists(self, bucket: str, key: str) -> bool:
        """
        异步检查对象是否存在
        
        Args:
            bucket: 存储桶名称
            key: 对象键
            
        Returns:
            bool: 对象是否存在
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                
                def _check():
                    if not client.bucket_exists(bucket):
                        return False
                    client.stat_object(bucket, key)
                    return True
                
                return await loop.run_in_executor(self.connection_manager._executor, _check)
        except S3Error as e:
            if e.code in ["NoSuchKey", "NoSuchBucket", "ResourceNotFound"]:
                return False
            logging.error(f"Error checking object existence {bucket}/{key}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error checking object existence {bucket}/{key}: {e}")
            return False
    
    async def list_objects(self, bucket: str, prefix: str = "", recursive: bool = True) -> List[Dict[str, Any]]:
        """
        异步列出对象
        
        Args:
            bucket: 存储桶名称
            prefix: 对象前缀
            recursive: 是否递归列出
            
        Returns:
            List[Dict]: 对象信息列表
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                
                def _list():
                    objects = []
                    for obj in client.list_objects(bucket, prefix=prefix, recursive=recursive):
                        objects.append({
                            'name': obj.object_name,
                            'size': obj.size,
                            'last_modified': obj.last_modified,
                            'etag': obj.etag,
                            'bucket': bucket
                        })
                    return objects
                
                return await loop.run_in_executor(self.connection_manager._executor, _list)
        except Exception as e:
            logging.error(f"Failed to list objects in {bucket}: {e}")
            return []
    
    async def get_object_metadata(self, bucket: str, key: str) -> Optional[ObjectMetadata]:
        """
        异步获取对象的元数据
        
        Args:
            bucket: 存储桶名称
            key: 对象键
            
        Returns:
            ObjectMetadata: 对象元数据，如果对象不存在返回None
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                
                def _get_metadata():
                    stat = client.stat_object(bucket, key)
                    
                    # 提取自定义元数据
                    custom_metadata = {}
                    for key_name, value in stat.metadata.items():
                        if key_name.lower().startswith('x-amz-meta-'):
                            custom_key = key_name[11:]  # 移除 'x-amz-meta-' 前缀
                            custom_metadata[custom_key] = value
                    
                    # 解码Unicode转义字符
                    decoded_metadata = ObjectMetadata.decode_metadata(custom_metadata)
                    
                    return ObjectMetadata(
                        content_type=stat.content_type,
                        custom_metadata=decoded_metadata
                    )
                
                return await loop.run_in_executor(self.connection_manager._executor, _get_metadata)
                
        except S3Error as e:
            if e.code in ["NoSuchKey", "NoSuchBucket", "ResourceNotFound"]:
                return None
            logging.error(f"Failed to get metadata for {bucket}/{key}: {e}")
            return None
        except Exception as e:
            logging.error(f"Failed to get metadata for {bucket}/{key}: {e}")
            return None
    
    async def get_presigned_url(self, bucket: str, key: str, expires: int = 3600) -> Optional[str]:
        """
        异步获取预签名URL
        
        Args:
            bucket: 存储桶名称
            key: 对象键
            expires: 过期时间（秒）
            
        Returns:
            str: 预签名URL，失败返回None
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                from datetime import timedelta
                
                return await loop.run_in_executor(
                    self.connection_manager._executor,
                    lambda: client.get_presigned_url("GET", bucket, key, expires=timedelta(seconds=expires))
                )
        except Exception as e:
            logging.error(f"Failed to get presigned URL for {bucket}/{key}: {e}")
            return None
    
    async def create_bucket(self, bucket: str) -> bool:
        """
        异步创建存储桶
        
        Args:
            bucket: 存储桶名称
            
        Returns:
            bool: 创建是否成功
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                
                def _create():
                    if not client.bucket_exists(bucket):
                        client.make_bucket(bucket)
                    return True
                
                return await loop.run_in_executor(self.connection_manager._executor, _create)
        except Exception as e:
            logging.error(f"Failed to create bucket {bucket}: {e}")
            return False
    
    async def delete_bucket(self, bucket: str, force: bool = False) -> bool:
        """
        异步删除存储桶
        
        Args:
            bucket: 存储桶名称
            force: 是否强制删除（删除所有对象）
            
        Returns:
            bool: 删除是否成功
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                
                def _delete():
                    if not client.bucket_exists(bucket):
                        return True
                    
                    if force:
                        # 删除所有对象
                        for obj in client.list_objects(bucket, recursive=True):
                            client.remove_object(bucket, obj.object_name)
                    
                    client.remove_bucket(bucket)
                    return True
                
                return await loop.run_in_executor(self.connection_manager._executor, _delete)
        except Exception as e:
            logging.error(f"Failed to delete bucket {bucket}: {e}")
            return False
    
    async def bucket_exists(self, bucket: str) -> bool:
        """
        异步检查存储桶是否存在
        
        Args:
            bucket: 存储桶名称
            
        Returns:
            bool: 存储桶是否存在
        """
        try:
            async with self.connection_manager.get_client() as client:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    self.connection_manager._executor,
                    client.bucket_exists,
                    bucket
                )
        except Exception as e:
            logging.error(f"Error checking bucket existence {bucket}: {e}")
            return False
    
    async def close(self):
        """关闭存储实例"""
        await self.connection_manager.close()


# ============================================================================
# 工厂函数
# ============================================================================

async def create_async_minio_storage(config: Union[MinioConfig, dict]) -> AsyncMinioStorage:
    """
    创建异步MinIO存储实例的工厂函数
    
    Args:
        config: MinIO配置对象或配置字典
        
    Returns:
        AsyncMinioStorage: 异步MinIO存储实例
    """
    if isinstance(config, dict):
        config = MinioConfig.from_dict(config)
    
    return AsyncMinioStorage(config)


# ============================================================================
# 使用示例
# ============================================================================

async def main():
    """异步使用示例"""
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 创建配置
    config = MinioConfig(
        host="172.18.1.7:9002",
        access_key="deepctrl",
        secret_key="deepctrl",
        secure=False,
        max_workers=10,
        max_concurrent=20
    )
    
    # 创建异步存储实例
    storage = await create_async_minio_storage(config)
    
    try:
        # 创建存储桶
        bucket_name = "test-bucket"
        if await storage.create_bucket(bucket_name):
            print(f"存储桶 {bucket_name} 创建成功")
        
        # 创建元数据 - 测试各种Unicode字符
        metadata = ObjectMetadata(
            content_type="text/plain",
            custom_metadata={
                "author": "AI Assistant",
                "version": "2.1.0",
                "description": "精简版测试文件",
                "emoji": "😵‍💫🎉",
                "trademark": "ACME™ Anvil",
                "japanese": "こんにちは",
                "mixed": "Hello 世界 🌍"
            }
        )
        
        # 异步上传对象
        test_data = b"Hello, Simplified Async MinIO Storage!"
        if await storage.upload_object(bucket_name, "test.txt", test_data, metadata):
            print("文件上传成功")
        
        # 检查对象是否存在
        if await storage.object_exists(bucket_name, "test.txt"):
            print("对象存在")
        
        # 获取对象元数据
        obj_metadata = await storage.get_object_metadata(bucket_name, "test.txt")
        if obj_metadata:
            print(f"内容类型: {obj_metadata.content_type}")
            print(f"自定义元数据: {obj_metadata.custom_metadata}")
        
        # 异步下载对象
        downloaded_data = await storage.download_object(bucket_name, "test.txt")
        if downloaded_data:
            print(f"下载的数据: {downloaded_data.decode()}")
        
        # 异步列出对象
        objects = await storage.list_objects(bucket_name)
        for obj in objects:
            print(f"对象: {obj['name']}, 大小: {obj['size']}")
        
        # 清理
        await storage.delete_object(bucket_name, "test.txt")
        await storage.delete_bucket(bucket_name, force=True)
        print("清理完成")
        
    finally:
        # 关闭连接
        await storage.close()


if __name__ == "__main__":
    asyncio.run(main())
