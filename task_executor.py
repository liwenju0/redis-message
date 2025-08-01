"""
Redis Stream 消息处理框架

本框架基于 Redis Stream 实现了一个高可用、可扩展的分布式消息处理系统。

核心概念：
1. Stream: 消息流，存储有序的消息集合
2. Consumer Group: 消费者组，支持多个消费者协同处理消息
3. Consumer: 消费者，实际处理消息的工作进程
4. ACK: 消息确认机制，确保消息不丢失

设计特点：
- 消息可靠性：支持消息确认和故障恢复
- 负载均衡：多个消费者自动分担负载
- 高可用性：支持消费者故障转移
- 监控能力：实时状态监控和健康检查
"""

import asyncio
import json
import uuid
import time
import logging
import threading
from datetime import datetime
from typing import Optional, Dict, List, Callable, Any
from abc import ABC, abstractmethod

import redis.asyncio as redis


class RedisMessage:
    """
    Redis Stream 消息封装类
    
    封装了消息的基本信息和操作方法，提供统一的消息处理接口
    """
    
    def __init__(self, redis_client, stream_name: str, group_name: str, 
                 message_id: str, payload: Dict[str, Any]):
        self._redis_client = redis_client
        self._stream_name = stream_name
        self._group_name = group_name
        self._message_id = message_id
        self._payload = json.loads(payload.get("data", "{}"))
        self._acked = False
    
    def get_payload(self) -> Dict[str, Any]:
        """获取消息内容"""
        return self._payload
    
    def get_message_id(self) -> str:
        """获取消息ID"""
        return self._message_id
    
    async def ack(self) -> bool:
        """
        确认消息已处理
        
        ACK 操作只影响当前消费者组的 Pending List，不影响其他消费者组
        """
        if self._acked:
            return True
            
        try:
            await self._redis_client.xack(self._stream_name, self._group_name, self._message_id)
            self._acked = True
            logging.debug(f"Message {self._message_id} acked successfully")
            return True
        except Exception as e:
            logging.error(f"Failed to ack message {self._message_id}: {str(e)}")
            return False


class MessageHandler(ABC):
    """
    消息处理器抽象基类
    
    定义了消息处理的统一接口，具体的业务逻辑需要继承此类实现
    """
    
    @abstractmethod
    async def handle(self, message: RedisMessage) -> bool:
        """
        处理消息的抽象方法
        
        Args:
            message: 待处理的消息
            
        Returns:
            bool: 处理成功返回 True，失败返回 False
        """
        pass
    
    def on_error(self, message: RedisMessage, error: Exception):
        """
        错误处理回调
        
        Args:
            message: 处理失败的消息
            error: 异常信息
        """
        logging.error(f"Failed to handle message {message.get_message_id()}: {str(error)}")


class RedisStreamProducer:
    """
    Redis Stream 消息生产者
    
    负责将消息发送到指定的 Stream 中
    """
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
    
    async def send_message(self, stream_name: str, payload: Dict[str, Any], 
                    max_retries: int = 3) -> Optional[str]:
        """
        发送消息到指定 Stream
        
        Args:
            stream_name: Stream 名称
            payload: 消息内容
            max_retries: 最大重试次数
            
        Returns:
            str: 消息ID，发送失败返回 None
        """
        message_data = {"data": json.dumps(payload)}
        
        for attempt in range(max_retries):
            try:
                message_id = await self.redis_client.xadd(stream_name, message_data)
                logging.info(f"Message sent to {stream_name}: {message_id}")
                return message_id
            except Exception as e:
                logging.warning(f"Send message attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logging.error(f"Failed to send message after {max_retries} attempts")
                    return None
                await asyncio.sleep(0.1 * (2 ** attempt))  # 指数退避
        
        return None


class RedisStreamConsumer:
    """
    Redis Stream 消息消费者
    
    负责从指定的 Stream 和消费者组中读取消息并处理
    """
    
    def __init__(self, redis_client, consumer_name: str = None):
        self.redis_client = redis_client
        self.consumer_name = consumer_name or f"consumer_{uuid.uuid4().hex[:8]}"
        self.running = False
        self._handlers: Dict[str, MessageHandler] = {}
    
    def register_handler(self, stream_name: str, handler: MessageHandler):
        """
        注册消息处理器
        
        Args:
            stream_name: Stream 名称
            handler: 消息处理器实例
        """
        self._handlers[stream_name] = handler
        logging.info(f"Handler registered for stream: {stream_name}")
    
    async def _ensure_consumer_group(self, stream_name: str, group_name: str):
        """
        确保消费者组存在，不存在则创建
        
        Args:
            stream_name: Stream 名称
            group_name: 消费者组名称
        """
        try:
            # 检查消费者组是否存在
            groups = await self.redis_client.xinfo_groups(stream_name)
            group_exists = any(group['name'] == group_name for group in groups)
            
            if not group_exists:
                # 创建消费者组，从 Stream 开始位置消费
                await self.redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
                logging.info(f"Consumer group '{group_name}' created for stream '{stream_name}'")
                
        except redis.exceptions.ResponseError as e:
            if "no such key" in str(e).lower():
                # Stream 不存在，创建消费者组时会自动创建 Stream
                await self.redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
                logging.info(f"Stream '{stream_name}' and group '{group_name}' created")
            else:
                raise
    
    async def _read_messages(self, stream_name: str, group_name: str, 
                      count: int = 1, block: int = 1000) -> List[RedisMessage]:
        """
        从消费者组读取消息
        
        Args:
            stream_name: Stream 名称
            group_name: 消费者组名称
            count: 每次读取的消息数量
            block: 阻塞时间（毫秒）
            
        Returns:
            List[RedisMessage]: 读取到的消息列表
        """
        try:
            # 使用 XREADGROUP 从消费者组读取消息
            result = await self.redis_client.xreadgroup(
                group_name,
                self.consumer_name,
                {stream_name: '>'},  # '>' 表示读取新消息
                count=count,
                block=block
            )
            
            messages = []
            for stream, msgs in result:
                for msg_id, fields in msgs:
                    message = RedisMessage(
                        self.redis_client, stream_name, group_name, msg_id, fields
                    )
                    messages.append(message)
            
            return messages
            
        except Exception as e:
            logging.error(f"Failed to read messages from {stream_name}: {str(e)}")
            return []
    
    async def consume(self, stream_name: str, group_name: str, 
                     recovery_mode: bool = True):
        """
        开始消费消息
        
        Args:
            stream_name: Stream 名称
            group_name: 消费者组名称
            recovery_mode: 是否启用故障恢复模式
        """
        if stream_name not in self._handlers:
            raise ValueError(f"No handler registered for stream: {stream_name}")
        
        handler = self._handlers[stream_name]
        self.running = True
        
        # 确保消费者组存在
        await self._ensure_consumer_group(stream_name, group_name)
        
        logging.info(f"Consumer {self.consumer_name} started for stream {stream_name}")
        
        # 故障恢复：优先处理所有未确认的消息
        if recovery_mode:
            await self._recover_pending_messages(stream_name, group_name, handler)
        
        # 主消费循环
        while self.running:
            try:
                messages = await self._read_messages(stream_name, group_name)
                
                for message in messages:
                    await self._handle_message(message, handler)
                    
            except KeyboardInterrupt:
                logging.info("Consumer interrupted by user")
                break
            except Exception as e:
                logging.error(f"Consumer error: {str(e)}")
                await asyncio.sleep(1)
        
        logging.info(f"Consumer {self.consumer_name} stopped")

    async def _recover_pending_messages(self, stream_name: str, group_name: str, handler: MessageHandler):
        """
        恢复所有未确认的消息
        
        Args:
            stream_name: Stream 名称
            group_name: 消费者组名称
            handler: 消息处理器
        """
        logging.info(f"Starting recovery of pending messages for consumer {self.consumer_name}")
        
        total_recovered = 0
        batch_size = 10  # 每批处理10条消息
        
        while True:
            # 获取一批未确认消息
            pending_messages = await self._process_pending_messages(stream_name, group_name, batch_size)
            
            if not pending_messages:
                break  # 没有更多未确认消息
            
            # 处理这批消息
            for message in pending_messages:
                await self._handle_message(message, handler)
                total_recovered += 1
            
            logging.info(f"Recovered batch of {len(pending_messages)} messages, total: {total_recovered}")
            
            # 如果这批消息数量少于批次大小，说明已经处理完所有消息
            if len(pending_messages) < batch_size:
                break
        
        if total_recovered > 0:
            logging.info(f"Recovery completed: {total_recovered} messages recovered")
        else:
            logging.info("No pending messages to recover")

    async def _process_pending_messages(self, stream_name: str, group_name: str, batch_size: int = 10) -> List[RedisMessage]:
        """
        处理未确认的消息（故障恢复）
        
        Args:
            stream_name: Stream 名称
            group_name: 消费者组名称
            batch_size: 批次大小
            
        Returns:
            List[RedisMessage]: 未确认的消息列表
        """
        try:
            # 获取当前消费者的待处理消息
            pending = await self.redis_client.xpending_range(
                stream_name, group_name, '-', '+', batch_size, self.consumer_name
            )
            
            messages = []
            for msg_info in pending:
                msg_id = msg_info['message_id']
                # 重新读取消息内容
                msg_data = await self.redis_client.xrange(stream_name, msg_id, msg_id)
                if msg_data:
                    _, fields = msg_data[0]
                    message = RedisMessage(
                        self.redis_client, stream_name, group_name, msg_id, fields
                    )
                    messages.append(message)
            
            if messages:
                logging.debug(f"Found {len(messages)} pending messages for recovery")
            
            return messages
            
        except Exception as e:
            logging.error(f"Failed to process pending messages: {str(e)}")
            return []
    
    async def _handle_message(self, message: RedisMessage, handler: MessageHandler):
        """
        处理单个消息
        
        Args:
            message: 待处理的消息
            handler: 消息处理器
        """
        try:
            # 调用业务处理逻辑
            success = await handler.handle(message)
            
            if success:
                # 处理成功，确认消息
                await message.ack()
                logging.debug(f"Message {message.get_message_id()} processed successfully")
            else:
                # 处理失败，不确认消息（会重新投递）
                logging.warning(f"Message {message.get_message_id()} processing failed")
                
        except Exception as e:
            # 异常处理
            handler.on_error(message, e)
            logging.error(f"Exception handling message {message.get_message_id()}: {str(e)}")
    
    def stop(self):
        """停止消费者"""
        self.running = False


class StreamMonitor:
    """
    Stream 监控器
    
    提供 Stream 和消费者组的状态监控功能
    """
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
    
    async def get_stream_info(self, stream_name: str) -> Dict[str, Any]:
        """
        获取 Stream 基本信息
        
        Args:
            stream_name: Stream 名称
            
        Returns:
            Dict: Stream 信息
        """
        try:
            info = await self.redis_client.xinfo_stream(stream_name)
            return {
                'length': info['length'],
                'groups': info['groups'],
                'first_entry': info.get('first-entry'),
                'last_entry': info.get('last-entry')
            }
        except Exception as e:
            logging.error(f"Failed to get stream info for {stream_name}: {str(e)}")
            return {}
    
    async def get_consumer_group_info(self, stream_name: str, group_name: str) -> Dict[str, Any]:
        """
        获取消费者组信息
        
        Args:
            stream_name: Stream 名称
            group_name: 消费者组名称
            
        Returns:
            Dict: 消费者组信息
        """
        try:
            groups = await self.redis_client.xinfo_groups(stream_name)
            for group in groups:
                if group['name'] == group_name:
                    return {
                        'name': group['name'],
                        'consumers': group['consumers'],
                        'pending': group['pending'],
                        'last_delivered_id': group['last-delivered-id'],
                        'lag': group.get('lag', 0)
                    }
            return {}
        except Exception as e:
            logging.error(f"Failed to get group info for {stream_name}:{group_name}: {str(e)}")
            return {}
    
    async def get_pending_messages(self, stream_name: str, group_name: str) -> List[Dict[str, Any]]:
        """
        获取待处理消息列表
        
        Args:
            stream_name: Stream 名称
            group_name: 消费者组名称
            
        Returns:
            List[Dict]: 待处理消息信息
        """
        try:
            pending = await self.redis_client.xpending_range(stream_name, group_name, '-', '+', 100)
            return [
                {
                    'message_id': msg['message_id'],
                    'consumer': msg['consumer'],
                    'time_since_delivered': msg['time_since_delivered'],
                    'delivery_count': msg['delivery_count']
                }
                for msg in pending
            ]
        except Exception as e:
            logging.error(f"Failed to get pending messages: {str(e)}")
            return []


class RedisMessageFramework:
    """
    Redis 消息处理框架主类
    
    提供统一的消息处理框架接口，整合生产者、消费者和监控功能
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        """
        初始化框架
        
        Args:
            redis_url: Redis 连接URL
        """
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        logging.info(f"Redis connected to {redis_url}")
        self.producer = RedisStreamProducer(self.redis_client)
        self.monitor = StreamMonitor(self.redis_client)
        self._consumers: List[RedisStreamConsumer] = []
    
    def create_producer(self) -> RedisStreamProducer:
        """创建消息生产者"""
        return self.producer
    
    def create_consumer(self, consumer_name: str = None) -> RedisStreamConsumer:
        """
        创建消息消费者
        
        Args:
            consumer_name: 消费者名称，不指定则自动生成
            
        Returns:
            RedisStreamConsumer: 消费者实例
        """
        consumer = RedisStreamConsumer(self.redis_client, consumer_name)
        self._consumers.append(consumer)
        return consumer
    
    def get_monitor(self) -> StreamMonitor:
        """获取监控器"""
        return self.monitor
    
    async def shutdown(self):
        """关闭框架，停止所有消费者"""
        for consumer in self._consumers:
            consumer.stop()
        await self.redis_client.close()
        logging.info("Redis Message Framework shutdown completed")

# 自定义消息处理器
class TaskHandler(MessageHandler):
    async def handle(self, message: RedisMessage) -> bool:
        payload = message.get_payload()
        task_id = payload.get('task_id')
        task_type = payload.get('task_type')
        
        logging.info(f"Processing task {task_id} of type {task_type}")
        
        # 模拟任务处理
        await asyncio.sleep(1)
        
        # 模拟处理成功/失败
        return task_id != "fail"
    
async def main():
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 初始化框架
    framework = RedisMessageFramework("redis://172.18.1.7:6379/0")
    
    # 创建生产者发送消息
    producer = framework.create_producer()
    await producer.send_message("task_queue", {
        "task_id": "task_001",
        "task_type": "document_parse",
        "data": {"file_path": "/path/to/document.pdf"}
    })
    
    # 创建消费者处理消息
    consumer = framework.create_consumer("worker_001")
    consumer.register_handler("task_queue", TaskHandler())
    
    # 开始消费消息
    try:
        await consumer.consume("task_queue", "task_workers")
    except KeyboardInterrupt:
        logging.info("Received interrupt signal")
    finally:
        await framework.shutdown()

# 使用示例
if __name__ == "__main__":
    asyncio.run(main())
