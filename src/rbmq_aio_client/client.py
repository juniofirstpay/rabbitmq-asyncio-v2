import ast
import json
import aio_pika
import aio_pika.pool
import aio_pika.tools
import asyncio
import socket
import asyncio.queues
from contextlib import asynccontextmanager
from structlog import get_logger
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Tuple, List, Awaitable, Any, Callable

logger = get_logger(__name__)

@dataclass
class ConnectionConfig:
    connection_uri: str
    connection_timeout: int
    connection_max_count: int
    channel_max_count: int
    queue_max_size: int
    debug: bool = False

@dataclass
class ExchangeConfig:
    name: str
    type: str
    durable: bool
    auto_delete: bool
    internal: bool
    passive: bool
    timeout: int

@dataclass
class QueueConfig:
    name: str
    durable: bool
    auto_delete: bool
    exclusive: bool
    routing_keys: List[str]
    prefetch_count: int = 10

class RBMQAsyncioClient:

    def __init__(self, config: ConnectionConfig):
        self.__connection_uri: str = config.connection_uri
        self.__connection_timeout: int = config.connection_timeout
        self.__connection_max_count: int = config.connection_max_count
        self.__channel_max_count: int = config.channel_max_count
        self.__queue_max_size: int = config.queue_max_size
        self.__debug: bool = config.debug

    async def get_connection(self) -> aio_pika.abc.AbstractConnection:
        return await aio_pika.connect_robust(self.__connection_uri, 
                                             timeout=self.__connection_timeout)

    async def get_channel(self) -> aio_pika.abc.AbstractChannel:
        async with self.__connection_pool.acquire() as connection: # type: aio_pika.abc.AbstractConnection
            return await connection.channel()
        
    @asynccontextmanager
    async def setup(self):
        self.__message_queue = asyncio.queues.Queue(maxsize=self.__queue_max_size)
        self.__connection_pool = aio_pika.pool.Pool(self.get_connection, 
                                                    max_size=self.__connection_max_count)

        self.__channel_pool = aio_pika.pool.Pool(self.get_channel, 
                                                 max_size=self.__channel_max_count)
        
        yield

        await self.__channel_pool.close()
        await self.__connection_pool.close()

    async def configure(self):
        self.__message_queue = asyncio.queues.Queue(maxsize=self.__queue_max_size)
        self.__connection_pool = aio_pika.pool.Pool(self.get_connection, 
                                                    max_size=self.__connection_max_count)

        self.__channel_pool = aio_pika.pool.Pool(self.get_channel, 
                                                 max_size=self.__channel_max_count)
        
    async def destroy(self):
        self.__publisher_running = False
        await self.__channel_pool.close()
        await self.__connection_pool.close()


    async def create_publisher(self, exchange_config: ExchangeConfig):
        delay_counter = 0
        while self.__publisher_running:
            try:
                async with self.__connection_pool, self.__channel_pool:
                    async with self.__channel_pool.acquire() as ch: # type: aio_pika.abc.AbstractChannel
                        channel: aio_pika.abc.AbstractChannel = ch
                        exchange: aio_pika.abc.AbstractExchange = await channel.declare_exchange(
                            name=exchange_config.name,
                            type=exchange_config.type,
                            durable=exchange_config.durable,
                            auto_delete=exchange_config.auto_delete,
                            internal=exchange_config.internal,
                            passive=exchange_config.passive,
                            timeout=exchange_config.timeout
                        )
                        while self.__publisher_running:
                            item: Tuple[aio_pika.Message, str, int] = await self.__message_queue.get()
                            message = item[0]
                            routing_key = item[1]
                            publish_timeout = item[2]
                            confirmation = await exchange.publish(
                                message,
                                routing_key,
                                timeout=publish_timeout
                            )
                            logger.debug(f"message-delivery: {message.message_id}: {confirmation.delivery_tag}")

            except Exception as e:
                logger.error(e, exc_info=self.__debug)
                if self.__publisher_running == True:
                    delay_counter += 1
                    await asyncio.sleep(delay_counter * 3)

    async def create_subscriber(self, exchange_config: ExchangeConfig, queue_config: QueueConfig, callback: Callable[[Any], Awaitable[Any]]):
        delay_counter = 0
        try:
            async with self.__connection_pool, self.__channel_pool:
                async with self.__channel_pool.acquire() as ch: # type: aio_pika.abc.AbstractChannel
                    channel: aio_pika.abc.AbstractChannel = ch
                    exchange: aio_pika.abc.AbstractExchange = await channel.declare_exchange(
                        name=exchange_config.name,
                        type=exchange_config.type,
                        durable=exchange_config.durable,
                        auto_delete=exchange_config.auto_delete,
                        internal=exchange_config.internal,
                        passive=exchange_config.passive,
                        timeout=exchange_config.timeout
                    )
                    queue: aio_pika.Queue = await channel.declare_queue(
                        name=queue_config.name,
                        durable=queue_config.durable,
                        auto_delete=queue_config.auto_delete,
                        exclusive=queue_config.exclusive
                    )
                    await channel.set_qos(prefetch_count=queue_config.prefetch_count)
                    for routing_key in queue_config.routing_keys:
                        await queue.bind(exchange=exchange, routing_key=routing_key)

                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            try:
                                async with message.process(ignore_processed=True):
                                    try:
                                        payload = json.loads(message.body.decode())
                                    except json.JSONDecodeError as e:
                                        payload = ast.literal_eval(message.body.decode())
                                    except:
                                        await message.reject()

                                    try:
                                        ack = await callback(payload)
                                        if ack:
                                            await message.ack()
                                        else:
                                            await message.reject()
                                    except Exception as e:
                                        logger.error(e)
                                        await message.reject(requeue=True)
                            except Exception as e:
                                logger.error(e, exc_info=self.__debug)
        except Exception as e:
            logger.error(e, exc_info=self.__debug)
            delay_counter += 1
            await asyncio.sleep(delay_counter * 3)

    async def run_publisher(self, config: ExchangeConfig):
        self.__publisher_running = True
        loop = asyncio.get_running_loop()
        return loop.create_task(self.create_publisher(config))

    async def run_subscriber(self, exchange_config: ExchangeConfig, queue_config: QueueConfig, callback: Callable[[Any], Awaitable[Any]]):
        loop = asyncio.get_running_loop()
        return loop.create_task(self.create_subscriber(exchange_config, queue_config, callback))

    async def run_healthcheck_server(self, port: int = 8000):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('localhost', port))
        server.listen(8)
        server.setblocking(False)

        loop = asyncio.get_running_loop()

        while True:
            client, _ = await loop.sock_accept(server)
            await loop.sock_sendall(client, "pong".encode("utf-8"))
            client.close()

    async def push(
        self,
        routing_key: str,
        message_id: str,
        payload: Any,
        persistent: int = aio_pika.DeliveryMode.PERSISTENT,
        expiration: int = 86400,
        publish_timeout: int = 1,
        reply_queue: str = None,
    ):
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=persistent,
            expiration=expiration,
            message_id=message_id,
            timestamp=datetime.now(UTC),
            reply_to=reply_queue,
        )
        await self.__message_queue.put((message, routing_key, publish_timeout))
        
        return self

    
