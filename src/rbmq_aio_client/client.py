import ast
import json
import aio_pika
import aio_pika.pool
import aio_pika.tools
import asyncio
import socket
import logging
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
        self.__subscriber_running = False
        self.__publisher_running = False
        logging.getLogger(__name__).setLevel(logging.DEBUG if self.__debug else logging.INFO)

    async def get_connection(self) -> aio_pika.abc.AbstractConnection:
        return await aio_pika.connect_robust(
            self.__connection_uri, timeout=self.__connection_timeout
        )

    async def get_channel(self) -> aio_pika.abc.AbstractChannel:
        async with self.__connection_pool.acquire() as connection:  # type: aio_pika.abc.AbstractConnection
            return await connection.channel()

    @asynccontextmanager
    async def setup(self):
        self.__message_queue = asyncio.queues.Queue(maxsize=self.__queue_max_size)
        self.__connection_pool = aio_pika.pool.Pool(
            self.get_connection, max_size=self.__connection_max_count
        )

        self.__channel_pool = aio_pika.pool.Pool(
            self.get_channel, max_size=self.__channel_max_count
        )

        yield

        await self.__channel_pool.close()
        await self.__connection_pool.close()

    async def configure(self):
        self.__message_queue = asyncio.queues.Queue(maxsize=self.__queue_max_size)
        self.__connection_pool = aio_pika.pool.Pool(
            self.get_connection, max_size=self.__connection_max_count
        )

        self.__channel_pool = aio_pika.pool.Pool(
            self.get_channel, max_size=self.__channel_max_count
        )

    async def destroy(self):
        if self.__publisher_running:
            await self.__message_queue.put("stop")
            await self.__publisher_stop_event.wait()
            await logger.adebug("publisher stop relay received")

        if self.__subscriber_running:
            self.__subscriber_running = False
            await self.__subscriber_stop_event.wait()
            await logger.adebug("subscriber stop relay received")

        await logger.adebug("closing channel pool")
        await self.__channel_pool.close()
        await logger.adebug("closing connection pool")
        await self.__connection_pool.close()

    async def create_publisher(
        self, exchange_config: ExchangeConfig, stop_event: asyncio.Event
    ):
        await logger.adebug("starting publisher")
        
        delay_counter = 0
        
        while self.__publisher_running:
            plogger = logger.bind(counter=delay_counter)    
            try:
                if self.__channel_pool.is_closed:
                    await plogger.adebug("breaking the loop since channel pool is already closed")
                    break
                async with self.__connection_pool, self.__channel_pool:
                    await plogger.adebug("established channel pool + connection pool")

                    async with self.__channel_pool.acquire() as ch:  # type: aio_pika.abc.AbstractChannel
                        await plogger.adebug("channel acquired")        
                        channel: aio_pika.abc.AbstractChannel = ch
                        exchange: aio_pika.abc.AbstractExchange = (
                            await channel.declare_exchange(
                                name=exchange_config.name,
                                type=exchange_config.type,
                                durable=exchange_config.durable,
                                auto_delete=exchange_config.auto_delete,
                                internal=exchange_config.internal,
                                passive=exchange_config.passive,
                                timeout=exchange_config.timeout,
                            )
                        )
                        await plogger.adebug("exchange published")        
                        while self.__publisher_running:
                            await plogger.adebug("publisher inner iterator started")        
                            item: Tuple[aio_pika.Message, str, int] | str = (
                                await self.__message_queue.get()
                            )

                            await plogger.adebug("read an item off queue")

                            if isinstance(item, str) and item == "stop":
                                await plogger.adebug("stop item read off the queue")
                                raise Exception("stop-publisher")

                            message = item[0]
                            routing_key = item[1]
                            publish_timeout = item[2]
                            confirmation = await exchange.publish(
                                message, routing_key, timeout=publish_timeout
                            )
                            await plogger.adebug("message published")
                            await logger.ainfo(
                                f"message-delivery: {message.message_id}: {confirmation.delivery_tag}"
                            )

            except Exception as e:
                await plogger.adebug("captured an exception")
                if str(e) == "stop-publisher":
                    await plogger.adebug("stop publisher exception")
                    break
                
                logger.error(e, exc_info=self.__debug)
                delay_counter += 1
                await plogger.adebug("delay counter incremented")
                await asyncio.sleep(delay_counter * 3)
                await plogger.adebug("waking up to reconnect")
        await plogger.adebug("setting event to relay publisher stop")
        stop_event.set()

    async def create_subscriber(
        self,
        exchange_config: ExchangeConfig,
        queue_config: QueueConfig,
        callback: Callable[[Any], Awaitable[Any]],
        stop_event: asyncio.Event,
    ):
        delay_counter = 0
        while self.__subscriber_running:
            try:
                async with self.__connection_pool, self.__channel_pool:
                    async with self.__channel_pool.acquire() as ch:  # type: aio_pika.abc.AbstractChannel
                        channel: aio_pika.abc.AbstractChannel = ch
                        exchange: aio_pika.abc.AbstractExchange = (
                            await channel.declare_exchange(
                                name=exchange_config.name,
                                type=exchange_config.type,
                                durable=exchange_config.durable,
                                auto_delete=exchange_config.auto_delete,
                                internal=exchange_config.internal,
                                passive=exchange_config.passive,
                                timeout=exchange_config.timeout,
                            )
                        )
                        queue: aio_pika.Queue = await channel.declare_queue(
                            name=queue_config.name,
                            durable=queue_config.durable,
                            auto_delete=queue_config.auto_delete,
                            exclusive=queue_config.exclusive,
                        )
                        await channel.set_qos(
                            prefetch_count=queue_config.prefetch_count
                        )
                        for routing_key in queue_config.routing_keys:
                            await queue.bind(exchange=exchange, routing_key=routing_key)

                        while self.__subscriber_running:
                            message = await queue.get(timeout=1000, fail=False)

                            if message is None:
                                if self.__subscriber_running == False:
                                    break
                                else:
                                    await asyncio.sleep(0.2)
                                    continue
                            try:
                                async with message.process(ignore_processed=True):
                                    try:
                                        payload = json.loads(message.body.decode())
                                    except json.JSONDecodeError as e:
                                        payload = ast.literal_eval(
                                            message.body.decode()
                                        )
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
                if self.__subscriber_running == False:
                    break
                
                logger.error(e, exc_info=self.__debug)
                delay_counter += 1
                await asyncio.sleep(delay_counter * 3)
        stop_event.set()

    async def run_publisher(self, config: ExchangeConfig):
        self.__publisher_running = True
        self.__publisher_stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        return loop.create_task(
            self.create_publisher(config, stop_event=self.__publisher_stop_event)
        )

    async def run_subscriber(
        self,
        exchange_config: ExchangeConfig,
        queue_config: QueueConfig,
        callback: Callable[[Any], Awaitable[Any]],
    ):
        self.__subscriber_running = True
        self.__subscriber_stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        return loop.create_task(
            self.create_subscriber(
                exchange_config,
                queue_config,
                callback,
                stop_event=self.__subscriber_stop_event,
            )
        )

    async def run_healthcheck_server(self, port: int = 8000, shutdown_event: asyncio.Event=None):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("localhost", port))
        server.listen(8)
        server.setblocking(False)
        loop = asyncio.get_running_loop()
        loop.create_task(self._health_check_server(loop, server, port))
        await shutdown_event.wait()
        server.close()
    
    async def _health_check_server(self, loop, server, port: int):
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
