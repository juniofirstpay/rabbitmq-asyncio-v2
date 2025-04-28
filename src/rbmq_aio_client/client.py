import ast
import json
import aio_pika
import aio_pika.pool
import aio_pika.tools
import asyncio
import socket
import logging
import asyncio.queues
import concurrent.futures
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
        
        self.__publisher_running = False
        self.__publisher_task = None
        
        self.__subscriber_task = None
        self.__subscriber_running = False
        
        self.__socket_server = None
        self.__healthcheck_server_running = False
        self.__healthcheck_server_task = None
        
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
        if self.__publisher_running == True:
            self.__publisher_running = False
        
        tasks = []
        
        if self.__subscriber_task:
            tasks.append(self.__subscriber_task)
        
        if self.__publisher_task:
            tasks.append(self.__publisher_task)

        await logger.adebug("before wait")
        _ = concurrent.futures.wait(tasks, timeout=None, return_when=concurrent.futures.ALL_COMPLETED)
        await logger.adebug("after wait")

        await logger.adebug("closing channel pool")
        await self.__channel_pool.close()
        await logger.adebug("closing connection pool")
        await self.__connection_pool.close()

    async def create_publisher(
        self, exchange_config: ExchangeConfig
    ):
        await logger.adebug("starting publisher")
        
        delay_counter = 0
        
        while (self.__publisher_running or self.__message_queue.qsize() > 0):
            plogger = logger.bind(counter=delay_counter, type="publisher")    
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
                        while (self.__publisher_running or self.__message_queue.qsize() > 0):
                            # await plogger.adebug("publisher inner iterator started")        
                            try:
                                item: Tuple[aio_pika.Message, str, int] | str = self.__message_queue.get_nowait()
                            except asyncio.QueueEmpty:
                                if self.__publisher_running == False:
                                    break
                                else:
                                    await asyncio.sleep(0.2)
                                    continue

                            await plogger.adebug("read an item off queue")

                            # if isinstance(item, str) and item == "stop":
                            #     await plogger.adebug("stop item read off the queue")
                            #     raise Exception("stop-publisher")

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
                # if str(e) == "stop-publisher":
                #     await plogger.adebug("stop publisher exception")
                #     break
                
                logger.error(e, exc_info=self.__debug)
                delay_counter += 1
                await plogger.adebug("delay counter incremented")
                await asyncio.sleep(delay_counter * 3)
                await plogger.adebug("waking up to reconnect")
            
            await logger.adebug("publisher-running", value=self.__publisher_running)
            if self.__publisher_running == False:
                break

    async def create_subscriber(
        self,
        exchange_config: ExchangeConfig,
        queue_config: QueueConfig,
        callback: Callable[[Any], Awaitable[Any]]
    ):
        delay_counter = 0
        while self.__subscriber_running:
            plogger = logger.bind(counter=delay_counter, type="subscriber")    
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
                        queue: aio_pika.Queue = await channel.declare_queue(
                            name=queue_config.name,
                            durable=queue_config.durable,
                            auto_delete=queue_config.auto_delete,
                            exclusive=queue_config.exclusive,
                        )
                        await plogger.adebug("queue declared")
                        await channel.set_qos(
                            prefetch_count=queue_config.prefetch_count
                        )
                        for routing_key in queue_config.routing_keys:
                            await queue.bind(exchange=exchange, routing_key=routing_key)

                        await plogger.adebug("subscriber iterator starting")
                        while self.__subscriber_running:
                            message = await queue.get(timeout=1000, fail=False)

                            if message is None and self.__subscriber_running == False:
                                await plogger.adebug("breaking the inner loop")
                                break
                            elif message is None:
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
                logger.error(e, exc_info=self.__debug)
                delay_counter += 1
                await asyncio.sleep(delay_counter * 3)
            await plogger.adebug("is_subscriber_running", value=self.__subscriber_running)
            if self.__subscriber_running == False:
                    break

    async def run_publisher(self, config: ExchangeConfig):
        self.__publisher_running = True
        loop = asyncio.get_running_loop()
        self.__publisher_task = loop.create_task(self.create_publisher(config))

    async def run_subscriber(
        self,
        exchange_config: ExchangeConfig,
        queue_config: QueueConfig,
        callback: Callable[[Any], Awaitable[Any]],
    ):
        self.__subscriber_running = True
        await self.create_subscriber(
            exchange_config,
            queue_config,
            callback,
        )

    async def run_healthcheck_server(self, port: int = 8000):
        self.__healthcheck_server_running = True
        self.__socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__socket_server.bind(("localhost", port))
        self.__socket_server.listen(8)
        self.__socket_server.setblocking(False)
        loop = asyncio.get_running_loop()
        self.__healthcheck_server_task = loop.create_task(self._health_check_server(loop, self.__socket_server, port))
    
    async def _health_check_server(self, loop, server, port: int):
        while self.__healthcheck_server_running:
            await logger.adebug("running healthcheck server loop")
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


    async def shutdown(self):
        if self.__socket_server:
            await logger.adebug("calling for socket server close")
            self.__socket_server.close()
        
        if self.__healthcheck_server_task:
            self.__healthcheck_server_running = False
            self.__healthcheck_server_task.cancel()
        
        self.__subscriber_running = False
        self.__publisher_running = False

        
