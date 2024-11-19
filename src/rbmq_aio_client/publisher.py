import json
import addict
import asyncio
import aio_pika
import aio_pika.abc
import queue
from structlog import get_logger
from typing import Union
from datetime import datetime, UTC


class Publisher:
    def __init__(self, config: "dict", debug=True):
        self.__config = addict.Dict(config)
        self.__debug = debug
        self.__logger = get_logger()
        self.__should_loop = True

    async def __get_connection(self, connection_type) -> "aio_pika.RobustConnection":
        loop = asyncio.get_running_loop()
        if isinstance(connection_type, str):
            connection_args = self.__config.connections.get(connection_type)
            connection: aio_pika.RobustConnection = await aio_pika.connect_robust(
                connection_args.uri, loop=loop, timeout=connection_args.timeout
            )
        elif isinstance(connection_type, aio_pika.RobustConnection):
            connection = connection_type
        else:
            raise Exception("Invalid Connection Type")

        if (
            self.__debug
            and isinstance(connection_type, str)
            and isinstance(connection_args, dict)
        ):
            self.__logger.debug(f"ConnectionProfile: {connection_args.uri}")
        self.__logger.info("Connection Established")
        return connection

    async def __get_channel(
        self, connection: "aio_pika.RobustConnection"
    ) -> "aio_pika.abc.AbstractChannel":
        channel: "aio_pika.abc.AbstractChannel" = await connection.channel()
        self.__logger.info("Channel Established")
        return channel

    async def __get_exchange(
        self, exchange: str, channel: "aio_pika.abc.AbstractChannel"
    ):
        exchange_args = self.__config.exchanges.get(exchange)

        if self.__debug:
            for key, value in exchange_args.items():
                self.__logger.debug(f"QueueProfile: {key}={value}")

        exchange_obj: aio_pika.Exchange = await channel.declare_exchange(
            exchange_args.name,
            exchange_args.type,
            durable=exchange_args.durable,
            auto_delete=exchange_args.auto_delete,
            internal=exchange_args.internal,
            passive=exchange_args.passive,
            timeout=exchange_args.timeout,
        )
        self.__logger.info("Exchange Declared")
        return exchange_obj

    async def __process_message_queue(self, exchange_obj):
        item = None
        try:
            self.__logger.info("Starting to read message")
            while True:
                try:
                    if self.__debug:
                        self.__logger.debug("QSize: {}".format(self.__queue.qsize()))

                    # item = self.__queue.get(block=True, timeout=3)
                    item = await self.__queue.get()

                    if self.__debug:
                        self.__logger.debug(
                            "Message Profile: {}".format(item[0].message_id)
                        )

                    await exchange_obj.publish(item[0], item[1], timeout=item[2])
                    item = None
                except queue.Empty:
                    if self.__debug:
                        self.__logger.debug("Queue empty timeout", queue=self.__queue)
                except Exception as e:
                    self.__logger.error(e)
                    raise e

        except Exception as e:
            self.__logger.error(e)
            if item is not None:
                await self.__queue.put(item)
                self.__logger.info("Message Requed")

    async def __main(
        self, connection_type: "Union[str, aio_pika.RobustConnection]", exchange: "str"
    ):
        delay_counter = 1
        while self.__should_loop:
            try:
                self.__connection = await self.__get_connection(connection_type)
                channel = await self.__get_channel(self.__connection)
                exchange_obj = await self.__get_exchange(exchange, channel)

                delay_counter = 1
                await self.__process_message_queue(exchange_obj)

                await self.__connection.close()
            except Exception as e:
                await asyncio.sleep(0.3 * delay_counter)
                delay_counter += 1
                self.__logger.error(e)

    async def run(self, connection: str, exchange: str, queue_size=1000000):
        self.__queue = asyncio.Queue(maxsize=queue_size)

        # this assumes the loop is already running
        loop = asyncio.get_event_loop()

        self.__task = loop.create_task(self.__main(connection, exchange))

        # if self.__is_daemon:
        #     self.__queue = queue.Queue(maxsize=queue_size)
        # #     self.__run_daemon_thread(connection, exchange)
        # # else:
        # await self.__run_blocking(connection, exchange)
        # # asyncio.create_task(self.__main(connection, exchange))

    async def stop(self):
        self.__should_loop = False
        if self.__connection:
            try:
                await self.__connection.close()
            except Exception as e:
                self.__logger.error(e)
        self.__task.cancel()

    async def push(
        self,
        routing_key: "str",
        message_id: "str",
        payload: "str",
        persistent: "int" = aio_pika.DeliveryMode.PERSISTENT,
        expiration: "int" = 86400,
        publish_timeout: "int" = 1,
        reply_queue: "str" = None,
    ):
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=persistent,
            expiration=expiration,
            message_id=message_id,
            timestamp=datetime.now(UTC),
            reply_to=reply_queue,
        )
        await self.__queue.put((message, routing_key, publish_timeout))
        if self.__debug:
            self.__logger.info(
                "message put in queue", queue=self.__queue, quesize=self.__queue.qsize()
            )

        # this allows for method chaining
        return self
