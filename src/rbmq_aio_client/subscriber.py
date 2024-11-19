import json
import ast
import addict
import asyncio
import aio_pika
import aio_pika.abc
import pprint
import traceback
from typing import Union
from structlog import get_logger
from typing import Callable


class Subscriber:
    def __init__(
        self, config: "dict", callback: "Callable", debug=True, expose_connection=False
    ):
        self.__config = addict.Dict(config)
        self.__debug = debug
        self.__callback = callback
        self.__logger = get_logger()
        self.__expose_connection = expose_connection
        self.__failure_sleep_counter = 0

    async def main(
        self,
        loop,
        connection_type: "Union[str, aio_pika.RobustConnection]",
        queue: "str",
    ):

        try:
            if isinstance(connection_type, str):
                connection_args = self.__config.connections.get(connection_type)
                connection: aio_pika.RobustConnection = await aio_pika.connect_robust(
                    connection_args.uri, loop=loop, timeout=connection_args.timeout
                )
            elif isinstance(connection_type, aio_pika.RobustConnection):
                connection = connection_type
            else:
                raise Exception("Invalid Connection Type")

            queue_args = self.__config.queues.get(queue)
            exchange_args = self.__config.exchanges.get(queue_args.exchange)

            if self.__debug:
                if isinstance(connection_type, str) and isinstance(
                    connection_args, dict
                ):
                    self.__logger.debug(f"ConnectionProfile: {connection_args.uri}")

                for key, value in queue_args.items():
                    self.__logger.debug(f"QueueProfile: {key}={value}")
                for key, value in exchange_args.items():
                    self.__logger.debug(f"QueueProfile: {key}={value}")

            self.__logger.info("Connection Established")
            channel: aio_pika.abc.AbstractChannel = await connection.channel()

            self.__logger.info("Channel Established")
            exchange: aio_pika.Exchange = await channel.declare_exchange(
                exchange_args.name,
                exchange_args.type,
                durable=exchange_args.durable,
                auto_delete=exchange_args.auto_delete,
                internal=exchange_args.internal,
                passive=exchange_args.passive,
                timeout=exchange_args.timeout,
            )
            self.__logger.info("Exchange Declared")
            # exchange = await channel.get_exchange(exchange_args.name)
            async with connection:
                # Creating channel
                channel: aio_pika.abc.AbstractChannel = await connection.channel()
                # Declaring queue
                queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
                    queue_args.name,
                    durable=(queue_args.durable or False),
                    auto_delete=queue_args.auto_delete,
                )
                self.__logger.info("Queue Declared")

                await queue.bind(exchange, routing_key=queue_args.routing_key)
                self.__logger.info("Queue Bound")

                async with queue.iterator() as queue_iter:
                    # Cancel consuming after __aexit__
                    async for message in queue_iter:
                        message_info = message.info()
                        self.__logger.info(
                            "Message received", id=message_info.get("message_id")
                        )

                        if self.__debug:
                            for key, value in message_info.items():
                                self.__logger.debug(
                                    "Message Info",
                                    key=key,
                                    value=value,
                                    id=message_info.get("message_id"),
                                )
                        try:
                            async with message.process(requeue=True):
                                try:
                                    payload = json.loads(message.body.decode())
                                except:
                                    payload = ast.literal_eval(message.body.decode())

                                if self.__debug:
                                    self.__logger.debug(
                                        pprint.pformat(payload),
                                        id=message_info.get("message_id"),
                                    )
                                if self.__expose_connection == True:
                                    # callback needs to be await to be processed
                                    await self.__callback(
                                        payload,
                                        connection=connection,
                                        loop=loop,
                                        exchange=exchange,
                                    )
                                else:
                                    await self.__callback(payload)

                                self.__logger.info(
                                    "Message processed successfully",
                                    id=message_info.get("message_id"),
                                )
                        except Exception as e:
                            print(e)
                            raise e
        except Exception as e:
            self.__logger.error(e)
            self.__logger.debug(traceback.format_exc())
            self.__failure_sleep_counter += 1
            self.__logger.debug(
                "sleeping for {} before recreating connection".format(
                    self.__failure_sleep_counter * 3
                )
            )
            await asyncio.sleep(self.__failure_sleep_counter * 3)

    async def run(self, connection, queue):
        while True:
            try:
                loop = asyncio.get_event_loop()
                if not loop:
                    loop = asyncio.new_event_loop()
                loop.run_until_complete(await self.main(loop, connection, queue))
                loop.close()
            except Exception as e:
                traceback.print_exc()
                print(e)
