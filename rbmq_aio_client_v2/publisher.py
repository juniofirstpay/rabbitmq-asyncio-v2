import json
import time
import addict
import asyncio
import aio_pika
import aio_pika.abc
import threading
import queue
from structlog import get_logger
from typing import List, Union
from datetime import datetime

class Publisher:
    
    def __init__(self, config: "dict", debug=True):
        self.__config = addict.Dict(config)
        self.__debug = debug
        self.__logger = get_logger()
        self.__messages: "List[aio_pika.Message, str]" = []
        self.__is_daemon = False
        self.__should_loop = True
    
    async def __get_connection(self, connection_type) -> "aio_pika.RobustConnection":
        loop = asyncio.get_running_loop()
        if isinstance(connection_type, str):
            connection_args = self.__config.connections.get(connection_type)
            connection: aio_pika.RobustConnection = await aio_pika.connect_robust(connection_args.uri,
                                                                                  loop=loop,
                                                                                  timeout=connection_args.timeout)
        elif isinstance(connection_type, aio_pika.RobustConnection):
            connection = connection_type
        else:
            raise Exception("Invalid Connection Type")
        
        if self.__debug and isinstance(connection_type, str) and isinstance(connection_args, dict):
            self.__logger.debug(f"ConnectionProfile: {connection_args.uri}")
        self.__logger.info("Connection Established")
        return connection

    async def __get_channel(self, connection: "aio_pika.RobustConnection") -> "aio_pika.abc.AbstractChannel":
        channel: "aio_pika.abc.AbstractChannel" = await connection.channel()
        self.__logger.info("Channel Established")
        return channel
    
    async def __get_exchange(self, exchange: str, channel: "aio_pika.abc.AbstractChannel"):
        exchange_args = self.__config.exchanges.get(exchange)

        if self.__debug:
            for key, value in exchange_args.items():
                self.__logger.debug(f"QueueProfile: {key}={value}")

        exchange_obj: aio_pika.Exchange = await channel.declare_exchange(exchange_args.name,
                                                                         exchange_args.type,
                                                                         durable=exchange_args.durable,
                                                                         auto_delete=exchange_args.auto_delete,
                                                                         internal=exchange_args.internal,
                                                                         passive=exchange_args.passive,
                                                                         timeout=exchange_args.timeout)
        self.__logger.info("Exchange Declared")
        return exchange_obj
        
    async def __process_message_queue(self, exchange_obj):
        item = None
        try:
            self.__logger.info("Starting to read message")
            while True:
                try:
                    if self.__debug:
                        self.__logger.debug(
                            'QSize: {}'.format(self.__queue.qsize()))

                    # item = self.__queue.get(block=True, timeout=3)
                    item = await asyncio.to_thread(self.__queue.get, block=True, timeout=3)
                    

                    if self.__debug:
                        self.__logger.debug(
                            'Message Profile: {}'.format(item[0].message_id))

                    await exchange_obj.publish(item[0], item[1], timeout=item[2])
                    item = None
                except queue.Empty:
                    if self.__debug:
                        self.__logger.debug(
                            "Queue empty timeout", queue=self.__queue)
                except Exception as e:
                    self.__logger.error(e)
                    raise e

        except Exception as e:
            self.__logger.error(e)
            if item is not None:
                self.__queue.put(item, block=True)
                self.__logger.info("Message Requed")
    
    async def __process_message_list(self, exchange_obj):
        for index, item in enumerate(self.__messages):
            try:
                if self.__debug:
                    self.__logger.debug('Message Published @ {}'.format(index))
                    self.__logger.debug(
                        'Message Profile: {}'.format(item[0].message_id))

                await exchange_obj.publish(item[0], item[1], timeout=item[2])
            except Exception as e:
                if self.__debug:
                    self.logger.error(e)
    
    async def __main(self, connection_type: "Union[str, aio_pika.RobustConnection]", exchange: "str"):
        delay_counter = 1
        while self.__should_loop:
            try:
                connection = await self.__get_connection(connection_type)
                channel = await self.__get_channel(connection)
                exchange_obj = await self.__get_exchange(exchange, channel)
                
                delay_counter = 1
                if self.__is_daemon:
                    await self.__process_message_queue(exchange_obj)
                else:
                    await self.__process_message_list(exchange_obj)
                
                await connection.close()
                if self.__is_daemon == False:
                    self.__should_loop = False 
            except Exception as e:
                time.sleep(0.3 * delay_counter)
                delay_counter += 1
                self.__logger.error(e)
    
    async def __run_daemon_thread(self, connection, exchange):
        def _daemon_thread_worker(self: "Publisher"):
            asyncio.run(self.__main(connection, exchange))
        self.__daemon_thread = threading.Thread(target=_daemon_thread_worker, args=[self], daemon=True)
        self.__daemon_thread.start()
    
    async def __run_blocking(self, connection, exchange):
        existing_loop = False
        try:
            loop = asyncio.get_running_loop()
            existing_loop = True
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        print("existing_loop", existing_loop)
        
        if existing_loop:
            loop.create_task(self.__main(connection, exchange))
        else:
            loop.run_until_complete(self.__main(connection, exchange))
    
    async def mark_daemon(self):
        self.__is_daemon = True
        
    async def run(self, connection: str, exchange: str, queue_size=1000000):
        # self.__queue = queue.Queue(maxsize=queue_size)    
        if self.__is_daemon:
            self.__queue = queue.Queue(maxsize=queue_size)    
        #     self.__run_daemon_thread(connection, exchange)
        # else:
        await self.__run_blocking(connection, exchange)
        # asyncio.create_task(self.__main(connection, exchange))

    async def push(self,
             routing_key: "str",
             message_id: "str",
             payload: "str",
             persistent: "int" = aio_pika.DeliveryMode.PERSISTENT,
             expiration: "int" = 86400,
             publish_timeout: "int" = 1,
             reply_queue: "str" = None):
       
        message = aio_pika.Message(body=json.dumps(payload).encode(),
                                   delivery_mode=persistent,
                                   expiration=expiration,
                                   message_id=message_id,
                                   timestamp=datetime.utcnow().timestamp(),
                                   reply_to=reply_queue)
        
        if self.__is_daemon:
            await asyncio.to_thread(self.__queue.put, [message, routing_key, publish_timeout])
            # self.__queue.put([message, routing_key, publish_timeout])
            if self.__debug:
                self.__logger.info("Message put in queue",
                                   queue=self.__queue, quesize=self.__queue.qsize())
        else:
            self.__messages.append([message, routing_key, publish_timeout])
        return self
