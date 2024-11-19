from src.publisher import Publisher
import asyncio

config = {
    "connections": {
        "default": {"timeout": 5, "uri": "amqp://guest:guest@localhost:5672/"}
    },
    "credentials": {
        "host": "localhost",
        "password": "guest",
        "port": 5672,
        "username": "guest",
    },
    "exchanges": {
        "default": {
            "auto_delete": False,
            "durable": False,
            "internal": False,
            "name": "notif-test",
            "passive": False,
            "timeout": 5,
            "type": "topic",
        }
    },
    "oms_consumer": {
        "durable": False,
        "exchange": "notif-test",
        "exchange_durable": False,
        "exchange_type": "topic",
        "prefetch_count": 10,
        "prefetch_size": 0,
        "queue": "events.django.con",
        "routing_key": "oms.django.#",
    },
    "publisher": {
        "exchange": "notif-test",
        "exchange_durable": False,
        "exchange_type": "topic",
        "routing_key_prefix": "",
    },
    "queues": {
        "test-queue": {
            "auto_delete": False,
            "durable": False,
            "exchange": "default",
            "name": "test-queue",
            "routing_key": "oms.django.*",
        }
    },
    "zeta_consumer": {
        "durable": True,
        "exchange": "notif-test",
        "exchange_durable": False,
        "exchange_type": "topic",
        "prefetch_count": 10,
        "prefetch_size": 0,
        "queue": "queue.django.zeta",
        "routing_key": "events.zeta.dispatch_update",
    },
}


async def main():
    publisher = Publisher(config=config, debug=True)

    await publisher.mark_daemon()
    await publisher.run("default", "default")
    await publisher.push(
        "oms.django.test",
        "test-id-1",
        {
            "action": "PAYTM_WEBHOOK1",
            "attributes": {"order_id": "1234"},
        },
    )
    # await publisher.run('default', 'default')
    await asyncio.sleep(5)

    # Stop the Publisher after test run
    publisher._Publisher__should_loop = False


if __name__ == "__main__":
    asyncio.run(main())
