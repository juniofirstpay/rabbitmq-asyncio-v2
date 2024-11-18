
import click
import json
import traceback
import threading
import asyncio
from src.subscriber import Subscriber
from src.server import HealthCheckServer
from tests.test_publisher import config
# from app.env import rabbitmq_conf_v2
# from app.events.paytm import PaytmWebhook
from structlog import get_logger

rabbitmq_conf_v2 = config

class PaytmWebhook:
    def process(self, data):
        print("data", data)

actions_dict = {
    'PAYTM_WEBHOOK1': PaytmWebhook()
}

LOGGER = get_logger()



@click.command()
@click.option("--connection")
@click.option("--queue")
@click.option("--health_port", default="8000")
@click.option("--debug", default="false")
def run(connection: "str", queue: "str", health_port: "str", debug: "str"):
    async def run_async():
        # Start HealthCheckServer
        print("HERE")
        print(connection, queue)
        # HealthCheckServer(port=int(health_port), log=True).start()
        
        # Define on_message callback function
        def on_message(payload: dict):
            try:
                if isinstance(payload, str):
                    payload = json.loads(payload)

                action = payload.get('action')
                attributes = payload.get('attributes')
                print("Action:", action)
                print("Attributes:", attributes)

                action_func = actions_dict.get(action)
                if not action_func:
                    LOGGER.warning("[action] not setup", action=action, payload=payload)
                    return

                action_func.process(attributes)
            except Exception as e:
                print("Error processing message:", e)
                traceback.print_exc()
        
        # Initialize and run Subscriber
        await Subscriber(
            rabbitmq_conf_v2, on_message, debug=(True if debug == "true" else False)
        ).run(connection, queue)

    # Run the asynchronous code
    asyncio.run(run_async())


if __name__ == '__main__':
    run()