import asyncio
import sys
import random
import json

from aio_pika import DeliveryMode, Message, connect
from aio_pika import IncomingMessage, connect_robust, ExchangeType

QUEUE_NAME = "test_queue"
QUEUE_RCV = "results"
EXCHANGE_NAME = "test_exchange"
CONNECT_AMQP = "amqp://guest:guest@localhost/"
TASKS =1

def generate_data():
    return dict(
        phones = [random.randint(0, 200) for _ in range(10)],
        correlation_id =random.randint(0, 0xffffffff)
    )


async def main() -> None:
    
    receive_task = asyncio.create_task(consume())
    connection = await connect(CONNECT_AMQP)

    async with connection: 
        # Creating a channel
        channel = await connection.channel()

        for i in range(TASKS):
            message_body = json.dumps(generate_data())
            message = Message(
                message_body.encode("UTF-8"),
                content_encoding="UTF-8",
                delivery_mode=DeliveryMode.PERSISTENT,
            )
            # Sending the message
            await channel.default_exchange.publish(
                message,
                routing_key=QUEUE_NAME)
            print(f'send message:{i+1} body: \n{message_body}')

    await asyncio.gather(receive_task)


async def callback(message: IncomingMessage):
    async with message.process():
        message_body = message.body.decode()
        message_data = json.loads(message_body)
        message_data = json.dumps(message_data, ensure_ascii=False, indent=2)
        print(f"Received message: {message_data}")        


async def consume():
    connection = await connect_robust(CONNECT_AMQP)
    channel = await connection.channel()
    queue = await channel.declare_queue(QUEUE_RCV, durable=False)
    await queue.consume(callback)

    print("Waiting for messages. To exit press CTRL+C")
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())