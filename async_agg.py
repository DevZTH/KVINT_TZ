import asyncio
import aio_pika
import json
from motor.motor_asyncio import AsyncIOMotorClient
from timeit import default_timer as timer
from datetime import datetime
QUEUE_NAME = "test_queue"
QUEUE_RCV = "results"
EXCHANGE_NAME = "test_exchange"
CONNECT_AMQP = "amqp://guest:guest@rabbitmq_core/"


CONNECT_MONGO = "mongodb://root:example@mongo:27017"
MONGODB_DB = "test"
MONGODB_COLLECTON = "call_record"

MAXINT= 0xffffffff
MINUTE_COST=10


async def perform_aggregation(phones: list[int]) -> None:

    client = AsyncIOMotorClient(CONNECT_MONGO)
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTON]

    pipeline = [
        {'$match':{'phone':{ '$in':phones }}},
        {'$project':{
            'duration': {'$divide':[ {'$subtract': ["$end_date", "$start_date"]}, 1000 ] },
            'phone': 1,
        }
        },
        {'$group': {
            '_id': '$phone',
            'cnt_all_attempts': {'$sum': 1}, 
            #'total_duration': {'$sum':'$duration'}, 
            '10_sec': {'$sum': {'$cond': [{'$lt': ['$duration', 10 ]}, 1, 0 ] } },
            
            '10_30_sec': {
                '$sum': {
                    '$cond': [
                        { '$and': [ { '$gte': ['$duration', 10] }, { '$lt': ['$duration', 30] } ] },
                        1, 0 ]}
            },
            '30_sec': { '$sum':{ '$cond':[ {'$gte': ['$duration', 30] },1,0 ] } },

            'min_duration': {'$min':'$duration'},
            #'min_duration_nz': { '$min':{ '$cond':[ {'$gt': ['$duration', 0] } ,'$duration', MAXINT ] } },
            
            'max_duration': {'$max':'$duration'},
            'avg_dur_att': {'$avg':'$duration'},

            'summ_gt_15s': {'$sum': {'$cond': [ {'$gt': ['$duration', 15] } ,'$duration', 0]}}
            },
        },

        {'$project': {
            '_id': 0,
            'phone': "$_id",
            'cnt_all_attempts': 1,
            #'total_duration': 1,
            'cnt_att_dur':{
                '10_sec': '$10_sec',
                '10_30_sec': '$10_30_sec',
                '30_sec': '$30_sec'
            },
            "min_price_att": {'$multiply': ['$min_duration', MINUTE_COST]},
            "max_price_att": {'$multiply': ['$max_duration', MINUTE_COST]},
            "avg_dur_att": 1,
            'sum_price_att_over_15': {'$multiply': ['$summ_gt_15s', MINUTE_COST]}
         }
        }
    ]

    publish_data= await collection.aggregate(pipeline).to_list(length=None)
    
    print(publish_data)
    client.close()
    return publish_data


async def callback(message: aio_pika.IncomingMessage):
    async with message.process():
        message_body = message.body.decode()
        message_data = json.loads(message_body)
        print(f"Received message: {message_data}")
        task_received = datetime.now()

        
        correlation_id = message_data['correlation_id']
        phones = message_data['phones']
        start = timer()
        data = await perform_aggregation(phones)
        print(f"Publish: {data},{correlation_id}")
        run_time = timer()-start
        print(f"exec time: {run_time}")
        
        publish_data ={
            "correlation_id": 13242421424214,
            "status": "Complete",
            "task_received":task_received.isoformat(),
            "from": QUEUE_NAME,
            "to": QUEUE_RCV,
            "data": data,
            "total_duration": run_time
        }

        connection = await aio_pika.connect_robust(CONNECT_AMQP)
        async with connection: 
            channel = await connection.channel()
            message_body = json.dumps(publish_data)
            message = aio_pika.Message(
                message_body.encode("UTF-8"),
                content_encoding="UTF-8",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            )
            await channel.default_exchange.publish(
                message,
                routing_key=QUEUE_RCV)

            
async def consume():
    connection = await aio_pika.connect_robust(CONNECT_AMQP)

    channel = await connection.channel()
    queue = await channel.declare_queue(QUEUE_NAME, durable=False)

    await queue.consume(callback)

    print("Waiting for messages. To exit press CTRL+C")
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(consume())
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
