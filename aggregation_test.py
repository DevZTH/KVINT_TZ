# %%
import json_stream
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from tqdm import tqdm
from bson import ObjectId
from 
# {'phone': 0, 'start_date': 1684957028364, 'end_date': 1684957074364}

# %%
client = MongoClient("mongodb://root:example@localhost:27017")
client.admin.command('ping')
db = client["test"]
collection : Collection = db.call_record

async def publish_message():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name)
    message_properties = aio_pika.spec.BasicProperties(
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT)


# %%
a = collection.find({"phone": 0})
print(a)
# %%
collection.count_documents({"phone": 0})


result = list(collection.aggregate(pipeline))
# %%
#this
MAXINT= 0xffffffff
MINUTE_COST=10

pipeline = [
    {'$match':{'phone':{ '$in':[0] }}},
    {'$project':{
        'duration': {'$divide':[ {'$subtract': ["$end_date", "$start_date"]}, 1000 ] },
        'phone': 1,
    }
    },
    {'$group': {
            '_id': '$phone',
            'cnt_all_attempts': {'$sum': 1}, 
            'total_duration': {'$sum':'$duration'}, 
            '10_sec': {'$sum': {'$cond': [{'$lt': ['$duration', 10 ]}, 1, 0 ] } },
            
            '10_30_sec': {
                '$sum': {
                    '$cond': [
                        { '$and': [ { '$gte': ['$duration', 10] }, { '$lt': ['$duration', 30] } ] },
                        1, 0
                ]}
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
            '_id' 0,
            'phone': "$_id",
            'cnt_all_attempts': 1,
            'total_duration': 1,
            'cnt_att_dur':{
                '10_sec': '$10_sec',
                '10_30_sec': '$10_30_sec',
                '30_sec': '$30_sec'
            },
            #'t_30_sec':1,
            "min_price_att": {'$multiply': ['$min_duration', MINUTE_COST]},
            "max_price_att": {'$multiply': ['$max_duration', MINUTE_COST]},
            "avg_dur_att": 1,
            'sum_price_att_over_15': {'$multiply': ['$summ_gt_15s', MINUTE_COST]}
        }
    }
]

list(collection.aggregate(pipeline))

# %%
pipeline = [
    {'$match':{'phone':{ '$in':[0] }}},
    {'$project':{
        'duration': {'$divide':[ {'$subtract': ["$end_date", "$start_date"]}, 1000 ] },
        'phone': 1,
    }
    },
]
list(collection.aggregate(pipeline))
# %%

