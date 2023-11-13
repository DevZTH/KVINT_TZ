# %%
import json_stream
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from tqdm import tqdm
# {'phone': 0, 'start_date': 1684957028364, 'end_date': 1684957074364}

# %%
client = MongoClient("mongodb://root:example@localhost:27017")
client.admin.command('ping')
db = client["test"]
collection : Collection = db.call_record

# %%
def as_chunk(lst, size):
    acc, cnt =[], 0
    for l in lst:
        cnt += 1
        # streamed item can read only once lib bug ?
        accdict = {}
        for k,v in l.items():
            accdict[k]=v
        acc.append(accdict)
        if cnt >= size:
            yield acc
            acc, cnt = [], 0
    if acc:
        yield acc

# %%
with open("data/data.json", "rt") as file:
    data = json_stream.load(file, persistent=False)
    for chunk in tqdm(as_chunk(data, 500), total=0 ):
        collection.insert_many(chunk)
        #pass

# %%
#file = open("data/data.json", "rt")
#data = json_stream.load(file, persistent=False)
#for a in data[0].items():
#    print(a)
# %%
