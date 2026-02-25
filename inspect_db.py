from pymongo import MongoClient
import json
from bson import ObjectId

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

client = MongoClient(
    host="localhost",
    port=27017,
    username="admin",
    password="mike",
    authSource="admin"
)
db = client["db-inver"]
etf_coll = db["etfs"]
one_etf = etf_coll.find_one()
if one_etf:
    print(json.dumps(one_etf, indent=2, cls=JSONEncoder))
else:
    print("No ETF found")
