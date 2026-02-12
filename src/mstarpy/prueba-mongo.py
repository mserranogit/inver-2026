from pymongo import MongoClient

client = MongoClient("mongodb://admin:mike@localhost:27017/admin")
db = client["db-inver"]
col = db["fondos"]

col.update_one(
    {"isin": "TEST123"},
    {"$set": {"isin": "TEST123", "test": True}},
    upsert=True
)

print("Insertado")
