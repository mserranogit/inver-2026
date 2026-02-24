from pymongo import MongoClient
import json

MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'admin',
    'password': 'mike',
    'database': 'db-inver',
    'collection': 'etfs',
    'auth_source': 'admin'
}

def check_field():
    uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['auth_source']}"
    client = MongoClient(uri)
    db = client[MONGO_CONFIG['database']]
    coll = db[MONGO_CONFIG['collection']]
    
    # Buscar uno que ya haya sido procesado
    doc = coll.find_one({"justetf_url": {"$exists": True}})
    
    if doc:
        # Limpiar para imprimir
        doc['_id'] = str(doc['_id'])
        print(json.dumps(doc, indent=4, ensure_ascii=False))
    else:
        print("No se encontraron documentos procesados.")
    
    client.close()

if __name__ == "__main__":
    check_field()
