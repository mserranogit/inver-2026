from pymongo import MongoClient

def update_etf_field_name():
    client = MongoClient(
        host="localhost",
        port=27017,
        username="admin",
        password="mike",
        authSource="admin"
    )
    db = client["db-inver"]
    coll = db["carteras_etf"]
    
    # Renombrar 'fondos' a 'etfs'
    res1 = coll.update_many(
        {"fondos": {"$exists": True}},
        {"$rename": {"fondos": "etfs"}}
    )
    print(f"Documentos actualizados (fondos -> etfs): {res1.modified_count}")

    # Por si acaso existiera alguno con 'activos' de una versión previa experimental
    res2 = coll.update_many(
        {"activos": {"$exists": True}},
        {"$rename": {"activos": "etfs"}}
    )
    print(f"Documentos actualizados (activos -> etfs): {res2.modified_count}")

    client.close()

if __name__ == "__main__":
    update_etf_field_name()
