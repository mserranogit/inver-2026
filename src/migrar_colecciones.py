from pymongo import MongoClient

def migrate_collections():
    client = MongoClient(
        host="localhost",
        port=27017,
        username="admin",
        password="mike",
        authSource="admin"
    )
    db = client["db-inver"]
    
    # 1. Renombrar 'carteras' a 'carteras_fondos'
    if "carteras" in db.list_collection_names():
        db["carteras"].rename("carteras_fondos")
        print("Colección 'carteras' renombrada a 'carteras_fondos' ✅")
    else:
        print("La colección 'carteras' no existe o ya fue renombrada.")

    # 2. Asegurar que 'carteras_etf' existe
    if "carteras_etf" not in db.list_collection_names():
        db.create_collection("carteras_etf")
        print("Colección 'carteras_etf' creada ✅")
    else:
        print("La colección 'carteras_etf' ya existe.")

    client.close()

if __name__ == "__main__":
    migrate_collections()
