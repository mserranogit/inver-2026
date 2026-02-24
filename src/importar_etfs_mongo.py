import json
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Configuración MongoDB
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'admin',
    'password': 'mike',
    'database': 'db-inver',
    'collection': 'etfs',
    'auth_source': 'admin'
}

def importar_etfs():
    # Rutas de los archivos JSON
    files = [
        os.path.join('assets', 'json', 'etf_open_R1.json'),
        os.path.join('assets', 'json', 'etf_open_R2.json'),
        os.path.join('assets', 'json', 'etf_open_R3.json')
    ]
    
    # Comprobar si los archivos existen
    records_to_insert = []
    for file_path in files:
        if not os.path.exists(file_path):
            print(f"⚠️ Advertencia: El archivo {file_path} no existe. Se saltará.")
            continue
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    records_to_insert.extend(data)
                else:
                    records_to_insert.append(data)
            print(f"✅ Cargados {len(data)} registros de {file_path}")
        except Exception as e:
            print(f"❌ Error al leer {file_path}: {e}")

    if not records_to_insert:
        print("❌ No hay registros para importar.")
        return

    # Conectar a MongoDB
    try:
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['auth_source']}"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        
        db = client[MONGO_CONFIG['database']]
        collection = db[MONGO_CONFIG['collection']]
        
        # Opcional: Limpiar la colección antes de insertar para evitar duplicados si se vuelve a ejecutar
        # Por ahora, simplemente insertamos como se pidió "crear una colección"
        # Si ya existe, insertaremos a continuación.
        
        # Contar documentos actuales
        count_before = collection.count_documents({})
        if count_before > 0:
            print(f"ℹ️ La colección '{MONGO_CONFIG['collection']}' ya contiene {count_before} documentos.")
            # Podríamos borrarla para una carga limpia
            # collection.delete_many({})
            # print("🧹 Colección limpiada para carga fresca.")

        # Insertar registros
        result = collection.insert_many(records_to_insert)
        
        print(f"\n🚀 ¡Éxito! Se han insertado {len(result.inserted_ids)} registros en la colección '{MONGO_CONFIG['collection']}'.")
        print(f"📁 Total en DB: {collection.count_documents({})}")
        
    except ConnectionFailure:
        print("❌ Error: No se pudo conectar a MongoDB. Asegúrate de que el servicio esté corriendo.")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    importar_etfs()
