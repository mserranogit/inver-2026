import mstarpy as ms
import time
import random
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

def enrich_bond_data():
    """Enriquece los ETFs de renta fija con datos de duración y vencimiento de Morningstar"""
    try:
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['auth_source']}"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[MONGO_CONFIG['database']]
        collection = db[MONGO_CONFIG['collection']]
        
        # Filtramos por tipos que suelen ser Renta Fija o Mercado Monetario
        # También buscamos los que no tengan todavía 'duracion_efectiva'
        query = {
            "$and": [
                {"tipoEtf": {"$in": ["Mercado Monetario", "Renta Fija"]}},
                {"duracion_efectiva": {"$exists": False}}
            ]
        }
        
        etfs = list(collection.find(query))
        print(f"🔍 Encontrados {len(etfs)} ETFs de Renta Fija para enriquecer con datos de bonos.")
        
        count = 0
        for etf in etfs:
            isin = etf.get('isin')
            if not isin or isin == "N/A":
                continue
                
            print(f"[{count+1}/{len(etfs)}] Procesando {isin} - {etf.get('nombreEtf')}...")
            
            try:
                # Inicializar mstarpy
                f = ms.Funds(isin)
                fis = f.fixedIncomeStyle()
                
                if fis and 'fund' in fis:
                    info = fis['fund']
                    bond_data = {
                        "duracion_efectiva": info.get("avgEffectiveDuration"),
                        "duracion_modificada": info.get("modifiedDuration"),
                        "vencimiento_efectivo": info.get("avgEffectiveMaturity"),
                        "cupon_medio": info.get("avgCoupon"),
                        "yield_to_maturity": info.get("yieldToMaturity"),
                        "calidad_crediticia": info.get("avgCreditQualityName"),
                        "fecha_datos_bonos": info.get("portfolioDate")
                    }
                    
                    collection.update_one({"_id": etf["_id"]}, {"$set": bond_data})
                    print(f"   ✅ Datos de bonos actualizados (Duración: {bond_data['duracion_efectiva']})")
                else:
                    print(f"   ⚠️ No se encontraron detalles de renta fija para {isin}")
                    # Marcamos como procesado aunque no tenga datos para no repetir
                    collection.update_one({"_id": etf["_id"]}, {"$set": {"duracion_efectiva": None}})

            except Exception as e:
                print(f"   ❌ Error con mstarpy para {isin}: {e}")
                # En caso de error de conexión o API, podríamos esperar un poco más
                time.sleep(2)
            
            # Delay para no saturar la API de Morningstar
            time.sleep(random.uniform(1, 3))
            count += 1
                
        client.close()
        
    except ConnectionFailure:
        print("❌ Error: No se pudo conectar a MongoDB.")
    except Exception as e:
        print(f"❌ Error general: {e}")

if __name__ == "__main__":
    enrich_bond_data()
