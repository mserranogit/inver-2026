import mstarpy as ms
from pymongo import MongoClient
import time

def refresh_links():
    client = MongoClient(
        host="localhost",
        port=27017,
        username="admin",
        password="mike",
        authSource="admin"
    )
    db = client["db-inver"]
    fondos_coll = db["fondos"]

    fondos = list(fondos_coll.find({}))
    total = len(fondos)
    print(f"Buscando Morningstar IDs (SecId) para {total} fondos...")

    updated_count = 0
    for i, fondo in enumerate(fondos):
        isin = fondo.get("isin")
        if not isin:
            continue
        
        if fondo.get("mstar_id"):
            continue

        try:
            # Quitamos country="es" que daba error
            f = ms.Funds(isin)
            mstar_id = f.code 
            
            if mstar_id:
                fondos_coll.update_one(
                    {"isin": isin},
                    {"$set": {"mstar_id": mstar_id}}
                )
                updated_count += 1
                if updated_count % 10 == 0:
                    print(f"Actualizados {updated_count} fondos...")
            
            time.sleep(0.25)
            
        except Exception as e:
            # Algunos fondos pueden no estar en Morningstar
            pass

    print(f"\nProceso finalizado. Se han actualizado {updated_count} fondos.")

if __name__ == "__main__":
    refresh_links()
