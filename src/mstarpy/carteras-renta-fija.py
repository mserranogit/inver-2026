from pymongo import MongoClient
from datetime import datetime

# -----------------------------------
# CONFIG MONGO
# -----------------------------------
MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "username": "admin",
    "password": "mike",
    "database": "db-inver",
    "collection_fondos": "fondos",
    "collection_carteras": "carteras_fondos",
    "auth_source": "admin"
}

client = MongoClient(
    host=MONGO_CONFIG["host"],
    port=MONGO_CONFIG["port"],
    username=MONGO_CONFIG["username"],
    password=MONGO_CONFIG["password"],
    authSource=MONGO_CONFIG["auth_source"]
)

db = client[MONGO_CONFIG["database"]]
fondos_col = db[MONGO_CONFIG["collection_fondos"]]
carteras_col = db[MONGO_CONFIG["collection_carteras"]]

# -----------------------------------
# 1️⃣ CLASIFICACIÓN BONOS / LETRAS
# -----------------------------------
def tipo_renta_fija(categoria: str) -> str:
    if not categoria:
        return "Desconocido"

    cat = categoria.lower()

    if "money market" in cat or "short term" in cat:
        return "Letras"

    if "government" in cat or "aggregate" in cat:
        return "Bonos"

    return "Otros"


def normalizar_tipo_rf():
    """
    Añade el campo tipo_rf a todos los fondos
    """
    for f in fondos_col.find():
        tipo = tipo_renta_fija(f.get("categoria"))
        fondos_col.update_one(
            {"_id": f["_id"]},
            {"$set": {"tipo_rf": tipo}}
        )

# -----------------------------------
# 2️⃣ CONSULTAS BASE
# -----------------------------------
def obtener_bonos_invertibles(
    sharpe_min=0,
    drawdown_max=-0.10
):
    return list(fondos_col.find({
        "tipo_rf": "Bonos",
        "riesgo.sharpe_3y": {"$gt": sharpe_min},
        "riesgo.max_drawdown": {"$gt": drawdown_max}
    }))


def obtener_letras_conservadoras(
    drawdown_max=-0.02
):
    return list(fondos_col.find({
        "tipo_rf": "Letras",
        "riesgo.max_drawdown": {"$gt": drawdown_max}
    }))


def top_por_sharpe(tipo_rf, limit=5):
    return list(
        fondos_col.find({"tipo_rf": tipo_rf})
        .sort("riesgo.sharpe_3y", -1)
        .limit(limit)
    )

# -----------------------------------
# 3️⃣ CONSTRUCCIÓN DE CARTERA
# -----------------------------------
def construir_cartera(
    bonos,
    letras,
    peso_bonos=0.6
):
    cartera = []

    peso_letras = 1 - peso_bonos

    if bonos:
        peso_individual_bono = peso_bonos / len(bonos)
        for f in bonos:
            cartera.append({
                "isin": f["isin"],
                "nombre": f["nombre"],
                "tipo": "Bonos",
                "peso": round(peso_individual_bono, 4)
            })

    if letras:
        peso_individual_letra = peso_letras / len(letras)
        for f in letras:
            cartera.append({
                "isin": f["isin"],
                "nombre": f["nombre"],
                "tipo": "Letras",
                "peso": round(peso_individual_letra, 4)
            })

    return cartera

# -----------------------------------
# 4️⃣ GUARDAR CARTERA
# -----------------------------------
def guardar_cartera(
    nombre,
    cartera,
    peso_bonos
):
    doc = {
        "nombre": nombre,
        "bonos_pct": peso_bonos,
        "letras_pct": 1 - peso_bonos,
        "componentes": cartera,
        "created_at": datetime.utcnow()
    }

    carteras_col.insert_one(doc)

# -----------------------------------
# 5️⃣ EJEMPLO DE USO (OPCIONAL)
# -----------------------------------
if __name__ == "__main__":
    # Ejecutar UNA VEZ
    normalizar_tipo_rf()

    # Consultas
    bonos = top_por_sharpe("Bonos", limit=2)
    letras = top_por_sharpe("Letras", limit=2)

    # Construcción
    cartera = construir_cartera(
        bonos=bonos,
        letras=letras,
        peso_bonos=0.6
    )

    # Guardar
    guardar_cartera(
        nombre="RF Conservadora 60/40",
        cartera=cartera,
        peso_bonos=0.6
    )

    print("✅ Cartera creada y guardada")
