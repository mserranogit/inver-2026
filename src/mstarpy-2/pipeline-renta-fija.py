# ==========================================================
# ESTE SCRIPT CONSTRUYE COMPLETAMENTE LA COLECCIÓN "fondos"
# - Usa replaceOne(upsert=True)
# - NO es incremental
# - Si cambia el esquema → borrar colección y regenerar
# ==========================================================

import json
import time
from datetime import datetime, UTC

from pymongo import MongoClient
from mstarpy.funds import Funds


# =========================
# CONFIGURACIÓN MONGODB
# =========================
MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "username": "admin",
    "password": "mike",
    "database": "db-inver",
    "collection": "fondos",
    "auth_source": "admin"
}


# =========================
# CONEXIÓN MONGODB
# =========================
def get_mongo_collection():
    client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_source"]
    )

    db = client[MONGO_CONFIG["database"]]
    return db[MONGO_CONFIG["collection"]]


# =========================
# HELPERS
# =========================
def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def extract_duration(funds):
    """
    Duration / Effective Duration / Interest Rate Sensitivity
    Nunca rompe el pipeline
    """
    try:
        fi = funds.fixedIncomeStatistics()

        return {
            "duration": safe_float(fi.get("duration")),
            "effective_duration": safe_float(fi.get("effectiveDuration")),
            "interest_rate_sensitivity": safe_float(
                fi.get("interestRateSensitivity")
            )
        }
    except Exception:
        return {
            "duration": None,
            "effective_duration": None,
            "interest_rate_sensitivity": None
        }


def classify_sensibilidad(duration_data):
    """
    Clasificación de sensibilidad a tipos
    """
    d = (
        duration_data.get("effective_duration")
        or duration_data.get("duration")
        or duration_data.get("interest_rate_sensitivity")
    )

    if d is None:
        return None

    if d < 1:
        return "very_low"
    elif d < 3:
        return "low"
    elif d < 6:
        return "medium"
    else:
        return "high"


def classify_rf(allocation_map):
    """
    Clasificación RF usando allocationMap
    """
    try:
        bond_pct = safe_float(
            allocation_map["allocationMap"]["AssetAllocBond"]["netAllocation"]
        )

        if bond_pct is None:
            return "Desconocido", "unknown"

        if bond_pct < 30:
            return "Letras", "very_short"
        elif bond_pct < 70:
            return "Bonos CP", "short"
        else:
            return "Bonos LP", "long"

    except Exception:
        return "Desconocido", "unknown"

def calcular_sensibilidad_tipos(tramo_rf: str) -> dict:
    mapping = {
        "very_short": {
            "nivel": "muy_baja",
            "descripcion": "Sensibilidad muy baja a movimientos de tipos de interés"
        },
        "short": {
            "nivel": "baja",
            "descripcion": "Sensibilidad baja a movimientos de tipos de interés"
        },
        "medium": {
            "nivel": "media",
            "descripcion": "Sensibilidad media a movimientos de tipos de interés"
        },
        "long": {
            "nivel": "alta",
            "descripcion": "Alta sensibilidad a movimientos de tipos de interés"
        },
        "very_long": {
            "nivel": "muy_alta",
            "descripcion": "Muy alta sensibilidad a movimientos de tipos de interés"
        }
    }

    base = mapping.get(tramo_rf, {
        "nivel": "desconocida",
        "descripcion": "Sensibilidad a tipos no determinada"
    })

    return {
        **base,
        "fuente": "duration_estimada"
    }

# =========================
# PROCESAR FONDO
# =========================
def process_fondo(fondo, collection):
    isin = fondo["isin"]
    print(f"🔍 Procesando {isin}")

    funds = Funds(isin)

    # --- Allocation ---
    allocation_map = funds.allocationMap()
    tipo_rf, tramo_rf = classify_rf(allocation_map)


    # --- Performance ---
    perf_raw = funds.performanceTable()

    returns = {}
    table = perf_raw.get("table", {})
    columns = table.get("columnDefs", [])
    rows = table.get("growth10KReturnData", [])

    fund_row = next(
        (r for r in rows if r.get("label") == "fund"), None
    )

    if fund_row:
        for col, val in zip(columns, fund_row.get("datum", [])):
            returns[col] = safe_float(val)

    # --- Risk ---
    risk_raw = funds.riskVolatility()
    risk_blocks = {}

    fund_risk = risk_raw.get("fundRiskVolatility", {})

    for period in ["for1Year", "for3Year", "for5Year", "for10Year"]:
        data = fund_risk.get(period)
        if data:
            risk_blocks[period] = {
                "volatility": safe_float(data.get("standardDeviation")),
                "sharpe": safe_float(data.get("sharpeRatio"))
            }

    # --- Duration & Sensibilidad ---
    duration_data = extract_duration(funds)
    #sensibilidad_tipos = classify_sensibilidad(duration_data)
    sensibilidad_tipos = calcular_sensibilidad_tipos(tramo_rf)

    # =========================
    # DOCUMENTO FINAL
    # =========================
    doc = {
        "isin": isin,
        "nombre": fondo.get("nombre"),
        "categoria": allocation_map.get("categoryName"),

        "tipo_rf": tipo_rf,
        "tramo_rf": tramo_rf,
        "sensibilidad_tipos": sensibilidad_tipos,

        "rentabilidad": {
            "historica": returns
        },

        "riesgo": risk_blocks,
        "duration": duration_data,

        "allocation_map": allocation_map,
        "rentabilidad_raw": perf_raw,
        "riesgo_raw": risk_raw,

        "updated_at": datetime.now(UTC)
    }

    collection.update_one(
        {"isin": isin},
        {"$set": doc},
        upsert=True
    )

    print(f"📝 OK {isin} | {tipo_rf} | {tramo_rf}")


# =========================
# MAIN
# =========================
def main():
    collection = get_mongo_collection()

    with open("../../assets/json/fondos_prueba.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    fondos = [f for f in data if isinstance(f, dict) and "isin" in f]

    for fondo in fondos:
        try:
            process_fondo(fondo, collection)
            time.sleep(2)
        except Exception as e:
            print(f"❌ Error procesando {fondo}: {e}")


if __name__ == "__main__":
    main()
