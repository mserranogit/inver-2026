# ==========================================================
# ESTE SCRIPT CONSTRUYE COMPLETAMENTE LA COLECCIÓN "fondos"
# - Usa update_one(upsert=True)
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

def get_audit_collection():
    client = MongoClient(
        host=MONGO_CONFIG["host"],
        port=MONGO_CONFIG["port"],
        username=MONGO_CONFIG["username"],
        password=MONGO_CONFIG["password"],
        authSource=MONGO_CONFIG["auth_source"]
    )
    db = client[MONGO_CONFIG["database"]]
    return db["fondos_audit"]

# =========================
# HELPERS
# =========================
def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def classify_tipo_rf(category_name: str) -> str:
    if not category_name:
        return "Bonos"

    category = category_name.lower()

    # Money Market
    if "money market" in category:
        return "Letras"

    # Ultra / Short
    if "ultra short" in category:
        return "Bonos CP"

    if "short-term" in category or "short term" in category:
        return "Bonos CP"

    # Intermediate
    if "intermediate" in category:
        return "Bonos MP"

    # Long
    if "long-term" in category or "long term" in category:
        return "Bonos LP"

    # Government
    if "government" in category or "govt" in category:
        return "Bonos Gobierno"

    # Corporate
    if "corporate" in category:
        return "Bonos Corporativos"

    # High Yield
    if "high yield" in category:
        return "Bonos High Yield"

    return "Bonos"


def extract_duration_from_style(funds):
    """
    Extrae duration REAL desde fixedIncomeStyle()
    """
    duration_data = {
        "avg_effective_duration": None,
        "modified_duration": None,
        "avg_effective_maturity": None,
        "yield_to_maturity": None,
        "avg_credit_quality": None
    }

    category_duration_data = duration_data.copy()

    try:
        fi_style = funds.fixedIncomeStyle()

        if fi_style and fi_style.get("fund"):
            fund_data = fi_style["fund"]

            duration_data = {
                "avg_effective_duration": safe_float(fund_data.get("avgEffectiveDuration")),
                "modified_duration": safe_float(fund_data.get("modifiedDuration")),
                "avg_effective_maturity": safe_float(fund_data.get("avgEffectiveMaturity")),
                "yield_to_maturity": safe_float(fund_data.get("yieldToMaturity")),
                "avg_credit_quality": fund_data.get("avgCreditQualityName"),
            }

        if fi_style and fi_style.get("categoryAverage"):
            cat_data = fi_style["categoryAverage"]

            category_duration_data = {
                "avg_effective_duration": safe_float(cat_data.get("avgEffectiveDuration")),
                "modified_duration": safe_float(cat_data.get("modifiedDuration")),
                "avg_effective_maturity": safe_float(cat_data.get("avgEffectiveMaturity")),
                "yield_to_maturity": safe_float(cat_data.get("yieldToMaturity")),
                "avg_credit_quality": cat_data.get("avgCreditQualityName"),
            }

    except Exception as e:
        print(f"[WARNING] fixedIncomeStyle no disponible: {e}")

    return duration_data, category_duration_data


def classify_sensibilidad_por_duration(avg_effective_duration):
    if avg_effective_duration is None:
        return None

    if avg_effective_duration < 1:
        nivel = "muy_baja"
        descripcion = "Muy baja sensibilidad a movimientos de tipos de interés"
    elif avg_effective_duration < 3:
        nivel = "baja"
        descripcion = "Baja sensibilidad a movimientos de tipos de interés"
    elif avg_effective_duration < 5:
        nivel = "media"
        descripcion = "Sensibilidad moderada a movimientos de tipos de interés"
    elif avg_effective_duration < 7:
        nivel = "alta"
        descripcion = "Alta sensibilidad a movimientos de tipos de interés"
    else:
        nivel = "muy_alta"
        descripcion = "Muy alta sensibilidad a movimientos de tipos de interés"

    return {
        "nivel": nivel,
        "descripcion": descripcion,
        "fuente": "avg_effective_duration"
    }


# =========================
# PROCESAR FONDO
# =========================
def process_fondo(fondo, collection, audit_collection):

    start_time = time.time()

    isin = fondo.get("isin")
    nombre = fondo.get("nombre")

    print(f"🔍 Procesando {isin}")

    try:
        # -----------------------------
        # INSTANCIAR FONDS
        # -----------------------------
        funds = Funds(isin)

        # --- Allocation ---
        allocation_map = funds.allocationMap()
        category_name = allocation_map.get("categoryName", "")
        tipo_rf = classify_tipo_rf(category_name)

        # --- Performance ---
        perf_raw = funds.performanceTable()

        returns = {}
        table = perf_raw.get("table", {})
        columns = table.get("columnDefs", [])
        rows = table.get("growth10KReturnData", [])

        fund_row = next((r for r in rows if r.get("label") == "fund"), None)

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

        # --- Duration ---
        duration_data, category_duration_data = extract_duration_from_style(funds)

        sensibilidad_tipos = classify_sensibilidad_por_duration(
            duration_data.get("avg_effective_duration")
        )

        # --- Tramo RF ---
        duration_value = duration_data.get("avg_effective_duration")

        if duration_value is not None:
            if duration_value < 0.5:
                tramo_rf = "very_short"
            elif duration_value < 2:
                tramo_rf = "short"
            elif duration_value < 7:
                tramo_rf = "intermediate"
            else:
                tramo_rf = "long"
        else:
            category = allocation_map.get("categoryName", "").lower()
            if "money market" in category:
                tramo_rf = "very_short"
            elif "ultra short" in category or "short-term" in category:
                tramo_rf = "short"
            elif "intermediate" in category:
                tramo_rf = "intermediate"
            else:
                tramo_rf = "long"

        # -----------------------------
        # DOCUMENTO PRINCIPAL
        # -----------------------------
        doc = {
            "isin": isin,
            "nombre": nombre,
            "categoria": allocation_map.get("categoryName"),
            "tipo_rf": tipo_rf,
            "tramo_rf": tramo_rf,
            "sensibilidad_tipos": sensibilidad_tipos,
            "rentabilidad": {"historica": returns},
            "riesgo": risk_blocks,
            "duration": duration_data,
            "category_duration": category_duration_data,
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

        duration_exec = round(time.time() - start_time, 2)

        # -----------------------------
        # AUDITORÍA OK
        # -----------------------------
        audit_collection.insert_one({
            "isin": isin,
            "nombre": nombre,
            "status": "OK",
            "error": None,
            "execution_time_seconds": duration_exec,
            "timestamp": datetime.now(UTC)
        })

        print(f"📝 OK {isin} | {tipo_rf} | {tramo_rf}")

    except Exception as e:

        duration_exec = round(time.time() - start_time, 2)

        # -----------------------------
        # AUDITORÍA ERROR
        # -----------------------------
        audit_collection.insert_one({
            "isin": isin,
            "nombre": nombre,
            "status": "ERROR",
            "error": str(e),
            "execution_time_seconds": duration_exec,
            "timestamp": datetime.now(UTC)
        })

        print(f"❌ ERROR {isin}: {e}")


# =========================
# MAIN
# =========================
def main():

    collection = get_mongo_collection()
    audit_collection = get_audit_collection()

    with open("../../assets/json/fondos_open_R1.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    fondos = [f for f in data if isinstance(f, dict) and "isin" in f]

    for fondo in fondos:
        process_fondo(fondo, collection, audit_collection)
        time.sleep(2)


if __name__ == "__main__":
    main()
