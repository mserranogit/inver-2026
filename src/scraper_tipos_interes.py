"""
Módulo de scraping de tipos de interés de bancos centrales.
Fuentes:
 - Tipos actuales: web scraping de global-rates.com
 - Previsiones: consensus de analistas (fuentes públicas: Goldman Sachs, ING, JP Morgan, etc.)
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, UTC
import re


# ============================================================
# CONFIGURACIÓN DE BANCOS CENTRALES
# ============================================================

BANCOS_CENTRALES = {
    "FED": {
        "nombre_completo": "Federal Reserve (FED)",
        "pais": "Estados Unidos",
        "moneda": "USD",
        "tipo_referencia": "Federal Funds Rate",
        "url_scraping": "https://www.global-rates.com/en/interest-rates/central-banks/central-bank-america/fed-interest-rate.aspx",
        "emoji": "🇺🇸",
    },
    "BCE": {
        "nombre_completo": "Banco Central Europeo (BCE)",
        "pais": "Zona Euro",
        "moneda": "EUR",
        "tipo_referencia": "Main Refinancing Rate",
        "url_scraping": "https://www.global-rates.com/en/interest-rates/central-banks/european-central-bank/ecb-interest-rate.aspx",
        "emoji": "🇪🇺",
    },
    "BOJ": {
        "nombre_completo": "Bank of Japan (BOJ)",
        "pais": "Japón",
        "moneda": "JPY",
        "tipo_referencia": "Overnight Call Rate",
        "url_scraping": "https://www.global-rates.com/en/interest-rates/central-banks/central-bank-japan/boj-interest-rate.aspx",
        "emoji": "🇯🇵",
    },
    "PBOC": {
        "nombre_completo": "People's Bank of China (PBOC)",
        "pais": "China",
        "moneda": "CNY",
        "tipo_referencia": "Loan Prime Rate (1Y)",
        "url_scraping": "https://www.global-rates.com/en/interest-rates/central-banks/central-bank-china/pbc-interest-rate.aspx",
        "emoji": "🇨🇳",
    },
}

# ============================================================
# PREVISIONES DE CONSENSO (fuentes públicas)
# Actualizadas: Feb 2026
# Fuentes: Goldman Sachs, ING, JP Morgan, Morningstar, 
#           BNP Paribas, Fund Society
# ============================================================

PREVISIONES_CONSENSO = {
    "FED": {
        "actual_fallback": 4.50,  # Rango 4.25-4.50
        "prev_1y": 3.50,   # Feb 2027 - Consenso: ~3.25-3.75
        "prev_2y": 3.00,   # Feb 2028 - Consenso: ~2.75-3.25
        "prev_3y": 2.75,   # Feb 2029 - Consenso: ~2.50-3.00 (tasa neutral)
        "fuente_previsiones": "Goldman Sachs, JP Morgan, ING (Feb 2026)",
    },
    "BCE": {
        "actual_fallback": 2.15,
        "prev_1y": 1.75,   # Feb 2027 - Consenso: ~1.50-2.00
        "prev_2y": 1.75,   # Feb 2028 - Consenso: ~1.50-2.00 (estable)
        "prev_3y": 2.00,   # Feb 2029 - Consenso: ~1.75-2.25
        "fuente_previsiones": "Morningstar, BNP Paribas, ECB Staff (Feb 2026)",
    },
    "BOJ": {
        "actual_fallback": 0.50,
        "prev_1y": 1.00,   # Feb 2027 - Consenso: ~0.75-1.25
        "prev_2y": 1.25,   # Feb 2028 - Consenso: ~1.00-1.50
        "prev_3y": 1.50,   # Feb 2029 - Consenso: ~1.25-1.75
        "fuente_previsiones": "ING, Julius Baer, Fund Society (Feb 2026)",
    },
    "PBOC": {
        "actual_fallback": 3.10,
        "prev_1y": 2.80,   # Feb 2027 - Consenso: ~2.60-3.00
        "prev_2y": 2.50,   # Feb 2028 - Consenso: ~2.30-2.70
        "prev_3y": 2.50,   # Feb 2029 - Consenso: ~2.30-2.70
        "fuente_previsiones": "ING, Goldman Sachs (Feb 2026)",
    },
}


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}


# ============================================================
# FUNCIONES DE SCRAPING
# ============================================================

def _extraer_tipo_global_rates(url: str) -> float | None:
    """
    Scrapea global-rates.com para obtener el tipo de interés actual.
    Busca el valor en la primera tabla de la página.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")

        # Buscar tablas con datos de tipos
        tables = soup.find_all("table", class_="tabledata1")
        if not tables:
            tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                for cell in cells:
                    text = cell.get_text(strip=True)
                    # Buscar patrones como "4.500 %" o "2.150%"
                    match = re.search(r"(\d+[\.,]\d+)\s*%", text)
                    if match:
                        valor = match.group(1).replace(",", ".")
                        return float(valor)

        # Buscar en todo el texto de la página como fallback
        page_text = soup.get_text()
        # Buscar "current ... rate" seguido de un porcentaje
        match = re.search(
            r"(?:current|actual|present).*?(\d+[\.,]\d+)\s*%",
            page_text,
            re.IGNORECASE
        )
        if match:
            return float(match.group(1).replace(",", "."))

        return None

    except Exception as e:
        print(f"[WARN] Error scraping {url}: {e}")
        return None


def obtener_datos_banco(codigo: str) -> dict:
    """
    Obtiene los datos completos de un banco central:
    tipo actual (scraping) + previsiones (consenso).
    """
    config = BANCOS_CENTRALES[codigo]
    previsiones = PREVISIONES_CONSENSO[codigo]

    # Intentar scraping del tipo actual
    tipo_actual = _extraer_tipo_global_rates(config["url_scraping"])
    tipo_scrapeado = tipo_actual is not None

    if tipo_actual is None:
        tipo_actual = previsiones["actual_fallback"]

    ahora = datetime.now(UTC)
    anno_actual = ahora.year

    return {
        "codigo": codigo,
        "nombre_completo": config["nombre_completo"],
        "pais": config["pais"],
        "moneda": config["moneda"],
        "emoji": config["emoji"],
        "tipo_referencia_nombre": config["tipo_referencia"],
        "tipo_actual": tipo_actual,
        "tipo_scrapeado": tipo_scrapeado,
        "previsiones": {
            f"{anno_actual + 1}": previsiones["prev_1y"],
            f"{anno_actual + 2}": previsiones["prev_2y"],
            f"{anno_actual + 3}": previsiones["prev_3y"],
        },
        "fuente_previsiones": previsiones["fuente_previsiones"],
        "fecha_consulta": ahora,
    }


def obtener_todos_los_bancos() -> list[dict]:
    """Obtiene datos de todos los bancos centrales configurados."""
    resultados = []
    for codigo in BANCOS_CENTRALES:
        datos = obtener_datos_banco(codigo)
        resultados.append(datos)
    return resultados


# ============================================================
# GUARDAR EN MONGODB
# ============================================================

def guardar_en_mongodb(db, datos: list[dict]) -> dict:
    """
    Guarda los datos de tipos de interés en la colección 
    'tipos_interes' de MongoDB.
    
    Retorna un dict con info del resultado.
    """
    collection = db["tipos_interes"]
    ahora = datetime.now(UTC)

    documento = {
        "fecha_consulta": ahora,
        "consulta_id": f"TI-{ahora.strftime('%Y%m%d-%H%M%S')}",
        "bancos": datos,
        "num_bancos": len(datos),
    }

    result = collection.insert_one(documento)

    return {
        "inserted_id": str(result.inserted_id),
        "consulta_id": documento["consulta_id"],
        "num_bancos": len(datos),
        "fecha": ahora,
    }


def obtener_ultimo_registro(db) -> dict | None:
    """Devuelve el último registro guardado en MongoDB."""
    collection = db["tipos_interes"]
    doc = collection.find_one(sort=[("_id", -1)])
    return doc


if __name__ == "__main__":
    # Test rápido
    print("Obteniendo datos de bancos centrales...")
    datos = obtener_todos_los_bancos()
    for d in datos:
        print(f"\n{d['emoji']} {d['nombre_completo']}")
        print(f"  Tipo actual: {d['tipo_actual']}%")
        print(f"  Scrapeado: {d['tipo_scrapeado']}")
        print(f"  Previsiones: {d['previsiones']}")
