"""
Módulo de scraping de curvas de tipos (rendimientos de bonos soberanos).
Fuentes:
 - Rendimientos actuales: web scraping de worldgovernmentbonds.com
 - Previsiones: consensus de analistas (Morningstar, CBO, ING, Oxford Economics, etc.)
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, UTC
import re


# ============================================================
# CONFIGURACIÓN: PAÍSES Y URLs DE SCRAPING
# ============================================================

PAISES = {
    "US": {
        "nombre": "Estados Unidos",
        "emoji": "🇺🇸",
        "moneda": "USD",
        "url": "https://www.worldgovernmentbonds.com/country/united-states/",
    },
    "EUR": {
        "nombre": "Zona Euro (Alemania)",
        "emoji": "🇪🇺",
        "moneda": "EUR",
        "url": "https://www.worldgovernmentbonds.com/country/germany/",
    },
    "JP": {
        "nombre": "Japón",
        "emoji": "🇯🇵",
        "moneda": "JPY",
        "url": "https://www.worldgovernmentbonds.com/country/japan/",
    },
    "CN": {
        "nombre": "China",
        "emoji": "🇨🇳",
        "moneda": "CNY",
        "url": "https://www.worldgovernmentbonds.com/country/china/",
    },
}


# ============================================================
# PLAZOS QUE NOS INTERESAN
# ============================================================
# Mapeo de nombres que aparecen en la web → nuestro identificador
PLAZOS_OBJETIVO = {
    "3 months":  "3M",
    "6 months":  "6M",
    "1 year":    "1Y",
    "2 years":   "2Y",
    "5 years":   "5Y",
    "10 years":  "10Y",
    "30 years":  "30Y",
}


# ============================================================
# PREVISIONES DE CONSENSO (fuentes públicas)
# Actualizadas: Feb 2026
# Fuentes: Morningstar, CBO/CEIC, ING, Oxford Economics,
#           Trading Economics, World Government Bonds
# ============================================================

PREVISIONES_CONSENSO = {
    "US": {
        "3M":  {"actual_fb": 4.30, "1y": 3.50, "2y": 3.00, "3y": 2.75},
        "6M":  {"actual_fb": 4.25, "1y": 3.45, "2y": 2.95, "3y": 2.70},
        "1Y":  {"actual_fb": 4.20, "1y": 3.40, "2y": 2.90, "3y": 2.65},
        "2Y":  {"actual_fb": 4.10, "1y": 3.50, "2y": 3.10, "3y": 2.90},
        "5Y":  {"actual_fb": 4.15, "1y": 3.60, "2y": 3.25, "3y": 3.00},
        "10Y": {"actual_fb": 4.50, "1y": 3.90, "2y": 3.50, "3y": 3.25},
        "30Y": {"actual_fb": 4.70, "1y": 4.20, "2y": 3.80, "3y": 3.50},
        "fuente": "Morningstar, CBO/CEIC, Schwab (Feb 2026)",
    },
    "EUR": {
        "3M":  {"actual_fb": 2.50, "1y": 2.00, "2y": 1.80, "3y": 1.80},
        "6M":  {"actual_fb": 2.45, "1y": 2.00, "2y": 1.80, "3y": 1.80},
        "1Y":  {"actual_fb": 2.30, "1y": 1.90, "2y": 1.75, "3y": 1.80},
        "2Y":  {"actual_fb": 2.15, "1y": 1.85, "2y": 1.75, "3y": 1.80},
        "5Y":  {"actual_fb": 2.30, "1y": 2.00, "2y": 1.90, "3y": 1.95},
        "10Y": {"actual_fb": 2.55, "1y": 2.20, "2y": 2.10, "3y": 2.15},
        "30Y": {"actual_fb": 2.80, "1y": 2.50, "2y": 2.40, "3y": 2.40},
        "fuente": "ECB Staff, Morningstar, Capital.com (Feb 2026)",
    },
    "JP": {
        "3M":  {"actual_fb": 0.40, "1y": 0.60, "2y": 0.80, "3y": 1.00},
        "6M":  {"actual_fb": 0.50, "1y": 0.70, "2y": 0.90, "3y": 1.05},
        "1Y":  {"actual_fb": 0.70, "1y": 0.85, "2y": 1.00, "3y": 1.10},
        "2Y":  {"actual_fb": 1.28, "1y": 1.08, "2y": 1.20, "3y": 1.40},
        "5Y":  {"actual_fb": 1.68, "1y": 1.50, "2y": 1.60, "3y": 1.80},
        "10Y": {"actual_fb": 2.21, "1y": 1.98, "2y": 2.30, "3y": 2.50},
        "30Y": {"actual_fb": 3.44, "1y": 3.00, "2y": 3.20, "3y": 3.40},
        "fuente": "ING, Oxford Economics, MUFG Research (Feb 2026)",
    },
    "CN": {
        "3M":  {"actual_fb": 1.10, "1y": 1.00, "2y": 0.90, "3y": 0.85},
        "6M":  {"actual_fb": 1.20, "1y": 1.10, "2y": 1.00, "3y": 0.95},
        "1Y":  {"actual_fb": 1.25, "1y": 1.15, "2y": 1.05, "3y": 1.00},
        "2Y":  {"actual_fb": 1.36, "1y": 1.28, "2y": 1.15, "3y": 1.10},
        "5Y":  {"actual_fb": 1.55, "1y": 1.47, "2y": 1.35, "3y": 1.30},
        "10Y": {"actual_fb": 1.79, "1y": 1.70, "2y": 1.55, "3y": 1.50},
        "30Y": {"actual_fb": 2.25, "1y": 2.15, "2y": 2.00, "3y": 1.95},
        "fuente": "Trading Economics, MacroMicro (Feb 2026)",
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

def _normalizar_plazo(texto: str) -> str | None:
    """Normaliza el texto de un plazo encontrado en la web."""
    t = texto.strip().lower()
    for clave, codigo in PLAZOS_OBJETIVO.items():
        if clave in t:
            return codigo
    # Patrones alternativos
    if re.search(r"\b3\s*m", t):
        return "3M"
    if re.search(r"\b6\s*m", t):
        return "6M"
    if re.search(r"\b1\s*y", t):
        return "1Y"
    if re.search(r"\b2\s*y", t):
        return "2Y"
    if re.search(r"\b5\s*y", t):
        return "5Y"
    if re.search(r"\b10\s*y", t):
        return "10Y"
    if re.search(r"\b30\s*y", t):
        return "30Y"
    return None


def _extraer_rendimiento(texto: str) -> float | None:
    """Extrae un valor porcentual de un texto."""
    # Buscar patrones como "4.500%", "2.15 %", "-0.10%"
    match = re.search(r"(-?\d+[\.,]\d+)\s*%", texto)
    if match:
        return float(match.group(1).replace(",", "."))
    # Sólo número
    match = re.search(r"(-?\d+[\.,]\d+)", texto)
    if match:
        return float(match.group(1).replace(",", "."))
    return None


def _scrape_yield_curve(url: str) -> dict[str, float]:
    """
    Scrapea worldgovernmentbonds.com para obtener la curva de tipos.
    Retorna dict con plazo -> rendimiento.
    """
    rendimientos = {}
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Buscar tablas con datos de rendimiento
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue

                # La primera celda suele ser el plazo, la segunda el yield
                plazo_text = cells[0].get_text(strip=True)
                plazo = _normalizar_plazo(plazo_text)

                if plazo:
                    # Buscar el rendimiento en las celdas restantes
                    for cell in cells[1:]:
                        valor = _extraer_rendimiento(cell.get_text(strip=True))
                        if valor is not None:
                            rendimientos[plazo] = valor
                            break

    except Exception as e:
        print(f"[WARN] Error scraping {url}: {e}")

    return rendimientos


def obtener_curva_pais(codigo: str) -> dict:
    """
    Obtiene curva de tipos completa para un país:
    rendimientos actuales (scraping) + previsiones (consenso).
    """
    config = PAISES[codigo]
    previsiones = PREVISIONES_CONSENSO[codigo]

    # Intentar scraping
    rendimientos_web = _scrape_yield_curve(config["url"])
    scrapeado = len(rendimientos_web) > 0

    ahora = datetime.now(UTC)
    anno_actual = ahora.year

    # Construir lista de plazos con datos
    plazos = []
    for plazo_codigo in ["3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y"]:
        prev_data = previsiones.get(plazo_codigo)
        if prev_data is None:
            continue

        # Rendimiento actual: primero scraping, luego fallback
        if plazo_codigo in rendimientos_web:
            actual = rendimientos_web[plazo_codigo]
            origen_actual = "scraping"
        else:
            actual = prev_data["actual_fb"]
            origen_actual = "fallback"

        plazos.append({
            "plazo": plazo_codigo,
            "rendimiento_actual": actual,
            "origen": origen_actual,
            "previsiones": {
                str(anno_actual + 1): prev_data["1y"],
                str(anno_actual + 2): prev_data["2y"],
                str(anno_actual + 3): prev_data["3y"],
            },
        })

    return {
        "codigo": codigo,
        "nombre": config["nombre"],
        "emoji": config["emoji"],
        "moneda": config["moneda"],
        "plazos": plazos,
        "scrapeado": scrapeado,
        "num_plazos_scrapeados": sum(1 for p in plazos if p["origen"] == "scraping"),
        "fuente_previsiones": previsiones["fuente"],
        "fecha_consulta": ahora,
    }


def obtener_todas_las_curvas() -> list[dict]:
    """Obtiene curvas de todos los países configurados."""
    resultados = []
    for codigo in PAISES:
        datos = obtener_curva_pais(codigo)
        resultados.append(datos)
    return resultados


# ============================================================
# GUARDAR EN MONGODB
# ============================================================

def guardar_curvas_en_mongodb(db, datos: list[dict]) -> dict:
    """
    Guarda los datos de curvas de tipos en la colección
    'curvas_tipos' de MongoDB.
    """
    collection = db["curvas_tipos"]
    ahora = datetime.now(UTC)

    documento = {
        "fecha_consulta": ahora,
        "consulta_id": f"CT-{ahora.strftime('%Y%m%d-%H%M%S')}",
        "paises": datos,
        "num_paises": len(datos),
    }

    result = collection.insert_one(documento)

    return {
        "inserted_id": str(result.inserted_id),
        "consulta_id": documento["consulta_id"],
        "num_paises": len(datos),
        "fecha": ahora,
    }


def obtener_ultimo_registro_curvas(db) -> dict | None:
    """Devuelve el último registro de curvas guardado en MongoDB."""
    collection = db["curvas_tipos"]
    doc = collection.find_one(sort=[("_id", -1)])
    return doc


if __name__ == "__main__":
    # Test rápido
    print("Obteniendo curvas de tipos...")
    datos = obtener_todas_las_curvas()
    for d in datos:
        print(f"\n{d['emoji']} {d['nombre']} (scrapeado: {d['scrapeado']})")
        for p in d["plazos"]:
            print(f"  {p['plazo']:>4s}: {p['rendimiento_actual']:.2f}% ({p['origen']}) | Prev: {p['previsiones']}")
