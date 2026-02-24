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
import os

from src import previsiones_dinamicas

# Módulo de previsiones dinámicas (FRED + ECB + MoF JP + ChinaBond)
try:
    from src.previsiones_dinamicas import obtener_todas_las_previsiones, FALLBACK_ESTATICO
    _PREVISIONES_DINAMICAS_DISPONIBLES = True
except ImportError:
    _PREVISIONES_DINAMICAS_DISPONIBLES = False
    # Fallback estático embebido — usado si previsiones_dinamicas.py no está disponible
    FALLBACK_ESTATICO = {
        "US": {
            "3M":  {"actual_fb": 4.30, "1y": 3.50, "2y": 3.00, "3y": 2.75, "4y": 2.65, "5y": 2.60},
            "6M":  {"actual_fb": 4.25, "1y": 3.45, "2y": 2.95, "3y": 2.70, "4y": 2.60, "5y": 2.55},
            "1Y":  {"actual_fb": 4.20, "1y": 3.40, "2y": 2.90, "3y": 2.65, "4y": 2.55, "5y": 2.50},
            "2Y":  {"actual_fb": 4.10, "1y": 3.50, "2y": 3.10, "3y": 2.90, "4y": 2.80, "5y": 2.75},
            "5Y":  {"actual_fb": 4.15, "1y": 3.60, "2y": 3.25, "3y": 3.00, "4y": 2.90, "5y": 2.85},
            "10Y": {"actual_fb": 4.50, "1y": 3.90, "2y": 3.50, "3y": 3.25, "4y": 3.15, "5y": 3.10},
            "30Y": {"actual_fb": 4.70, "1y": 4.20, "2y": 3.80, "3y": 3.50, "4y": 3.40, "5y": 3.35},
        },
        "EUR": {
            "3M":  {"actual_fb": 2.50, "1y": 2.00, "2y": 1.80, "3y": 1.80, "4y": 1.78, "5y": 1.75},
            "6M":  {"actual_fb": 2.45, "1y": 2.00, "2y": 1.80, "3y": 1.80, "4y": 1.78, "5y": 1.75},
            "1Y":  {"actual_fb": 2.30, "1y": 1.90, "2y": 1.75, "3y": 1.80, "4y": 1.78, "5y": 1.75},
            "2Y":  {"actual_fb": 2.15, "1y": 1.85, "2y": 1.75, "3y": 1.80, "4y": 1.78, "5y": 1.75},
            "5Y":  {"actual_fb": 2.30, "1y": 2.00, "2y": 1.90, "3y": 1.95, "4y": 1.93, "5y": 1.90},
            "10Y": {"actual_fb": 2.55, "1y": 2.20, "2y": 2.10, "3y": 2.15, "4y": 2.13, "5y": 2.10},
            "30Y": {"actual_fb": 2.80, "1y": 2.50, "2y": 2.40, "3y": 2.40, "4y": 2.38, "5y": 2.35},
        },
        "JP": {
            "3M":  {"actual_fb": 0.40, "1y": 0.60, "2y": 0.80, "3y": 1.00, "4y": 1.10, "5y": 1.20},
            "6M":  {"actual_fb": 0.50, "1y": 0.70, "2y": 0.90, "3y": 1.05, "4y": 1.15, "5y": 1.25},
            "1Y":  {"actual_fb": 0.70, "1y": 0.85, "2y": 1.00, "3y": 1.10, "4y": 1.20, "5y": 1.30},
            "2Y":  {"actual_fb": 1.28, "1y": 1.08, "2y": 1.20, "3y": 1.40, "4y": 1.50, "5y": 1.60},
            "5Y":  {"actual_fb": 1.68, "1y": 1.50, "2y": 1.60, "3y": 1.80, "4y": 1.90, "5y": 2.00},
            "10Y": {"actual_fb": 2.21, "1y": 1.98, "2y": 2.30, "3y": 2.50, "4y": 2.60, "5y": 2.70},
            "30Y": {"actual_fb": 3.44, "1y": 3.00, "2y": 3.20, "3y": 3.40, "4y": 3.50, "5y": 3.60},
        },
        "CN": {
            "3M":  {"actual_fb": 1.10, "1y": 1.00, "2y": 0.90, "3y": 0.85, "4y": 0.83, "5y": 0.80},
            "6M":  {"actual_fb": 1.20, "1y": 1.10, "2y": 1.00, "3y": 0.95, "4y": 0.92, "5y": 0.90},
            "1Y":  {"actual_fb": 1.25, "1y": 1.15, "2y": 1.05, "3y": 1.00, "4y": 0.97, "5y": 0.95},
            "2Y":  {"actual_fb": 1.36, "1y": 1.28, "2y": 1.15, "3y": 1.10, "4y": 1.07, "5y": 1.05},
            "5Y":  {"actual_fb": 1.55, "1y": 1.47, "2y": 1.35, "3y": 1.30, "4y": 1.27, "5y": 1.25},
            "10Y": {"actual_fb": 1.79, "1y": 1.70, "2y": 1.55, "3y": 1.50, "4y": 1.47, "5y": 1.45},
            "30Y": {"actual_fb": 2.25, "1y": 2.15, "2y": 2.00, "3y": 1.95, "4y": 1.92, "5y": 1.90},
        },
    }

    def obtener_todas_las_previsiones(fred_api_key: str = "") -> dict:
        """Stub usado cuando previsiones_dinamicas.py no está disponible."""
        return {}

# Clave FRED (desde variable de entorno o hardcodeada aquí)
FRED_API_KEY = os.getenv("FRED_API_KEY", "")


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
# PREVISIONES DE CONSENSO
# Las previsiones ahora son DINÁMICAS para US (FRED) y EUR (ECB).
# JP y CN usan fallback estático (sin API pública gratuita fiable).
# Ver módulo previsiones_dinamicas.py para el detalle.
# ============================================================


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


def obtener_curva_pais(codigo: str, _previsiones_cache: dict = None) -> dict:
    """
    Obtiene curva de tipos completa para un país:
    rendimientos actuales (scraping de worldgovernmentbonds) +
    previsiones dinámicas (FRED para US, ECB para EUR, estático para JP/CN).

    El parámetro _previsiones_cache permite reutilizar previsiones ya
    obtenidas (evita llamadas repetidas a las APIs en obtener_todas_las_curvas).
    """
    config = PAISES[codigo]

    # Obtener previsiones dinámicas (o usar caché si se pasó)
    if _previsiones_cache and codigo in _previsiones_cache:
        prev_dinamicas = _previsiones_cache[codigo]
    elif _PREVISIONES_DINAMICAS_DISPONIBLES:
        from previsiones_dinamicas import obtener_previsiones
        prev_dinamicas = obtener_previsiones(codigo, fred_api_key=FRED_API_KEY)
    else:
        # Sin módulo de previsiones dinámicas: usar fallback estático
        fb = FALLBACK_ESTATICO[codigo]
        anno = datetime.now(UTC).year
        prev_dinamicas = {
            "pais": codigo,
            "yields_actuales": {k: v["actual_fb"] for k, v in fb.items()},
            "previsiones": {
                k: {str(anno + i): v[f"{i}y"] for i in range(1, 6)}
                for k, v in fb.items()
            },
            "calidad": "degradado",
            "fuente": "Fallback estático — módulo previsiones_dinamicas.py no encontrado",
            "detalle_calidad": "Instala previsiones_dinamicas.py en el mismo directorio.",
        }

    # Intentar scraping de yields actuales
    rendimientos_web = _scrape_yield_curve(config["url"])
    scrapeado = len(rendimientos_web) > 0

    ahora = datetime.now(UTC)
    anno_actual = ahora.year

    # Combinar: yields de scraping (si disponibles) + previsiones dinámicas
    plazos = []
    for plazo_codigo in ["3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y"]:

        # Rendimiento actual: scraping > API dinámica > fallback estático
        if plazo_codigo in rendimientos_web:
            actual = rendimientos_web[plazo_codigo]
            origen_actual = "scraping"
        elif plazo_codigo in prev_dinamicas.get("yields_actuales", {}):
            actual = prev_dinamicas["yields_actuales"][plazo_codigo]
            origen_actual = f"api_{prev_dinamicas.get('calidad', 'desconocido')}"
        else:
            continue   # plazo sin dato, omitir

        # Previsiones dinámicas para este plazo
        prev_plazo = prev_dinamicas.get("previsiones", {}).get(plazo_codigo, {})

        plazos.append({
            "plazo": plazo_codigo,
            "rendimiento_actual": actual,
            "origen": origen_actual,
            "previsiones": prev_plazo,
            "horizonte_prevision": f"{anno_actual + 1}–{anno_actual + 5}",
        })

    return {
        "codigo": codigo,
        "nombre": config["nombre"],
        "emoji": config["emoji"],
        "moneda": config["moneda"],
        "plazos": plazos,
        "scrapeado": scrapeado,
        "num_plazos_scrapeados": sum(1 for p in plazos if p["origen"] == "scraping"),
        "calidad_previsiones": prev_dinamicas.get("calidad", "desconocido"),
        "fuente_previsiones": prev_dinamicas.get("fuente", ""),
        "detalle_calidad": prev_dinamicas.get("detalle_calidad", ""),
        "metodo_prevision": prev_dinamicas.get("metodo_prevision", ""),
        "fecha_consulta": ahora,
    }


def obtener_todas_las_curvas() -> list[dict]:
    """
    Obtiene curvas de todos los países configurados.
    Las llamadas a FRED y ECB se hacen una sola vez y se comparten
    entre países para evitar llamadas redundantes.
    """
    # Obtener todas las previsiones dinámicas de una vez
    cache_previsiones = {}
    if _PREVISIONES_DINAMICAS_DISPONIBLES:
        try:
            cache_previsiones = obtener_todas_las_previsiones(fred_api_key=FRED_API_KEY)
        except Exception as e:
            print(f"[WARN] Error obteniendo previsiones dinámicas: {e}")

    resultados = []
    for codigo in PAISES:
        datos = obtener_curva_pais(codigo, _previsiones_cache=cache_previsiones)
        resultados.append(datos)
    return resultados


# ============================================================
# GUARDAR EN MONGODB
# ============================================================

def guardar_curvas_en_mongodb(db, datos: list[dict]) -> dict:
    """
    Guarda los datos de curvas de tipos en la colección
    'curvas_tipos' de MongoDB.
    Incluye resumen de calidad de previsiones por país.
    """
    collection = db["curvas_tipos"]
    ahora = datetime.now(UTC)

    resumen_calidad = {
        d["codigo"]: {
            "calidad": d.get("calidad_previsiones", "desconocido"),
            "fuente": d.get("fuente_previsiones", ""),
            "metodo": d.get("metodo_prevision", ""),
        }
        for d in datos
    }

    documento = {
        "fecha_consulta": ahora,
        "consulta_id": f"CT-{ahora.strftime('%Y%m%d-%H%M%S')}",
        "paises": datos,
        "num_paises": len(datos),
        "resumen_calidad": resumen_calidad,
        "tiene_datos_degradados": any(
            d.get("calidad_previsiones") == "degradado" for d in datos
        ),
    }

    result = collection.insert_one(documento)

    return {
        "inserted_id": str(result.inserted_id),
        "consulta_id": documento["consulta_id"],
        "num_paises": len(datos),
        "fecha": ahora,
        "resumen_calidad": resumen_calidad,
    }


def obtener_ultimo_registro_curvas(db) -> dict | None:
    """Devuelve el último registro de curvas guardado en MongoDB."""
    collection = db["curvas_tipos"]
    doc = collection.find_one(sort=[("_id", -1)])
    return doc


if __name__ == "__main__":
    print("Obteniendo curvas de tipos...")
    datos = obtener_todas_las_curvas()
    for d in datos:
        calidad = d.get("calidad_previsiones", "?")
        print(f"\n{d['emoji']} {d['nombre']} | scraping: {d['scrapeado']} | previsiones: {calidad.upper()}")
        print(f"   Fuente previsiones: {d.get('fuente_previsiones', '')}")
        if calidad != "ok":
            print(f"   ⚠️  {d.get('detalle_calidad', '')}")
        for p in d["plazos"]:
            print(f"  {p['plazo']:>4s}: {p['rendimiento_actual']:.2f}% ({p['origen']}) | Prev {p['horizonte_prevision']}: {p['previsiones']}")
