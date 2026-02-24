"""
Módulo de previsiones dinámicas de curvas de tipos.

Fuentes:
  - US  → FRED API (Federal Reserve Bank of St. Louis) — oficial y gratuita
  - EUR → ECB Statistical Data Warehouse API            — oficial y gratuita
  - JP  → fallback estático (no hay API pública gratuita fiable)
  - CN  → fallback estático (no hay API pública gratuita fiable)

Metodología de previsiones:
  A partir de los yields actuales de cada vencimiento se calculan los
  forward rates implícitos usando la teoría de las expectativas puras.
  Es decir: el mercado ya está "descontando" los tipos futuros en la
  pendiente de la curva actual. Este método es estándar en análisis
  de renta fija y produce previsiones coherentes con la curva de mercado.

  Para los plazos cortos (3M, 6M, 1Y) se aplica adicionalmente una
  convergencia suavizada hacia el tipo neutral estimado de cada economía,
  ya que los forwards implícitos a corto plazo son muy volátiles.

Calidad de datos:
  Cada resultado incluye el campo "calidad":
    - "ok"        → datos obtenidos de la API correctamente
    - "degradado" → API falló, se usan valores estáticos de fallback
    - "estatico"  → país sin API dinámica (JP, CN), siempre estático
"""

import requests
from datetime import datetime, UTC, date
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURACIÓN DE APIs
# ============================================================

# FRED API — registro gratuito en https://fred.stlouisfed.org/docs/api/api_key.html
# Si no tienes clave, el módulo funciona en modo degradado con fallback
FRED_API_KEY = ""   # ← pon aquí tu clave FRED (o en variable de entorno)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Series FRED para yields del Tesoro USA (yields diarios)
FRED_SERIES_US = {
    "3M":  "DGS3MO",
    "6M":  "DGS6MO",
    "1Y":  "DGS1",
    "2Y":  "DGS2",
    "5Y":  "DGS5",
    "10Y": "DGS10",
    "30Y": "DGS30",
}

# ECB SDW API — sin autenticación, acceso libre
ECB_BASE_URL = "https://data-api.ecb.europa.eu/service/data"

# Series ECB para curva AAA zona euro (proxy Alemania)
# Dataset YC: yield curves estimadas por el BCE (Svensson)
ECB_SERIES_EUR = {
    "3M":  "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3M",
    "6M":  "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_6M",
    "1Y":  "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y",
    "2Y":  "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    "5Y":  "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y",
    "10Y": "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
    "30Y": "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_30Y",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 financial-data-scraper/1.0",
    "Accept": "application/json",
}

TIMEOUT = 15  # segundos


# ============================================================
# TIPOS NEUTRALES ESTIMADOS (largo plazo estructural)
# Usados para suavizar previsiones en plazos cortos
# ============================================================
TIPO_NEUTRAL = {
    "US":  2.75,   # Fed neutral rate estimado (dot plot mediano largo plazo)
    "EUR": 2.00,   # BCE neutral rate estimado (Schnabel et al.)
    "JP":  1.00,   # BoJ gradual normalización
    "CN":  1.50,   # PBOC entorno actual
}


# ============================================================
# FALLBACK ESTÁTICO (Feb 2026) — usado si APIs fallan
# ============================================================
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

# Duración en años de cada código de plazo
DURACION_ANOS = {
    "3M": 0.25, "6M": 0.50, "1Y": 1.0,
    "2Y": 2.0,  "5Y": 5.0,  "10Y": 10.0, "30Y": 30.0,
}


# ============================================================
# CÁLCULO DE FORWARD RATES IMPLÍCITOS
# ============================================================

def _calcular_forwards_implicitos(
    yields_actuales: dict[str, float],
    tipo_neutral: float,
    anno_base: int,
) -> dict[str, dict]:
    """
    Para cada plazo, calcula los forwards implícitos en +1, +2, +3, +4, +5 años
    usando la teoría de las expectativas puras con suavizado hacia el tipo neutral.

    Fórmula forward rate implícita entre t1 y t2:
        f(t1, t2) = [(1 + r2)^t2 / (1 + r1)^t1]^(1/(t2-t1)) - 1

    Para previsiones anuales (cada +1 año desde hoy), se usa el forward
    spot a 1Y calculado desplazando la curva.

    El suavizado mezcla el forward puro con el tipo neutral según
    el horizonte: a mayor horizonte, más peso al neutral (mean reversion).
    """
    plazos_ord = sorted(
        [(k, v) for k, v in yields_actuales.items() if k in DURACION_ANOS],
        key=lambda x: DURACION_ANOS[x[0]]
    )

    if not plazos_ord:
        return {}

    # Construir curva spot continua como dict {duracion: yield}
    curva = {DURACION_ANOS[k]: v / 100 for k, v in plazos_ord}

    def spot_interpolado(t: float) -> float:
        """Interpolación lineal del yield spot para cualquier vencimiento t."""
        puntos = sorted(curva.keys())
        if t <= puntos[0]:
            return curva[puntos[0]]
        if t >= puntos[-1]:
            return curva[puntos[-1]]
        for i in range(len(puntos) - 1):
            t1, t2 = puntos[i], puntos[i + 1]
            if t1 <= t <= t2:
                w = (t - t1) / (t2 - t1)
                return curva[t1] * (1 - w) + curva[t2] * w
        return curva[puntos[-1]]

    def forward_1y_en(inicio: float) -> float:
        """
        Calcula el forward rate a 1 año empezando en 'inicio' años desde hoy.
        f = [(1+r(t+1))^(t+1) / (1+r(t))^t] - 1
        """
        r_fin = spot_interpolado(inicio + 1.0)
        r_ini = spot_interpolado(inicio) if inicio > 0 else r_fin * 0.0
        if inicio == 0:
            return r_fin
        return ((1 + r_fin) ** (inicio + 1) / (1 + r_ini) ** inicio) - 1

    def suavizar(forward_puro: float, horizonte_anos: int) -> float:
        """
        Mezcla el forward puro con el tipo neutral.
        Peso del neutral crece con el horizonte (mean reversion parcial).
        horizonte 1 → 15% neutral, horizonte 5 → 40% neutral
        """
        peso_neutral = min(0.10 * horizonte_anos + 0.05, 0.40)
        neutral = tipo_neutral / 100
        return forward_puro * (1 - peso_neutral) + neutral * peso_neutral

    resultado = {}
    for codigo, _ in plazos_ord:
        duracion = DURACION_ANOS[codigo]
        prevs = {}
        for h in range(1, 6):  # +1 a +5 años
            fwd_puro = forward_1y_en(duracion + h - 1)
            fwd_suav = suavizar(fwd_puro, h)
            prevs[str(anno_base + h)] = round(fwd_suav * 100, 2)
        resultado[codigo] = prevs

    return resultado


# ============================================================
# OBTENCIÓN DE YIELDS DESDE FRED (US)
# ============================================================

def _obtener_yields_fred(api_key: str) -> tuple[dict[str, float], str]:
    """
    Obtiene los yields más recientes del Tesoro USA desde FRED.
    Retorna (dict plazo→yield, mensaje_estado).
    """
    if not api_key:
        return {}, "Sin clave FRED API — usando fallback"

    yields = {}
    errores = []

    for plazo, serie in FRED_SERIES_US.items():
        try:
            params = {
                "series_id": serie,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 5,          # últimas 5 observaciones para evitar huecos
                "observation_start": "2020-01-01",
            }
            resp = requests.get(FRED_BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            # Buscar el valor más reciente que no sea "." (dato no disponible)
            for obs in data.get("observations", []):
                if obs["value"] != ".":
                    yields[plazo] = float(obs["value"])
                    break

        except Exception as e:
            errores.append(f"{plazo}: {e}")
            logger.warning(f"FRED error en serie {serie}: {e}")

    estado = "ok" if len(yields) >= 5 else (
        "degradado" if yields else "sin_datos"
    )
    if errores:
        logger.warning(f"FRED errores parciales: {errores}")

    return yields, estado


# ============================================================
# OBTENCIÓN DE YIELDS DESDE ECB SDW (EUR)
# ============================================================

def _obtener_yields_ecb() -> tuple[dict[str, float], str]:
    """
    Obtiene los yields más recientes de la curva AAA zona euro desde el BCE.
    Retorna (dict plazo→yield, mensaje_estado).
    """
    yields = {}
    errores = []

    for plazo, serie in ECB_SERIES_EUR.items():
        try:
            url = f"{ECB_BASE_URL}/{serie}"
            params = {
                "format": "jsondata",
                "lastNObservations": 5,   # últimas 5 por si hay huecos
                "detail": "dataonly",
            }
            resp = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            # Navegar estructura SDMX-JSON del BCE
            datasets = data.get("dataSets", [])
            if datasets:
                series_data = datasets[0].get("series", {})
                if series_data:
                    # La primera (y única) serie
                    first_series = next(iter(series_data.values()))
                    observations = first_series.get("observations", {})
                    # Ordenar por índice descendente y tomar el primero no nulo
                    for idx in sorted(observations.keys(), key=int, reverse=True):
                        valor = observations[idx][0]
                        if valor is not None:
                            yields[plazo] = round(float(valor), 4)
                            break

        except Exception as e:
            errores.append(f"{plazo}: {e}")
            logger.warning(f"ECB SDW error en serie {serie}: {e}")

    estado = "ok" if len(yields) >= 5 else (
        "degradado" if yields else "sin_datos"
    )
    if errores:
        logger.warning(f"ECB errores parciales: {errores}")

    return yields, estado


# ============================================================
# OBTENCIÓN DE YIELDS DESDE MOF JAPÓN (JP)
# ============================================================

# URL del CSV oficial del Ministerio de Finanzas de Japón
MOF_JP_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"

# Mapeo cabeceras del CSV → nuestros códigos de plazo
# Las cabeceras reales son: "1Y","2Y","5Y","10Y","20Y","30Y","40Y"
# (no publican 3M ni 6M directamente; para esos usamos 1Y como aproximación)
MOF_JP_COLUMNAS = {
    "1Y":  "1Y",
    "2Y":  "2Y",
    "5Y":  "5Y",
    "10Y": "10Y",
    "20Y": None,   # no necesitamos 20Y
    "30Y": "30Y",
    "40Y": None,
}

def _obtener_yields_mof_jp() -> tuple[dict[str, float], str]:
    """
    Descarga el CSV oficial del Ministerio de Finanzas de Japón con
    los yields de referencia de JGBs (Japanese Government Bonds).

    El CSV tiene formato:
        Date,1Y,2Y,5Y,10Y,20Y,30Y,40Y
        2026/02/19,0.72,1.28,1.68,2.14,...

    Retorna (dict plazo→yield, estado).
    Nota: MoF no publica 3M ni 6M. Se estiman por interpolación.
    """
    try:
        resp = requests.get(MOF_JP_URL, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()

        # El CSV puede tener cabeceras con BOM o espacios — limpiar
        lineas = resp.text.strip().splitlines()

        # Encontrar la fila de cabeceras (buscar la que contiene "1Y" o "Date")
        idx_header = None
        for i, linea in enumerate(lineas):
            if "1Y" in linea or "Date" in linea.strip():
                idx_header = i
                break

        if idx_header is None:
            return {}, "sin_datos"

        cabeceras = [c.strip().strip('"').strip("'") for c in lineas[idx_header].split(",")]

        # Tomar la última fila con datos (la más reciente)
        datos_fila = None
        for linea in reversed(lineas[idx_header + 1:]):
            partes = [p.strip().strip('"') for p in linea.split(",")]
            if len(partes) >= 2 and partes[0] and partes[1]:
                try:
                    float(partes[1])  # verificar que hay un número
                    datos_fila = partes
                    break
                except ValueError:
                    continue

        if not datos_fila:
            return {}, "sin_datos"

        yields = {}
        for cabecera, codigo in MOF_JP_COLUMNAS.items():
            if codigo is None:
                continue
            try:
                idx = cabeceras.index(cabecera)
                valor = datos_fila[idx].strip()
                if valor and valor != "-":
                    yields[codigo] = float(valor)
            except (ValueError, IndexError):
                continue

        # Estimar 3M y 6M por extrapolación desde 1Y (pendiente negativa usual en JP)
        # La curva corta JP suele estar ~0.5–0.8% por debajo del 1Y
        if "1Y" in yields and "3M" not in yields:
            yields["3M"] = round(max(0.0, yields["1Y"] - 0.30), 4)
        if "1Y" in yields and "6M" not in yields:
            yields["6M"] = round(max(0.0, yields["1Y"] - 0.18), 4)

        estado = "ok" if len(yields) >= 5 else (
            "degradado" if yields else "sin_datos"
        )
        return yields, estado

    except Exception as e:
        logger.warning(f"MoF JP error: {e}")
        return {}, f"error: {e}"


# ============================================================
# OBTENCIÓN DE YIELDS DESDE CHINABOND (CN)
# ============================================================

CHINABOND_URL = "https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/more?locale=en_US"

# Mapeo de cabeceras de la tabla HTML → nuestros códigos
# La tabla publica: 3M, 6M, 1Y, 3Y, 5Y, 7Y, 10Y, 30Y
CHINABOND_COLUMNAS = {
    "3M":  "3M",
    "6M":  "6M",
    "1Y":  "1Y",
    "3Y":  None,   # no necesitamos 3Y
    "5Y":  "5Y",
    "7Y":  None,
    "10Y": "10Y",
    "30Y": "30Y",
}

def _obtener_yields_chinabond() -> tuple[dict[str, float], str]:
    """
    Scrapea la página oficial de ChinaBond (CCDC) para obtener los yields
    de la curva de bonos soberanos chinos (ChinaBond Government Bond Yield Curve).

    La página publica una tabla HTML con la curva actualizada diariamente
    a las 17:30h hora de Pekín. Tiene versión en inglés.

    Nota: no publica 2Y directamente; se estima por interpolación 1Y–5Y.
    """
    try:
        resp = requests.get(CHINABOND_URL, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")

        # Buscar la tabla principal
        tabla = soup.find("table")
        if not tabla:
            return {}, "sin_datos"

        filas = tabla.find_all("tr")
        if len(filas) < 2:
            return {}, "sin_datos"

        # Primera fila: cabeceras con los plazos
        cabeceras_raw = [th.get_text(strip=True) for th in filas[0].find_all(["th", "td"])]

        # Buscar la fila de "ChinaBond Government Bond Yield Curve" (bonos soberanos)
        fila_datos = None
        for fila in filas[1:]:
            celdas = fila.find_all(["th", "td"])
            if celdas and "Government Bond" in celdas[0].get_text():
                fila_datos = [c.get_text(strip=True) for c in celdas]
                break

        if not fila_datos:
            # Si no encontramos la fila exacta, tomar la primera fila de datos
            celdas = filas[1].find_all(["th", "td"])
            fila_datos = [c.get_text(strip=True) for c in celdas]

        yields = {}
        for plazo_web, codigo in CHINABOND_COLUMNAS.items():
            if codigo is None:
                continue
            try:
                # Buscar la columna por cabecera
                for i, cab in enumerate(cabeceras_raw):
                    if plazo_web in cab:
                        valor_str = fila_datos[i].replace("%", "").strip()
                        if valor_str and valor_str not in ("-", "N/A", ""):
                            yields[codigo] = round(float(valor_str), 4)
                        break
            except (ValueError, IndexError):
                continue

        # Estimar 2Y por interpolación lineal entre 1Y y 5Y
        if "1Y" in yields and "5Y" in yields and "2Y" not in yields:
            yields["2Y"] = round(yields["1Y"] + (yields["5Y"] - yields["1Y"]) * 0.25, 4)

        estado = "ok" if len(yields) >= 4 else (
            "degradado" if yields else "sin_datos"
        )
        return yields, estado

    except Exception as e:
        logger.warning(f"ChinaBond error: {e}")
        return {}, f"error: {e}"


# ============================================================
# FUNCIÓN PRINCIPAL: OBTENER PREVISIONES DINÁMICAS
# ============================================================

def obtener_previsiones(
    codigo_pais: str,
    fred_api_key: str = "",
    anno_base: Optional[int] = None,
) -> dict:
    """
    Obtiene previsiones dinámicas de la curva de tipos para un país.

    Retorna un dict con:
      - yields_actuales: dict plazo → yield actual (de la API)
      - previsiones:     dict plazo → dict año → yield previsto
      - calidad:         "ok" | "degradado" | "estatico"
      - fuente:          descripción de la fuente usada
      - fecha_consulta:  timestamp UTC
      - detalle_calidad: mensaje explicativo
    """
    ahora = datetime.now(UTC)
    if anno_base is None:
        anno_base = ahora.year

    # --- US: FRED ---
    if codigo_pais == "US":
        yields, estado_api = _obtener_yields_fred(fred_api_key or FRED_API_KEY)

        if estado_api == "ok":
            previsiones = _calcular_forwards_implicitos(yields, TIPO_NEUTRAL["US"], anno_base)
            return {
                "pais": "US",
                "yields_actuales": yields,
                "previsiones": previsiones,
                "calidad": "ok",
                "fuente": "FRED API (Federal Reserve Bank of St. Louis) + forward rates implícitos",
                "metodo_prevision": "forward_rates_implicitos_suavizados",
                "fecha_consulta": ahora,
                "detalle_calidad": f"Yields obtenidos de FRED. {len(yields)}/7 plazos disponibles.",
            }
        else:
            # Fallback estático
            fb = FALLBACK_ESTATICO["US"]
            yields_fb = {k: v["actual_fb"] for k, v in fb.items()}
            prevs_fb = {k: {str(anno_base + i): v[f"{i}y"] for i in range(1, 6)} for k, v in fb.items()}
            return {
                "pais": "US",
                "yields_actuales": yields_fb,
                "previsiones": prevs_fb,
                "calidad": "degradado",
                "fuente": "Fallback estático (Feb 2026) — FRED API no disponible",
                "metodo_prevision": "estatico_fallback",
                "fecha_consulta": ahora,
                "detalle_calidad": f"FRED API falló ({estado_api}). Datos estáticos de Feb 2026.",
            }

    # --- EUR: ECB SDW ---
    elif codigo_pais == "EUR":
        yields, estado_api = _obtener_yields_ecb()

        if estado_api == "ok":
            previsiones = _calcular_forwards_implicitos(yields, TIPO_NEUTRAL["EUR"], anno_base)
            return {
                "pais": "EUR",
                "yields_actuales": yields,
                "previsiones": previsiones,
                "calidad": "ok",
                "fuente": "ECB Statistical Data Warehouse (curva AAA zona euro) + forward rates implícitos",
                "metodo_prevision": "forward_rates_implicitos_suavizados",
                "fecha_consulta": ahora,
                "detalle_calidad": f"Yields obtenidos del BCE. {len(yields)}/7 plazos disponibles.",
            }
        else:
            fb = FALLBACK_ESTATICO["EUR"]
            yields_fb = {k: v["actual_fb"] for k, v in fb.items()}
            prevs_fb = {k: {str(anno_base + i): v[f"{i}y"] for i in range(1, 6)} for k, v in fb.items()}
            return {
                "pais": "EUR",
                "yields_actuales": yields_fb,
                "previsiones": prevs_fb,
                "calidad": "degradado",
                "fuente": "Fallback estático (Feb 2026) — ECB API no disponible",
                "metodo_prevision": "estatico_fallback",
                "fecha_consulta": ahora,
                "detalle_calidad": f"ECB SDW API falló ({estado_api}). Datos estáticos de Feb 2026.",
            }

    # --- JP: Ministerio de Finanzas de Japón (MoF CSV) ---
    elif codigo_pais == "JP":
        yields, estado_api = _obtener_yields_mof_jp()

        if estado_api == "ok":
            previsiones = _calcular_forwards_implicitos(yields, TIPO_NEUTRAL["JP"], anno_base)
            return {
                "pais": "JP",
                "yields_actuales": yields,
                "previsiones": previsiones,
                "calidad": "ok",
                "fuente": "Ministerio de Finanzas de Japón (MoF) — CSV oficial JGB + forward rates implícitos",
                "metodo_prevision": "forward_rates_implicitos_suavizados",
                "fecha_consulta": ahora,
                "detalle_calidad": f"Yields obtenidos del MoF. {len(yields)}/7 plazos disponibles (3M/6M estimados).",
            }
        else:
            fb = FALLBACK_ESTATICO["JP"]
            yields_fb = {k: v["actual_fb"] for k, v in fb.items()}
            prevs_fb = {k: {str(anno_base + i): v[f"{i}y"] for i in range(1, 6)} for k, v in fb.items()}
            return {
                "pais": "JP",
                "yields_actuales": yields_fb,
                "previsiones": prevs_fb,
                "calidad": "degradado",
                "fuente": "Fallback estático (Feb 2026) — MoF CSV no disponible",
                "metodo_prevision": "estatico_fallback",
                "fecha_consulta": ahora,
                "detalle_calidad": f"MoF CSV falló ({estado_api}). Datos estáticos de Feb 2026.",
            }

    # --- CN: ChinaBond (CCDC) ---
    elif codigo_pais == "CN":
        yields, estado_api = _obtener_yields_chinabond()

        if estado_api == "ok":
            previsiones = _calcular_forwards_implicitos(yields, TIPO_NEUTRAL["CN"], anno_base)
            return {
                "pais": "CN",
                "yields_actuales": yields,
                "previsiones": previsiones,
                "calidad": "ok",
                "fuente": "ChinaBond (CCDC) — curva oficial de bonos soberanos chinos + forward rates implícitos",
                "metodo_prevision": "forward_rates_implicitos_suavizados",
                "fecha_consulta": ahora,
                "detalle_calidad": f"Yields obtenidos de ChinaBond. {len(yields)}/7 plazos disponibles (2Y estimado).",
            }
        else:
            fb = FALLBACK_ESTATICO["CN"]
            yields_fb = {k: v["actual_fb"] for k, v in fb.items()}
            prevs_fb = {k: {str(anno_base + i): v[f"{i}y"] for i in range(1, 6)} for k, v in fb.items()}
            return {
                "pais": "CN",
                "yields_actuales": yields_fb,
                "previsiones": prevs_fb,
                "calidad": "degradado",
                "fuente": "Fallback estático (Feb 2026) — ChinaBond no disponible",
                "metodo_prevision": "estatico_fallback",
                "fecha_consulta": ahora,
                "detalle_calidad": f"ChinaBond scraping falló ({estado_api}). Datos estáticos de Feb 2026.",
            }

    else:
        raise ValueError(f"País no soportado: {codigo_pais}")


def obtener_todas_las_previsiones(fred_api_key: str = "") -> dict[str, dict]:
    """Obtiene previsiones dinámicas para todos los países configurados."""
    return {
        pais: obtener_previsiones(pais, fred_api_key=fred_api_key)
        for pais in ["US", "EUR", "JP", "CN"]
    }


# ============================================================
# TEST RÁPIDO
# ============================================================
if __name__ == "__main__":
    import os
    api_key = os.getenv("FRED_API_KEY", "")

    print("=" * 60)
    print("TEST: Previsiones dinámicas de curvas de tipos")
    print("=" * 60)

    for pais in ["US", "EUR", "JP", "CN"]:
        resultado = obtener_previsiones(pais, fred_api_key=api_key)
        print(f"\n📊 {pais} — Calidad: {resultado['calidad'].upper()}")
        print(f"   Fuente: {resultado['fuente']}")
        print(f"   Detalle: {resultado['detalle_calidad']}")
        print(f"   Yields actuales: {resultado['yields_actuales']}")
        if "10Y" in resultado["previsiones"]:
            print(f"   Previsiones 10Y: {resultado['previsiones']['10Y']}")
