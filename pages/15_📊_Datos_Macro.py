import streamlit as st
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
import plotly.graph_objects as go
from styles import apply_styles

# ==========================================
# CONFIG
# ==========================================
FRED_KEY  = "d1b8ad24807ab32d1786cbcd3501a337"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
ECB_BASE  = "https://data.ecb.europa.eu/api/data"
WB_BASE   = "https://api.worldbank.org/v2/country"

st.set_page_config(layout="wide")
apply_styles()

# ==========================================
# MONGODB
# ==========================================
@st.cache_resource
def get_db():
    client = MongoClient(
        host="localhost", port=27017,
        username="admin", password="mike",
        authSource="admin"
    )
    return client["db-inver"]

db         = get_db()
col_macro  = db["datos_macro"]

# ==========================================
# FRED HELPERS
# ==========================================
def fred_fetch(series_id, n=14):
    """Returns (latest, prev, dataframe_sorted_desc)"""
    params = {
        "series_id": series_id,
        "api_key": FRED_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": n,
    }
    try:
        r = requests.get(FRED_BASE, params=params, timeout=10)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        df  = pd.DataFrame(obs)
        if df.empty:
            return None, None, pd.DataFrame()
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["date"]  = pd.to_datetime(df["date"])
        df = df.dropna(subset=["value"]).sort_values("date", ascending=False).reset_index(drop=True)
        latest = round(float(df.iloc[0]["value"]), 2)
        prev   = round(float(df.iloc[1]["value"]), 2) if len(df) > 1 else None
        return latest, prev, df
    except Exception as e:
        return None, None, pd.DataFrame()


def fred_yoy(series_id):
    """Calcula variación interanual para series de nivel (CPI, etc.)"""
    _, _, df = fred_fetch(series_id, n=15)
    if df.empty or len(df) < 13:
        return None, None
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    cur   = df.iloc[0]["value"]
    ya    = df.iloc[12]["value"]
    prev  = df.iloc[1]["value"]
    ya_p  = df.iloc[13]["value"] if len(df) > 13 else ya
    yoy      = round((cur  - ya)   / ya   * 100, 2)
    yoy_prev = round((prev - ya_p) / ya_p * 100, 2)
    return yoy, yoy_prev

# ==========================================
# ECB HELPERS
# ==========================================
def ecb_fetch(flow, key_str, n=3):
    """Returns (latest, prev) from ECB SDW JSON API"""
    url    = f"{ECB_BASE}/{flow},{key_str}"
    params = {"lastNObservations": n, "format": "jsondata", "detail": "dataonly"}
    try:
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()
        data    = r.json()
        datasets = data.get("dataSets", [])
        if not datasets:
            return None, None
        series_dict = datasets[0].get("series", {})
        if not series_dict:
            return None, None
        for _, s in series_dict.items():
            obs    = s.get("observations", {})
            sorted_keys = sorted(obs.keys(), key=lambda x: int(x))
            values = [obs[k][0] for k in sorted_keys if obs[k][0] is not None]
            if not values:
                return None, None
            return round(float(values[-1]), 2), (round(float(values[-2]), 2) if len(values) > 1 else None)
        return None, None
    except Exception:
        return None, None

# ==========================================
# WORLD BANK: CPI YoY anual (China, Japón)
# ==========================================
def wb_cpi(country_code):
    """Inflación anual % (World Bank FP.CPI.TOTL.ZG) — gratis, sin clave"""
    url = f"{WB_BASE}/{country_code}/indicator/FP.CPI.TOTL.ZG"
    params = {"format": "json", "mrv": 3, "per_page": 3}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if len(data) < 2 or not data[1]:
            return None, None
        obs = [x for x in data[1] if x.get("value") is not None]
        obs.sort(key=lambda x: x["date"], reverse=True)
        if not obs:
            return None, None
        latest = round(float(obs[0]["value"]), 2)
        prev   = round(float(obs[1]["value"]), 2) if len(obs) > 1 else None
        return latest, prev
    except Exception:
        return None, None

# ==========================================
# TRADING ECONOMICS SCRAPER (China PMI, BOJ, etc.)
# Misma técnica que worldgovernmentbonds.com
# ==========================================
SCRAPER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://www.google.com/",
}

def scrape_te(slug):
    """Scrapea el valor actual de un indicador en Trading Economics.
    Slug ej: 'china/manufacturing-pmi', 'japan/unemployment-rate'
    Devuelve (valor_actual, None).
    """
    url = f"https://tradingeconomics.com/{slug}"
    try:
        r = requests.get(url, headers=SCRAPER_HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # Método 1: span#p (elemento principal de valor en TE)
        el = soup.find(id="p")
        if el:
            txt = el.get_text(strip=True).replace(",", ".")
            m = re.search(r"(-?[\d]+\.?[\d]*)", txt)
            if m:
                return round(float(m.group(1)), 2), None

        # Método 2: buscar lastValue en scripts
        for script in soup.find_all("script"):
            if script.string and "lastValue" in (script.string or ""):
                m = re.search(r'"lastValue"\s*:\s*([\d.\-]+)', script.string)
                if m:
                    return round(float(m.group(1)), 2), None

        # Método 3: buscar el valor en el primer elemento con clase 'price'
        el = soup.find(class_=re.compile(r"price", re.I))
        if el:
            m = re.search(r"(-?[\d]+\.?[\d]*)", el.get_text())
            if m:
                return round(float(m.group(1)), 2), None

    except Exception:
        pass
    return None, None

# ==========================================
# WORLD BANK HELPER
# ==========================================
def wb_gdp(country_code):
    """PIB real YoY growth % — World Bank (gratuito, sin clave)"""
    url = f"{WB_BASE}/{country_code}/indicator/NY.GDP.MKTP.KD.ZG"
    params = {"format": "json", "mrv": 3, "per_page": 3}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if len(data) < 2 or not data[1]:
            return None, None
        obs = [x for x in data[1] if x.get("value") is not None]
        obs.sort(key=lambda x: x["date"], reverse=True)
        if not obs:
            return None, None
        latest = round(float(obs[0]["value"]), 2)
        prev   = round(float(obs[1]["value"]), 2) if len(obs) > 1 else None
        return latest, prev
    except Exception:
        return None, None

# ==========================================
# RECOPILAR TODOS LOS DATOS
# ==========================================
def recopilar_datos():
    datos   = {}
    errores = []

    progress = st.progress(0, text="Iniciando recopilación...")

    # --- Fed Funds Rate ---
    progress.progress(10, "🇺🇸 Obteniendo tipo Fed...")
    v, p, _ = fred_fetch("DFF", n=5)
    datos["fed_rate"] = v;  datos["fed_rate_prev"] = p

    # --- US CPI YoY ---
    progress.progress(22, "🇺🇸 Obteniendo CPI US...")
    datos["us_cpi"], datos["us_cpi_prev"] = fred_yoy("CPIAUCSL")

    # --- US Core CPI YoY ---
    progress.progress(34, "🇺🇸 Obteniendo Core CPI US...")
    datos["us_core_cpi"], datos["us_core_cpi_prev"] = fred_yoy("CPILFESL")

    # --- US Unemployment ---
    progress.progress(44, "🇺🇸 Obteniendo desempleo US...")
    v, p, _ = fred_fetch("UNRATE", n=3)
    datos["us_unemployment"] = v;  datos["us_unemployment_prev"] = p

    # --- ISM Manufacturing PMI ---
    progress.progress(54, "🇺🇸 Obteniendo ISM PMI...")
    v, p, _ = fred_fetch("NAPM", n=3)
    datos["ism_pmi"] = v;  datos["ism_pmi_prev"] = p

    # --- US GDP real (QoQ anualizado) ---
    progress.progress(62, "🇺🇸 Obteniendo PIB US...")
    v, p, _ = fred_fetch("A191RL1Q225SBEA", n=3)
    datos["us_gdp"] = v;  datos["us_gdp_prev"] = p

    # --- US Treasuries 10Y / 2Y ---
    progress.progress(70, "🇺🇸 Obteniendo curva de tipos US...")
    y10, y10p, _ = fred_fetch("GS10", n=5)
    y2,  y2p,  _ = fred_fetch("GS2",  n=5)
    datos["us_10y"] = y10;  datos["us_10y_prev"] = y10p
    datos["us_2y"]  = y2;   datos["us_2y_prev"]  = y2p
    if y10 is not None and y2 is not None:
        datos["yield_spread"]      = round(y10 - y2, 2)
        datos["yield_spread_prev"] = round(y10p - y2p, 2) if (y10p and y2p) else None

    # --- ECB Deposit Facility Rate ---
    progress.progress(82, "🇪🇺 Obteniendo tipo BCE...")
    v, p = ecb_fetch("FM", "B.U2.EUR.4F.KR.DFR.LEV")
    datos["ecb_rate"] = v;  datos["ecb_rate_prev"] = p

    # --- EU HICP (general y subyacente) ---
    progress.progress(90, "🇪🇺 Obteniendo inflación EU...")
    v, p = ecb_fetch("ICP", "M.U2.N.000000.4.ANR")
    datos["eu_cpi"] = v;  datos["eu_cpi_prev"] = p
    v, p = ecb_fetch("ICP", "M.U2.N.XEF000.4.ANR")
    datos["eu_core_cpi"] = v;  datos["eu_core_cpi_prev"] = p

    # --- CHINA ---
    progress.progress(92, "🇨🇳 Obteniendo datos China (World Bank + Trading Economics)...")
    datos["cn_cpi"], datos["cn_cpi_prev"]   = wb_cpi("CN")        # World Bank
    datos["cn_gdp"], datos["cn_gdp_prev"]   = wb_gdp("CN")        # World Bank
    v, _ = scrape_te("china/manufacturing-pmi")                    # Trading Economics
    datos["cn_pmi"]      = v
    datos["cn_pmi_prev"] = None

    # --- JAPÓN ---
    progress.progress(96, "🇯🇵 Obteniendo datos Japón (World Bank + Trading Economics)...")
    datos["jp_cpi"], datos["jp_cpi_prev"]         = wb_cpi("JP")   # World Bank
    datos["jp_gdp"], datos["jp_gdp_prev"]         = wb_gdp("JP")   # World Bank
    v, _ = scrape_te("japan/unemployment-rate")                    # Trading Economics
    datos["jp_unemployment"]      = v
    datos["jp_unemployment_prev"] = None
    v, _ = scrape_te("japan/interest-rate")                        # Trading Economics
    datos["boj_rate"]      = v
    datos["boj_rate_prev"] = None

    progress.progress(100, "✅ Completado")
    progress.empty()

    datos["fecha_actualizacion"] = datetime.now()
    return datos

# ==========================================
# SEMÁFORO MACRO
# ==========================================
def calcular_semaforo(datos):
    alertas = []
    score   = 0

    def chk(val, label, thresholds):
        """thresholds = [(limit, pts, icon, msg), ...]"""
        nonlocal score
        if val is None:
            return
        for limit, pts, icon, msg in thresholds:
            if val >= limit:
                score += pts
                alertas.append((icon, label, msg, fmt(val)))
                return

    # CPI US
    cpi = datos.get("us_cpi")
    if cpi:
        if cpi > 4:   score += 3; alertas.append(("🔴", "Inflación US", "Muy alta (>4%)",        fmt(cpi, s="%")))
        elif cpi > 3: score += 1; alertas.append(("🟡", "Inflación US", "Elevada (>3%)",          fmt(cpi, s="%")))
        else:                     alertas.append(("🟢", "Inflación US", "Controlada (<3%)",        fmt(cpi, s="%")))

    # EU CPI
    eu = datos.get("eu_cpi")
    if eu:
        if eu > 4:   score += 2; alertas.append(("🔴", "Inflación EU", "Alta (>4%)",         fmt(eu, s="%")))
        elif eu > 3: score += 1; alertas.append(("🟡", "Inflación EU", "Moderada (>3%)",     fmt(eu, s="%")))
        else:                    alertas.append(("🟢", "Inflación EU", "Controlada (<3%)",   fmt(eu, s="%")))

    # ISM PMI
    ism = datos.get("ism_pmi")
    if ism:
        if ism < 48:  score += 2; alertas.append(("🔴", "ISM PMI US", "Contracción profunda (<48)", fmt(ism)))
        elif ism < 50: score += 1; alertas.append(("🟡", "ISM PMI US", "Debilidad (48-50)",         fmt(ism)))
        else:                      alertas.append(("🟢", "ISM PMI US", "Expansión (>50)",            fmt(ism)))

    # Yield Spread
    sp = datos.get("yield_spread")
    if sp is not None:
        if sp < -0.5:  score += 2; alertas.append(("🔴", "Curva tipos US", "Invertida — riesgo recesión", fmt(sp, s="%")))
        elif sp < 0:   score += 1; alertas.append(("🟡", "Curva tipos US", "Ligeramente invertida",       fmt(sp, s="%")))
        else:                      alertas.append(("🟢", "Curva tipos US", "Normal (positiva)",           fmt(sp, s="%")))

    # Desempleo US
    un = datos.get("us_unemployment")
    if un:
        if un > 5.5:  score += 2; alertas.append(("🔴", "Desempleo US", "Alto (>5.5%)",       fmt(un, s="%")))
        elif un > 4.5: score += 1; alertas.append(("🟡", "Desempleo US", "Subiendo (>4.5%)", fmt(un, s="%")))
        else:                      alertas.append(("🟢", "Desempleo US", "Sólido (<4.5%)",   fmt(un, s="%")))

    if score <= 1:
        return "verde",    "Entorno favorable — mantener exposición",  alertas, score
    elif score <= 4:
        return "amarillo", "Señales mixtas — revisar diversificación",  alertas, score
    else:
        return "rojo",     "Entorno de riesgo — posición defensiva",   alertas, score

# ==========================================
# MONGODB
# ==========================================
def guardar_snapshot(datos, notas=""):
    doc        = {k: v for k, v in datos.items()}
    doc["notas"] = notas
    doc["mes"]   = datetime.now().strftime("%Y-%m")
    col_macro.update_one({"mes": doc["mes"]}, {"$set": doc}, upsert=True)

def cargar_historial(n=12):
    return list(col_macro.find({}, {"_id": 0}).sort("fecha_actualizacion", -1).limit(n))

# ==========================================
# UTILS
# ==========================================
def fmt(val, d=2, s=""):
    if val is None: return "N/D"
    return f"{val:.{d}f}{s}"

def delta_str(cur, prev):
    if cur is None or prev is None: return None
    return f"{cur - prev:+.2f}"

def gauge(title, val, rng, steps, threshold=None):
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=val or 0,
        title={"text": title, "font": {"size": 13}},
        number={"font": {"size": 22}},
        gauge={
            "axis":      {"range": rng},
            "bar":       {"color": "#4a6fa5"},
            "steps":     steps,
            "threshold": {"line": {"color": "#ef4444", "width": 3}, "thickness": 0.75, "value": threshold} if threshold else {}
        }
    ))
    fig.update_layout(height=230, margin=dict(l=15, r=15, t=45, b=10), paper_bgcolor="rgba(0,0,0,0)")
    return fig

# ==========================================
# UI PRINCIPAL
# ==========================================
st.title("📊 Datos Macro")
st.caption("Indicadores macroeconómicos clave — actualización mensual recomendada")

historial = cargar_historial()
ultimo    = historial[0] if historial else None

# --- Cabecera: estado + botón ---
col_inf, col_btn = st.columns([5, 1])
with col_inf:
    if ultimo:
        ts = ultimo.get("fecha_actualizacion")
        st.info(f"📅 Último snapshot: **{ts.strftime('%d/%m/%Y %H:%M')}**  |  Mes: **{ultimo.get('mes', 'N/D')}**")
    else:
        st.warning("⚠️ Sin datos guardados. Pulsa **Actualizar** para obtener el primer snapshot.")
with col_btn:
    actualizar = st.button("🔄 Actualizar", type="primary", use_container_width=True)

if actualizar:
    datos = recopilar_datos()
    st.session_state["datos_macro"] = datos
    st.success("✅ Datos actualizados correctamente. Revisa los indicadores abajo.")

# Datos a mostrar: sesión > MongoDB
if "datos_macro" in st.session_state:
    D = st.session_state["datos_macro"]
elif ultimo:
    D = ultimo
else:
    D = {}

if not D:
    st.stop()

# ==========================================
# SEMÁFORO
# ==========================================
semaforo, descripcion, alertas, score = calcular_semaforo(D)

COLOR_MAP = {
    "verde":    ("#16a34a", "🟢", "VERDE"),
    "amarillo": ("#ca8a04", "🟡", "AMARILLO"),
    "rojo":     ("#dc2626", "🔴", "ROJO"),
}
col_hex, emoji, label = COLOR_MAP[semaforo]

st.markdown(f"""
<div style="background:linear-gradient(135deg,{col_hex}18,{col_hex}08);
     border-left:6px solid {col_hex};border-radius:12px;
     padding:1.2rem 1.8rem;margin:1rem 0 1.5rem 0;">
  <span style="font-size:2rem;font-weight:800;color:{col_hex};">{emoji} SEMÁFORO MACRO: {label}</span><br>
  <span style="font-size:1rem;color:#374151;">{descripcion} &nbsp;|&nbsp; Score de riesgo: <strong>{score}/10</strong></span>
</div>
""", unsafe_allow_html=True)

with st.expander("📋 Análisis detallado de señales"):
    cols = st.columns(3)
    for i, (ico, indicador, msg, val) in enumerate(alertas):
        cols[i % 3].markdown(f"{ico} **{indicador}**  \n{msg} — `{val}`")

# ==========================================
# BLOQUE 1: POLÍTICA MONETARIA
# ==========================================
st.markdown("---")
st.subheader("🏦 Política Monetaria")
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("🇺🇸 Tipo Fed (DFF)", fmt(D.get("fed_rate"), s="%"),
              delta=delta_str(D.get("fed_rate"), D.get("fed_rate_prev")),
              delta_color="inverse", help="Tasa efectiva de fondos federales")
with c2:
    st.metric("🇪🇺 Tipo BCE (DFR)", fmt(D.get("ecb_rate"), s="%"),
              delta=delta_str(D.get("ecb_rate"), D.get("ecb_rate_prev")),
              delta_color="inverse", help="Tasa de facilidad de depósito del BCE")
with c3:
    st.metric("🇺🇸 Bono 10Y US", fmt(D.get("us_10y"), s="%"),
              delta=delta_str(D.get("us_10y"), D.get("us_10y_prev")))
with c4:
    spread = D.get("yield_spread")
    estado_curva = "🔴 Invertida" if spread and spread < 0 else "🟢 Normal"
    st.metric(f"📈 Spread 10Y-2Y", fmt(spread, s="%"),
              delta=delta_str(spread, D.get("yield_spread_prev")),
              help=f"Estado curva: {estado_curva}")

# ==========================================
# BLOQUE 2: INFLACIÓN
# ==========================================
st.markdown("---")
st.subheader("📈 Inflación (YoY)")
c1, c2, c3, c4 = st.columns(4)

with c1:
    v = D.get("us_cpi")
    lbl = "🔴" if v and v > 3 else "🟡" if v and v > 2.5 else "🟢"
    st.metric(f"{lbl} CPI US", fmt(v, s="%"),
              delta=delta_str(v, D.get("us_cpi_prev")), delta_color="inverse")
with c2:
    v = D.get("us_core_cpi")
    lbl = "🔴" if v and v > 3 else "🟡" if v and v > 2.5 else "🟢"
    st.metric(f"{lbl} Core CPI US", fmt(v, s="%"),
              delta=delta_str(v, D.get("us_core_cpi_prev")), delta_color="inverse",
              help="Sin energía ni alimentos")
with c3:
    v = D.get("eu_cpi")
    lbl = "🔴" if v and v > 3 else "🟡" if v and v > 2.5 else "🟢"
    st.metric(f"{lbl} HICP EU", fmt(v, s="%"),
              delta=delta_str(v, D.get("eu_cpi_prev")), delta_color="inverse",
              help="Índice armonizado de precios al consumo (Eurozona)")
with c4:
    v = D.get("eu_core_cpi")
    lbl = "🔴" if v and v > 3 else "🟡" if v and v > 2.5 else "🟢"
    st.metric(f"{lbl} Core HICP EU", fmt(v, s="%"),
              delta=delta_str(v, D.get("eu_core_cpi_prev")), delta_color="inverse",
              help="Sin energía ni alimentos (Eurozona)")

# ==========================================
# BLOQUE 3: ACTIVIDAD + MERCADO LABORAL
# ==========================================
st.markdown("---")
st.subheader("🏭 Actividad Económica y Mercado Laboral")
c1, c2, c3 = st.columns(3)

with c1:
    v = D.get("ism_pmi")
    lbl = "🔴" if v and v < 48 else "🟡" if v and v < 50 else "🟢"
    est = "Contracción" if v and v < 50 else "Expansión"
    st.metric(f"{lbl} ISM PMI Manufacturing",
              f"{fmt(v)} ({est})" if v else "N/D",
              delta=delta_str(v, D.get("ism_pmi_prev")),
              help="ISM Manufacturing PMI US. >50 = expansión, <50 = contracción")
with c2:
    v = D.get("us_gdp")
    lbl = "🔴" if v and v < 0 else "🟡" if v and v < 1 else "🟢"
    st.metric(f"{lbl} PIB Real US (QoQ)", fmt(v, s="%"),
              delta=delta_str(v, D.get("us_gdp_prev")),
              help="Crecimiento PIB real anualizado (trimestral)")
with c3:
    v = D.get("us_unemployment")
    lbl = "🔴" if v and v > 5 else "🟡" if v and v > 4.5 else "🟢"
    st.metric(f"{lbl} Desempleo US", fmt(v, s="%"),
              delta=delta_str(v, D.get("us_unemployment_prev")), delta_color="inverse")

# ==========================================
# BLOQUE 4: CHINA
# ==========================================
st.markdown("---")
st.subheader("🇨🇳 China")
c1, c2, c3 = st.columns(3)

with c1:
    v = D.get("cn_cpi")
    lbl = "🔴" if v and v > 3 else "🟡" if v and v > 2 else "🟢"
    st.metric(f"{lbl} CPI China (YoY)", fmt(v, s="%"),
              delta=delta_str(v, D.get("cn_cpi_prev")), delta_color="inverse")
with c2:
    v = D.get("cn_pmi")
    lbl = "🔴" if v and v < 48 else "🟡" if v and v < 50 else "🟢"
    est = "Contracción" if v and v < 50 else "Expansión"
    st.metric(f"{lbl} PMI Manufacturing China (NBS)",
              f"{fmt(v)} ({est})" if v else "N/D",
              delta=delta_str(v, D.get("cn_pmi_prev")),
              help="PMI Manufacturero oficial NBS (fuente FRED)")
with c3:
    v = D.get("cn_gdp")
    lbl = "🔴" if v and v < 3 else "🟡" if v and v < 5 else "🟢"
    st.metric(f"{lbl} PIB China (YoY)", fmt(v, s="%"),
              delta=delta_str(v, D.get("cn_gdp_prev")),
              help="Crecimiento PIB real anual (World Bank)")

# ==========================================
# BLOQUE 5: JAPÓN
# ==========================================
st.markdown("---")
st.subheader("🇯🇵 Japón")
c1, c2, c3, c4 = st.columns(4)

with c1:
    v = D.get("jp_cpi")
    lbl = "🔴" if v and v > 3 else "🟡" if v and v > 2 else "🟢"
    st.metric(f"{lbl} CPI Japón (YoY)", fmt(v, s="%"),
              delta=delta_str(v, D.get("jp_cpi_prev")), delta_color="inverse")
with c2:
    v = D.get("jp_unemployment")
    lbl = "🔴" if v and v > 3.5 else "🟡" if v and v > 3 else "🟢"
    st.metric(f"{lbl} Desempleo Japón", fmt(v, s="%"),
              delta=delta_str(v, D.get("jp_unemployment_prev")), delta_color="inverse")
with c3:
    v = D.get("boj_rate")
    st.metric("🏦 Tipo BOJ (overnight)", fmt(v, s="%"),
              delta=delta_str(v, D.get("boj_rate_prev")), delta_color="inverse",
              help="Call Money / Overnight Rate del Banco de Japón")
with c4:
    v = D.get("jp_gdp")
    lbl = "🔴" if v and v < 0 else "🟡" if v and v < 1 else "🟢"
    st.metric(f"{lbl} PIB Japón (YoY)", fmt(v, s="%"),
              delta=delta_str(v, D.get("jp_gdp_prev")),
              help="Crecimiento PIB real anual (World Bank)")

# ==========================================
# GAUGES VISUALES
# ==========================================
st.markdown("---")
st.subheader("🎯 Panel Visual")
cg1, cg2, cg3 = st.columns(3)

with cg1:
    st.plotly_chart(gauge(
        "Inflación US (CPI %)", D.get("us_cpi"), [0, 8],
        [{"range": [0, 2.5],  "color": "#dcfce7"},
         {"range": [2.5, 3.5],"color": "#fef9c3"},
         {"range": [3.5, 8],  "color": "#fee2e2"}], threshold=3
    ), use_container_width=True)

with cg2:
    st.plotly_chart(gauge(
        "ISM PMI Manufacturing", D.get("ism_pmi"), [40, 65],
        [{"range": [40, 48], "color": "#fee2e2"},
         {"range": [48, 50], "color": "#fef9c3"},
         {"range": [50, 65], "color": "#dcfce7"}], threshold=50
    ), use_container_width=True)

with cg3:
    st.plotly_chart(gauge(
        "Desempleo US (%)", D.get("us_unemployment"), [0, 10],
        [{"range": [0, 4],  "color": "#dcfce7"},
         {"range": [4, 5],  "color": "#fef9c3"},
         {"range": [5, 10], "color": "#fee2e2"}], threshold=5
    ), use_container_width=True)

# ==========================================
# GUARDAR SNAPSHOT
# ==========================================
st.markdown("---")
st.subheader("💾 Guardar Snapshot Mensual")

with st.form("form_guardar_macro"):
    notas_input = st.text_area(
        "📝 Notas del mes",
        placeholder="Ej: Fed mantiene tipos. Inflación en tendencia bajista. PMI en recuperación...",
        height=110
    )
    col_f1, _ = st.columns([1, 3])
    with col_f1:
        submitted = st.form_submit_button("💾 Guardar en MongoDB", type="primary", use_container_width=True)
    if submitted:
        if "datos_macro" not in st.session_state or not st.session_state["datos_macro"]:
            st.error("❌ Primero actualiza los datos antes de guardar.")
        else:
            guardar_snapshot(st.session_state["datos_macro"], notas_input)
            st.success("✅ Snapshot guardado correctamente.")
            del st.session_state["datos_macro"]
            st.rerun()

# ==========================================
# HISTORIAL
# ==========================================
st.markdown("---")
st.subheader("📅 Historial de Snapshots")

historial = cargar_historial()

if historial:
    rows = []
    for doc in historial:
        ts  = doc.get("fecha_actualizacion")
        sem = calcular_semaforo(doc)[0]
        sem_lbl = {"verde": "🟢 Verde", "amarillo": "🟡 Amarillo", "rojo": "🔴 Rojo"}.get(sem, "N/D")
        rows.append({
            "Mes":           doc.get("mes", "N/D"),
            "Fecha":         ts.strftime("%d/%m/%Y") if ts else "N/D",
            "Fed":           fmt(doc.get("fed_rate"), s="%"),
            "BCE":           fmt(doc.get("ecb_rate"), s="%"),
            "CPI US":        fmt(doc.get("us_cpi"), s="%"),
            "HICP EU":       fmt(doc.get("eu_cpi"), s="%"),
            "ISM PMI":       fmt(doc.get("ism_pmi")),
            "Desempleo US":  fmt(doc.get("us_unemployment"), s="%"),
            "Spread 10Y-2Y": fmt(doc.get("yield_spread"), s="%"),
            "CPI China":     fmt(doc.get("cn_cpi"), s="%"),
            "PMI China":     fmt(doc.get("cn_pmi")),
            "CPI Japón":     fmt(doc.get("jp_cpi"), s="%"),
            "BOJ":           fmt(doc.get("boj_rate"), s="%"),
            "Semáforo":      sem_lbl,
            "Notas":         (doc.get("notas") or "")[:60],
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
else:
    st.info("Aún no hay snapshots guardados. Actualiza los datos y guarda el primero.")
