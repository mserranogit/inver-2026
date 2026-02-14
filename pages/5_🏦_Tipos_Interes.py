import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, UTC
import sys
import os

# Añadir src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from styles import apply_styles
from src.scraper_tipos_interes import (
    obtener_todos_los_bancos,
    guardar_en_mongodb,
    obtener_ultimo_registro,
    BANCOS_CENTRALES,
)

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(
    page_title="Tipos de Interés – Bancos Centrales",
    page_icon="🏦",
    layout="wide",
)
apply_styles()

# ==========================================================
# ESTILOS ESPECÍFICOS
# ==========================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ---- Tarjeta de banco central ---- */
.bank-card {
    background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
    border-radius: 16px;
    padding: 24px;
    margin-bottom: 16px;
    border: 1px solid rgba(255,255,255,0.08);
    box-shadow: 0 4px 24px rgba(0,0,0,0.18);
    transition: transform 0.2s, box-shadow 0.2s;
    font-family: 'Inter', sans-serif;
}
.bank-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 8px 32px rgba(0,0,0,0.28);
}

.bank-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 16px;
}
.bank-emoji {
    font-size: 36px;
    line-height: 1;
}
.bank-name {
    font-size: 18px;
    font-weight: 700;
    color: #f8fafc;
    margin: 0;
    letter-spacing: -0.3px;
}
.bank-country {
    font-size: 13px;
    color: #94a3b8;
    margin: 0;
}

/* ---- Tipo actual ---- */
.rate-current {
    display: flex;
    align-items: baseline;
    gap: 8px;
    margin: 12px 0 8px 0;
}
.rate-value {
    font-size: 42px;
    font-weight: 700;
    background: linear-gradient(135deg, #60a5fa, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: -1px;
    line-height: 1;
}
.rate-unit {
    font-size: 18px;
    color: #94a3b8;
    font-weight: 500;
}
.rate-label {
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #64748b;
    font-weight: 600;
}

/* ---- Previsiones ---- */
.forecast-row {
    display: flex;
    gap: 10px;
    margin-top: 14px;
}
.forecast-chip {
    flex: 1;
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 10px;
    padding: 10px 12px;
    text-align: center;
}
.forecast-year {
    font-size: 11px;
    color: #94a3b8;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.forecast-val {
    font-size: 22px;
    font-weight: 700;
    color: #e2e8f0;
    margin-top: 2px;
}
.forecast-delta {
    font-size: 11px;
    margin-top: 2px;
    font-weight: 600;
}
.delta-down { color: #34d399; }
.delta-up   { color: #f87171; }
.delta-flat { color: #94a3b8; }

/* ---- Badges ---- */
.badge-live {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: rgba(52, 211, 153, 0.12);
    color: #34d399;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.3px;
}
.badge-dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    background: #34d399;
    display: inline-block;
    animation: pulse 1.5s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
}

.badge-cached {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: rgba(250, 204, 21, 0.12);
    color: #facc15;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
}

/* ---- Fuente ---- */
.source-text {
    font-size: 11px;
    color: #64748b;
    margin-top: 12px;
    font-style: italic;
}

/* ---- Tabla comparativa ---- */
.comp-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    border-radius: 12px;
    overflow: hidden;
    font-family: 'Inter', sans-serif;
    margin-top: 16px;
}
.comp-table thead th {
    background: #1e293b;
    color: #94a3b8;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    padding: 14px 16px;
    text-align: center;
    border-bottom: 2px solid rgba(255,255,255,0.06);
}
.comp-table tbody td {
    background: #0f172a;
    color: #e2e8f0;
    padding: 14px 16px;
    text-align: center;
    font-size: 15px;
    font-weight: 500;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}
.comp-table tbody tr:hover td {
    background: #1e293b;
}
.comp-table tbody td:first-child {
    text-align: left;
    font-weight: 600;
}

/* ---- Sección título ---- */
.section-title {
    font-family: 'Inter', sans-serif;
    font-size: 14px;
    text-transform: uppercase;
    letter-spacing: 2px;
    color: #64748b;
    font-weight: 700;
    margin: 32px 0 16px 0;
    padding-bottom: 8px;
    border-bottom: 2px solid rgba(100,116,139,0.2);
}

/* ---- Info bar ---- */
.info-bar {
    background: linear-gradient(90deg, rgba(96,165,250,0.08), rgba(167,139,250,0.08));
    border: 1px solid rgba(96,165,250,0.15);
    border-radius: 12px;
    padding: 14px 20px;
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    color: #94a3b8;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 10px;
}
</style>
""", unsafe_allow_html=True)


# ==========================================================
# CONEXIÓN MONGO
# ==========================================================
@st.cache_resource
def get_db():
    client = MongoClient(
        host="localhost",
        port=27017,
        username="admin",
        password="mike",
        authSource="admin",
    )
    return client["db-inver"]


db = get_db()

# ==========================================================
# TÍTULO
# ==========================================================
st.title("🏦 Tipos de Interés – Bancos Centrales")

# Mensajes de éxito persistentes
if "ti_success_msg" in st.session_state:
    st.success(st.session_state.ti_success_msg)
    del st.session_state.ti_success_msg

# ==========================================================
# INFO SUPERIOR
# ==========================================================
ultimo = obtener_ultimo_registro(db)
if ultimo:
    fecha_ult = ultimo.get("fecha_consulta")
    if fecha_ult:
        fecha_str = fecha_ult.strftime("%d/%m/%Y %H:%M:%S UTC")
    else:
        fecha_str = "Desconocida"
    num_bancos = ultimo.get('num_bancos', 0)
    consulta_id = ultimo.get('consulta_id', '-')
    st.markdown(
        f'<div class="info-bar">📅 Última actualización guardada: <strong>{fecha_str}</strong> &nbsp;|&nbsp; 🏦 {num_bancos} bancos registrados &nbsp;|&nbsp; 🆔 {consulta_id}</div>',
        unsafe_allow_html=True,
    )

# ==========================================================
# BOTÓN ACTUALIZAR
# ==========================================================
col_btn, col_space = st.columns([2, 8])
with col_btn:
    actualizar = st.button("🔄 Obtener datos actualizados", type="primary")

if actualizar:
    with st.spinner("Consultando bancos centrales..."):
        datos = obtener_todos_los_bancos()

    if datos:
        # Guardar en MongoDB
        try:
            resultado = guardar_en_mongodb(db, datos)
            st.session_state.ti_success_msg = (
                f"✅ Datos guardados correctamente\n\n"
                f"🆔 Consulta: {resultado['consulta_id']}\n"
                f"🏦 Bancos: {resultado['num_bancos']}\n"
                f"📂 ID Mongo: {resultado['inserted_id']}"
            )
            st.session_state.datos_actuales = datos
            st.rerun()
        except Exception as e:
            st.error(f"Error al guardar en MongoDB: {e}")
    else:
        st.error("No se pudieron obtener datos.")

# ==========================================================
# CARGAR DATOS PARA VISUALIZACIÓN
# ==========================================================
datos_mostrar = None

if "datos_actuales" in st.session_state:
    datos_mostrar = st.session_state.datos_actuales
elif ultimo and "bancos" in ultimo:
    datos_mostrar = ultimo["bancos"]

if not datos_mostrar:
    st.markdown("---")
    st.info(
        "👆 Pulsa **'Obtener datos actualizados'** para consultar "
        "los tipos de interés de los bancos centrales y guardarlos en la base de datos."
    )
    st.stop()

# ==========================================================
# TARJETAS DE BANCOS CENTRALES
# ==========================================================
st.markdown('<div class="section-title">📊 Tipos de Interés Actuales y Previsiones</div>', unsafe_allow_html=True)

# Crear 2 columnas para las 4 tarjetas
col1, col2 = st.columns(2)

for i, banco in enumerate(datos_mostrar):
    target_col = col1 if i % 2 == 0 else col2

    # Calcular deltas para los chips de previsión
    actual = banco["tipo_actual"]
    previsiones = banco.get("previsiones", {})

    # Construir chips de previsión en línea (sin indentación para evitar code blocks)
    chips_parts = []
    prev_val = actual
    for year, val in sorted(previsiones.items()):
        delta = val - prev_val
        if delta < -0.01:
            delta_class = "delta-down"
            delta_str = f"▼ {abs(delta):.2f}"
        elif delta > 0.01:
            delta_class = "delta-up"
            delta_str = f"▲ {delta:.2f}"
        else:
            delta_class = "delta-flat"
            delta_str = "—"
        chip = (
            f'<div class="forecast-chip">'
            f'<div class="forecast-year">{year}</div>'
            f'<div class="forecast-val">{val:.2f}%</div>'
            f'<div class="forecast-delta {delta_class}">{delta_str}</div>'
            f'</div>'
        )
        chips_parts.append(chip)
        prev_val = val

    forecast_chips = "".join(chips_parts)

    # Badge de live/cached
    if banco.get("tipo_scrapeado", False):
        badge = '<span class="badge-live"><span class="badge-dot"></span>LIVE</span>'
    else:
        badge = '<span class="badge-cached">⚠ CACHED</span>'

    fuente = banco.get('fuente_previsiones', 'N/A')

    # HTML compacto sin indentación (Streamlit interpreta 4+ espacios como código)
    card_html = (
        f'<div class="bank-card">'
        f'<div class="bank-header">'
        f'<span class="bank-emoji">{banco["emoji"]}</span>'
        f'<div>'
        f'<p class="bank-name">{banco["nombre_completo"]}</p>'
        f'<p class="bank-country">{banco["pais"]} · {banco["moneda"]}</p>'
        f'</div></div>'
        f'<div class="rate-label">{banco["tipo_referencia_nombre"]} {badge}</div>'
        f'<div class="rate-current">'
        f'<span class="rate-value">{actual:.2f}</span>'
        f'<span class="rate-unit">%</span>'
        f'</div>'
        f'<div class="forecast-row">{forecast_chips}</div>'
        f'<div class="source-text">📎 Previsiones: {fuente}</div>'
        f'</div>'
    )
    target_col.markdown(card_html, unsafe_allow_html=True)

# ==========================================================
# TABLA COMPARATIVA
# ==========================================================
st.markdown('<div class="section-title">📋 Tabla Comparativa</div>', unsafe_allow_html=True)

# Construir datos para tabla
table_rows = []
for banco in datos_mostrar:
    previsiones = banco.get("previsiones", {})
    years = sorted(previsiones.keys())
    row = {
        "Banco": f"{banco['emoji']} {banco['codigo']}",
        "País": banco["pais"],
        "Tipo Referencia": banco["tipo_referencia_nombre"],
        "Actual (%)": banco["tipo_actual"],
    }
    for y in years:
        row[f"Prev. {y} (%)"] = previsiones[y]
    table_rows.append(row)

df_tabla = pd.DataFrame(table_rows)

# Generar HTML de tabla
header_html = "".join(f"<th>{col}</th>" for col in df_tabla.columns)
body_html = ""
for _, row in df_tabla.iterrows():
    cells = ""
    for col in df_tabla.columns:
        val = row[col]
        if isinstance(val, float):
            cells += f"<td>{val:.2f}%</td>"
        else:
            cells += f"<td>{val}</td>"
    body_html += f"<tr>{cells}</tr>"

st.markdown(
    f'<table class="comp-table"><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>',
    unsafe_allow_html=True,
)

# ==========================================================
# HISTORIAL DE CONSULTAS
# ==========================================================
st.markdown('<div class="section-title">🕐 Historial de Consultas</div>', unsafe_allow_html=True)

historial = list(db["tipos_interes"].find().sort("_id", -1).limit(10))

if historial:
    hist_rows = []
    for doc in historial:
        fecha = doc.get("fecha_consulta")
        hist_rows.append({
            "Consulta ID": doc.get("consulta_id", "-"),
            "Fecha": fecha.strftime("%d/%m/%Y %H:%M") if fecha else "-",
            "Bancos": doc.get("num_bancos", 0),
            "ID Mongo": str(doc.get("_id", "")),
        })
    df_hist = pd.DataFrame(hist_rows)
    st.dataframe(df_hist, use_container_width=True, hide_index=True)
else:
    st.info("No hay consultas guardadas todavía.")
