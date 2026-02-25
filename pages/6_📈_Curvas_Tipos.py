import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime, UTC
import sys
import os

# Añadir src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from styles import apply_styles
from src.scraper_curvas_tipos import (
    obtener_todas_las_curvas,
    guardar_curvas_en_mongodb,
    obtener_ultimo_registro_curvas,
    PAISES,
)

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(
    page_title="Curvas de Tipos – Bonos Soberanos",
    page_icon="📈",
    layout="wide",
)
apply_styles()

# ==========================================================
# ESTILOS ESPECÍFICOS
# ==========================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

.curve-card {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    border-radius: 16px;
    padding: 24px;
    margin-bottom: 16px;
    border: 1px solid rgba(255,255,255,0.06);
    box-shadow: 0 4px 24px rgba(0,0,0,0.2);
    font-family: 'Inter', sans-serif;
}
.curve-header {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 18px;
}
.curve-emoji { font-size: 32px; }
.curve-name {
    font-size: 18px;
    font-weight: 700;
    color: #f1f5f9;
    margin: 0;
}
.curve-sub {
    font-size: 12px;
    color: #64748b;
    margin: 0;
}

/* ---- Tabla de rendimientos ---- */
.yield-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    border-radius: 10px;
    overflow: hidden;
    font-family: 'Inter', sans-serif;
}
.yield-table thead th {
    background: #1e293b;
    color: #94a3b8;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    padding: 10px 12px;
    text-align: center;
    border-bottom: 2px solid rgba(96,165,250,0.15);
}
.yield-table thead th:first-child { text-align: left; }
.yield-table tbody td {
    background: #0b1221;
    color: #e2e8f0;
    padding: 10px 12px;
    text-align: center;
    font-size: 14px;
    font-weight: 500;
    border-bottom: 1px solid rgba(255,255,255,0.03);
}
.yield-table tbody td:first-child {
    text-align: left;
    font-weight: 700;
    color: #94a3b8;
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.yield-table tbody tr:hover td { background: rgba(30,41,59,0.8); }

.td-actual {
    font-weight: 700;
    font-size: 16px;
    background: linear-gradient(135deg, #60a5fa, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.delta-down { color: #34d399; font-size: 10px; }
.delta-up { color: #f87171; font-size: 10px; }
.delta-flat { color: #64748b; font-size: 10px; }

.badge-live {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    background: rgba(52,211,153,0.12);
    color: #34d399;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 10px;
    font-weight: 600;
}
.badge-dot {
    width: 5px; height: 5px;
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
    gap: 4px;
    background: rgba(250,204,21,0.12);
    color: #facc15;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 10px;
    font-weight: 600;
}
.badge-degraded {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    background: rgba(248,113,113,0.12);
    color: #f87171;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 10px;
    font-weight: 600;
}

.source-text {
    font-size: 11px;
    color: #475569;
    margin-top: 12px;
    font-style: italic;
}

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

.info-bar {
    background: linear-gradient(90deg, rgba(96,165,250,0.08), rgba(167,139,250,0.08));
    border: 1px solid rgba(96,165,250,0.15);
    border-radius: 12px;
    padding: 14px 20px;
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    color: #94a3b8;
    margin-bottom: 24px;
}

/* ---- Gran tabla comparativa ---- */
.big-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    border-radius: 12px;
    overflow: hidden;
    font-family: 'Inter', sans-serif;
    margin-top: 12px;
}
.big-table thead th {
    background: #1e293b;
    color: #94a3b8;
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    padding: 12px 10px;
    text-align: center;
    border-bottom: 2px solid rgba(255,255,255,0.06);
}
.big-table thead th:first-child { text-align: left; }
.big-table tbody td {
    background: #0b1221;
    color: #e2e8f0;
    padding: 10px;
    text-align: center;
    font-size: 13px;
    font-weight: 500;
    border-bottom: 1px solid rgba(255,255,255,0.03);
}
.big-table tbody td:first-child {
    text-align: left;
    font-weight: 600;
}
.big-table tbody tr:hover td { background: #1e293b; }
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
st.title("📈 Curvas de Tipos – Bonos Soberanos")

if "ct_success_msg" in st.session_state:
    st.success(st.session_state.ct_success_msg)
    del st.session_state.ct_success_msg

# ==========================================================
# INFO SUPERIOR
# ==========================================================
ultimo = obtener_ultimo_registro_curvas(db)
if ultimo:
    fecha_ult = ultimo.get("fecha_consulta")
    fecha_str = fecha_ult.strftime("%d/%m/%Y %H:%M:%S UTC") if fecha_ult else "Desconocida"
    num_p = ultimo.get("num_paises", 0)
    cid = ultimo.get("consulta_id", "-")
    st.markdown(
        f'<div class="info-bar">📅 Última actualización: <strong>{fecha_str}</strong> &nbsp;|&nbsp; 🌍 {num_p} economías &nbsp;|&nbsp; 🆔 {cid}</div>',
        unsafe_allow_html=True,
    )

# ==========================================================
# BOTÓN ACTUALIZAR
# ==========================================================
col_btn, _ = st.columns([2, 8])
with col_btn:
    actualizar = st.button("🔄 Obtener datos actualizados", type="primary", key="btn_ct")

if actualizar:
    with st.spinner("Consultando rendimientos de bonos soberanos..."):
        datos = obtener_todas_las_curvas()
    if datos:
        try:
            resultado = guardar_curvas_en_mongodb(db, datos)
            st.session_state.ct_success_msg = (
                f"✅ Datos guardados correctamente\n\n"
                f"🆔 Consulta: {resultado['consulta_id']}\n"
                f"🌍 Países: {resultado['num_paises']}\n"
                f"📂 ID Mongo: {resultado['inserted_id']}"
            )
            st.session_state.curvas_actuales = datos
            st.rerun()
        except Exception as e:
            st.error(f"Error al guardar en MongoDB: {e}")
    else:
        st.error("No se pudieron obtener datos.")

# ==========================================================
# CARGAR DATOS
# ==========================================================
datos_mostrar = None
if "curvas_actuales" in st.session_state:
    datos_mostrar = st.session_state.curvas_actuales
elif ultimo and "paises" in ultimo:
    datos_mostrar = ultimo["paises"]

# DEBUG: mostrar cuántos años de previsión tienen los datos cargados
if datos_mostrar:
    primer_pais = datos_mostrar[0]
    primer_plazo = primer_pais.get("plazos", [{}])[0] if primer_pais.get("plazos") else {}
    years_debug = list(primer_plazo.get("previsiones", {}).keys())
    origen_debug = "session_state" if "curvas_actuales" in st.session_state else "MongoDB"
    st.caption(f"🔍 Debug — Origen datos: `{origen_debug}` · Años previsión: `{sorted(years_debug)}`")

if not datos_mostrar:
    st.markdown("---")
    st.info(
        "👆 Pulsa **'Obtener datos actualizados'** para consultar "
        "las curvas de tipos de bonos soberanos y guardarlas en la base de datos."
    )
    st.stop()

# ==========================================================
# FUNCIÓN: CREAR GRÁFICO DE CURVA DE TIPOS
# ==========================================================
PLAZO_ORDEN = ["3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y"]
PLAZO_LABELS = {"3M": "3M", "6M": "6M", "1Y": "1Y", "2Y": "2Y", "5Y": "5Y", "10Y": "10Y", "30Y": "30Y"}
# Colores para las líneas
COLOR_ACTUAL = "#60a5fa"       # azul brillante
COLORES_PREV = ["#a78bfa", "#c084fc", "#e879f9", "#fb7185", "#f97316"]  # 5 años de previsión
COLOR_FILL   = "rgba(96,165,250,0.12)"


def crear_grafico_curva(plazos_data: list[dict], nombre_pais: str, emoji: str) -> go.Figure:
    """Crea un gráfico Plotly de curva de tipos con líneas de previsiones."""

    # Ordenar plazos según el orden definido
    plazos_ordenados = sorted(
        plazos_data,
        key=lambda p: PLAZO_ORDEN.index(p["plazo"]) if p["plazo"] in PLAZO_ORDEN else 99
    )

    x_labels = [p["plazo"] for p in plazos_ordenados]
    y_actual = [p["rendimiento_actual"] for p in plazos_ordenados]

    # Obtener años de previsiones
    years = sorted(plazos_ordenados[0].get("previsiones", {}).keys()) if plazos_ordenados else []

    fig = go.Figure()

    # ---- Área rellena bajo la curva actual ----
    fig.add_trace(go.Scatter(
        x=x_labels, y=y_actual,
        fill="tozeroy",
        fillcolor=COLOR_FILL,
        line=dict(color="rgba(0,0,0,0)"),
        showlegend=False,
        hoverinfo="skip",
    ))

    # ---- Línea de curva actual ----
    fig.add_trace(go.Scatter(
        x=x_labels, y=y_actual,
        mode="lines+markers",
        name="Actual",
        line=dict(color=COLOR_ACTUAL, width=3),
        marker=dict(size=7, color=COLOR_ACTUAL, line=dict(width=1, color="#1e293b")),
        hovertemplate="%{x}: <b>%{y:.2f}%</b><extra>Actual</extra>",
    ))

    # ---- Líneas de previsiones ----
    for idx, year in enumerate(years):
        y_prev = [p.get("previsiones", {}).get(year, None) for p in plazos_ordenados]
        color = COLORES_PREV[idx] if idx < len(COLORES_PREV) else "#94a3b8"
        fig.add_trace(go.Scatter(
            x=x_labels, y=y_prev,
            mode="lines+markers",
            name=f"Prev. {year}",
            line=dict(color=color, width=2, dash="dot"),
            marker=dict(size=5, color=color),
            hovertemplate=f"%{{x}}: <b>%{{y:.2f}}%</b><extra>Prev. {year}</extra>",
        ))

    # ---- Layout oscuro ----
    y_min = min(v for v in y_actual if v is not None) - 0.3
    y_max = max(v for v in y_actual if v is not None) + 0.5

    # Incluir previsiones en los límites
    for year in years:
        for p in plazos_ordenados:
            v = p.get("previsiones", {}).get(year)
            if v is not None:
                y_min = min(y_min, v - 0.3)
                y_max = max(y_max, v + 0.5)

    fig.update_layout(
        height=280,
        margin=dict(l=0, r=10, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, Arial", size=11, color="#94a3b8"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10, color="#94a3b8"),
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=dict(
            showgrid=False,
            showline=True,
            linecolor="rgba(148,163,184,0.2)",
            tickfont=dict(size=11, color="#94a3b8"),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(148,163,184,0.08)",
            zeroline=False,
            showline=False,
            ticksuffix="%",
            tickfont=dict(size=11, color="#94a3b8"),
            range=[max(y_min, -0.5), y_max],
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="#1e293b",
            font_color="#e2e8f0",
            font_size=12,
            bordercolor="rgba(96,165,250,0.3)",
        ),
    )

    return fig


# ==========================================================
# TARJETAS CON GRÁFICOS + TABLAS DE RENDIMIENTO
# ==========================================================
st.markdown('<div class="section-title">📊 Curvas de Tipos por Economía</div>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

for i, pais in enumerate(datos_mostrar):
    target_col = col1 if i % 2 == 0 else col2
    plazos = pais.get("plazos", [])
    anno_actual = datetime.now().year

    # Badge scraping
    if pais.get("scrapeado", False):
        badge = '<span class="badge-live"><span class="badge-dot"></span>LIVE</span>'
    else:
        badge = '<span class="badge-cached">⚠ REF</span>'

    # Badge calidad previsiones
    calidad_prev = pais.get("calidad_previsiones", "")
    if calidad_prev == "ok":
        badge_prev = '<span class="badge-live"><span class="badge-dot"></span>PREV DINÁMICA</span>'
    elif calidad_prev == "degradado":
        badge_prev = '<span class="badge-cached">⚠ PREV ESTÁTICA</span>'
    else:
        badge_prev = ""

    fuente = pais.get("fuente_previsiones", "N/A")
    detalle = pais.get("detalle_calidad", "")
    scrp = pais.get("num_plazos_scrapeados", 0)
    total = len(plazos)

    # ---- Header HTML de la tarjeta ----
    detalle_html = f'<p class="curve-sub" style="color:#475569;margin-top:4px;">{detalle}</p>' if detalle and calidad_prev != "ok" else ""
    header_html = (
        f'<div class="curve-card" style="padding-bottom:8px;">'
        f'<div class="curve-header">'
        f'<span class="curve-emoji">{pais["emoji"]}</span>'
        f'<div>'
        f'<p class="curve-name">{pais["nombre"]} {badge} {badge_prev}</p>'
        f'<p class="curve-sub">{pais["moneda"]} · {scrp}/{total} plazos live</p>'
        f'{detalle_html}'
        f'</div></div></div>'
    )
    target_col.markdown(header_html, unsafe_allow_html=True)

    # ---- Gráfico de curva de tipos ----
    if plazos:
        fig = crear_grafico_curva(plazos, pais["nombre"], pais["emoji"])
        target_col.plotly_chart(fig, width="stretch", key=f"chart_{pais['codigo']}")

    # ---- Tabla de rendimientos ----
    rows_html = ""
    for p in plazos:
        actual = p["rendimiento_actual"]
        prevs = p.get("previsiones", {})
        years = sorted(prevs.keys())

        actual_cell = f'<td><span class="td-actual">{actual:.2f}%</span></td>'

        prev_cells = ""
        ref = actual
        for y in years:
            v = prevs[y]
            delta = v - ref
            if delta < -0.01:
                ds = f'<span class="delta-down">▼{abs(delta):.2f}</span>'
            elif delta > 0.01:
                ds = f'<span class="delta-up">▲{delta:.2f}</span>'
            else:
                ds = f'<span class="delta-flat">—</span>'
            prev_cells += f'<td>{v:.2f}% {ds}</td>'
            ref = v

        rows_html += f'<tr><td>{p["plazo"]}</td>{actual_cell}{prev_cells}</tr>'

    years = sorted(plazos[0]["previsiones"].keys()) if plazos and plazos[0].get("previsiones") else []
    year_headers = "".join(f"<th>{y}</th>" for y in years)

    table_html = (
        f'<table class="yield-table">'
        f'<thead><tr><th>Plazo</th><th>Actual</th>{year_headers}</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        f'</table>'
    )

    metodo = pais.get("metodo_prevision", "")
    metodo_label = {
        "forward_rates_implicitos_suavizados": "forward rates implícitos de mercado",
        "estatico_fallback": "datos estáticos de referencia",
        "estatico_consenso": "consenso de analistas estático",
    }.get(metodo, metodo)

    footer_html = (
        f'{table_html}'
        f'<div class="source-text">📎 {fuente}'
        + (f' <span style="color:#334155"> · Método: {metodo_label}</span>' if metodo_label else '')
        + f'</div>'
    )
    target_col.markdown(footer_html, unsafe_allow_html=True)

# ==========================================================
# TABLA COMPARATIVA: 10 AÑOS
# ==========================================================
st.markdown('<div class="section-title">📋 Comparativa – Bono a 10 Años</div>', unsafe_allow_html=True)

comp_rows = []
for pais in datos_mostrar:
    p10 = next((p for p in pais.get("plazos", []) if p["plazo"] == "10Y"), None)
    if p10:
        prevs = p10.get("previsiones", {})
        years = sorted(prevs.keys())
        row = {
            "Economía": f'{pais["emoji"]} {pais["nombre"]}',
            "Actual (%)": p10["rendimiento_actual"],
        }
        for y in years:
            row[f"Prev. {y} (%)"] = prevs[y]
        comp_rows.append(row)

if comp_rows:
    df_comp = pd.DataFrame(comp_rows)
    h = "".join(f"<th>{c}</th>" for c in df_comp.columns)
    b = ""
    for _, r in df_comp.iterrows():
        cells = ""
        for c in df_comp.columns:
            v = r[c]
            if isinstance(v, float):
                cells += f"<td>{v:.2f}%</td>"
            else:
                cells += f"<td>{v}</td>"
        b += f"<tr>{cells}</tr>"
    st.markdown(
        f'<table class="big-table"><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table>',
        unsafe_allow_html=True,
    )

# ==========================================================
# HISTORIAL
# ==========================================================
st.markdown('<div class="section-title">🕐 Historial de Consultas</div>', unsafe_allow_html=True)

historial = list(db["curvas_tipos"].find().sort("_id", -1).limit(10))
if historial:
    hist_rows = []
    for doc in historial:
        f = doc.get("fecha_consulta")
        hist_rows.append({
            "Consulta ID": doc.get("consulta_id", "-"),
            "Fecha": f.strftime("%d/%m/%Y %H:%M") if f else "-",
            "Países": doc.get("num_paises", 0),
            "ID Mongo": str(doc.get("_id", "")),
        })
    st.dataframe(pd.DataFrame(hist_rows), width="stretch", hide_index=True)
else:
    st.info("No hay consultas guardadas todavía.")
