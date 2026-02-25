import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.graph_objects as go
import math
from styles import apply_styles

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(layout="wide")
apply_styles()
st.title("⚖️ Comparador de ETFs")

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
        authSource="admin"
    )
    return client["db-inver"]

db = get_db()
etfs_collection = db["etfs"]

# ==========================================================
# CARGA DATOS
# ==========================================================
etfs_cursor = etfs_collection.find({}, {
    "_id": 0,
    "isin": 1,
    "nombreEtf": 1,
    "tipoEtf": 1,
    "riesgo": 1,
    "yield_1y": 1,
    "yield_3y": 1,
    "yield_5y": 1,
    "ter": 1
})

df = pd.DataFrame(list(etfs_cursor))

if df.empty:
    st.warning("No hay ETFs disponibles en la base de datos.")
    st.stop()

# Limpieza de seguridad
df = df.drop_duplicates(subset=["isin"]).reset_index(drop=True)

# Helper para parsear rentabilidad
def parse_yield(y):
    if not y or y == "N/A": return 0.0
    try: return float(str(y).replace("%", "").replace("+", ""))
    except: return 0.0

df["rent_val"] = df["yield_1y"].apply(parse_yield)

# ==========================================================
# SECCIÓN 1: FILTROS
# ==========================================================
filter_container = st.container()
with filter_container:
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    
    with col_f1:
        isin_input = st.text_input("🔍 Buscar por ISIN o Nombre", placeholder="Escriba ISIN o nombre...").strip()
    
    with col_f2:
        tipo_options = ["Todos"] + sorted(df["tipoEtf"].dropna().unique().tolist())
        tipo_filter = st.selectbox("📊 Tipo ETF", tipo_options)
    
    with col_f3:
        rent_options = ["Todos", "Sinceramente Positiva (>0%)", "Alta (>3%)", "Muy Alta (>5%)", "Negativa (<0%)"]
        rent_filter = st.selectbox("📈 Rentabilidad (1A)", rent_options)
    
    with col_f4:
        riesgo_options = ["Todos"] + sorted(df["riesgo"].dropna().unique().tolist())
        riesgo_filter = st.selectbox("⚠️ Riesgo", riesgo_options)

# Aplicar Filtros
filtered_df = df.copy()

if isin_input:
    mask_isin = filtered_df["isin"].str.contains(isin_input, case=False, na=False)
    mask_nombre = filtered_df["nombreEtf"].str.contains(isin_input, case=False, na=False)
    filtered_df = filtered_df[mask_isin | mask_nombre]

if tipo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tipoEtf"] == tipo_filter]

if rent_filter != "Todos":
    if rent_filter == "Sinceramente Positiva (>0%)":
        filtered_df = filtered_df[filtered_df["rent_val"] > 0]
    elif rent_filter == "Alta (>3%)":
        filtered_df = filtered_df[filtered_df["rent_val"] > 3]
    elif rent_filter == "Muy Alta (>5%)":
        filtered_df = filtered_df[filtered_df["rent_val"] > 5]
    elif rent_filter == "Negativa (<0%)":
        filtered_df = filtered_df[filtered_df["rent_val"] < 0]

if riesgo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["riesgo"] == riesgo_filter]

# ==========================================================
# GESTIÓN DE SELECCIÓN (Hasta 4)
# ==========================================================
if "compare_isins_etfs" not in st.session_state:
    st.session_state.compare_isins_etfs = []

if "page_num_comp_etfs" not in st.session_state:
    st.session_state.page_num_comp_etfs = 1

rows_per_page = 15
total_rows = len(filtered_df)
total_pages = max(1, math.ceil(total_rows / rows_per_page))

if st.session_state.page_num_comp_etfs > total_pages:
    st.session_state.page_num_comp_etfs = 1

current_page = st.session_state.page_num_comp_etfs
start_idx = (current_page - 1) * rows_per_page
end_idx = start_idx + rows_per_page

page_df = filtered_df.iloc[start_idx:end_idx].copy()
page_df.insert(0, "Comparar", page_df["isin"].isin(st.session_state.compare_isins_etfs))

# ==========================================================
# SECCIÓN 2: TABLA SELECCIÓN
# ==========================================================
st.markdown(f"### 📋 Selecciona ETFs ({total_rows} encontrados)")
st.caption(f"Seleccionados: {len(st.session_state.compare_isins_etfs)} / 4 (Máximo)")

edited_df = st.data_editor(
    page_df,
    width="stretch",
    hide_index=True,
    height=38 + (rows_per_page * 35),
    column_config={
        "Comparar": st.column_config.CheckboxColumn("Seleccionar", default=False),
        "nombreEtf": st.column_config.TextColumn("Nombre", width="large"),
        "isin": st.column_config.TextColumn("ISIN", width="medium"),
        "ter": st.column_config.TextColumn("TER", width="small"),
        "yield_1y": st.column_config.TextColumn("Rent. 1A", width="small")
    },
    disabled=["isin", "nombreEtf", "tipoEtf", "riesgo", "yield_1y", "ter"],
    key=f"editor_cmp_etfs_{current_page}"
)

# Sincronización Estado
for idx, row in edited_df.iterrows():
    isin = row["isin"]
    is_checked = row["Comparar"]
    
    if is_checked and isin not in st.session_state.compare_isins_etfs:
        if len(st.session_state.compare_isins_etfs) < 4:
            st.session_state.compare_isins_etfs.append(isin)
        else:
            st.toast("⚠️ Máximo 4 ETFs permitidos.", icon="🛑")
            st.rerun()
            
    elif not is_checked and isin in st.session_state.compare_isins_etfs:
        st.session_state.compare_isins_etfs.remove(isin)

# --- PANEL DE CONTROL TABLA ---
c_reset, c_spacer, c_pag = st.columns([2, 4, 3])

with c_reset:
    if st.button("🗑️ Limpiar Selección", key="clear_etf_comp"):
        st.session_state.compare_isins_etfs = []
        st.rerun()

with c_pag:
    cols = st.columns(5)
    if cols[0].button("⏮", key="c_first_e"): st.session_state.page_num_comp_etfs = 1; st.rerun()
    if cols[1].button("◀", key="c_prev_e") and current_page > 1: st.session_state.page_num_comp_etfs -= 1; st.rerun()
    cols[2].markdown(f"<div style='text-align:center; padding-top:5px'>{current_page}/{total_pages}</div>", unsafe_allow_html=True)
    if cols[3].button("▶", key="c_next_e") and current_page < total_pages: st.session_state.page_num_comp_etfs += 1; st.rerun()
    if cols[4].button("⏭", key="c_last_e"): st.session_state.page_num_comp_etfs = total_pages; st.rerun()

st.divider()

# ==========================================================
# SECCIÓN 3: COMPARATIVA (DETALLE)
# ==========================================================
selected_isins = st.session_state.compare_isins_etfs

if len(selected_isins) > 0:
    st.header("⚖️ Comparativa Cara a Cara")
    
    # Recuperar datos completos
    etfs_data = list(etfs_collection.find({"isin": {"$in": selected_isins}}, {"_id": 0}))
    
    # Ordenar
    e_map = {e['isin']: e for e in etfs_data}
    ordered_etfs = [e_map[isin] for isin in selected_isins if isin in e_map]
    
    # --- 1. TABLA MÉTRICAS CLAVE ---
    st.subheader("📊 Datos Fundamentales")
    
    metrics_map = {
        "Categoría": lambda x: x.get("tipoEtf", "-"),
        "Coste (TER)": lambda x: x.get("ter", "-"),
        "Yield to Maturity": lambda x: f"{x.get('yield_to_maturity', '-')}%",
        "Duración Efectiva": lambda x: f"{x.get('duracion_efectiva', '-')} años",
        "Calidad Crediticia": lambda x: x.get('calidad_crediticia', '-'),
        "Riesgo (SRI)": lambda x: x.get('riesgo', '-'),
        "Volatilidad (3A)": lambda x: x.get('volatility_3y', '-'),
        "Rentabilidad 1A": lambda x: x.get('yield_1y', '-'),
        "Rentabilidad 3A": lambda x: x.get('yield_3y', '-'),
        "Rentabilidad 5A": lambda x: x.get('yield_5y', '-')
    }

    table_data = []
    for m_label, m_func in metrics_map.items():
        row = {"Métrica": m_label}
        for e in ordered_etfs:
            name = (e.get('nombreEtf') or 'Sin Nombre')[:20] + "..." 
            try:
                val = m_func(e)
                if "None" in str(val) or val is None: val = "-"
                row[name] = val
            except:
                row[name] = "-"
        table_data.append(row)

    st.dataframe(pd.DataFrame(table_data), width="stretch", hide_index=True)

    # --- 2. GESTIÓN DE DATOS VISUALES ---
    st.subheader("🕸️ Perfil Visual (Individual)")
    
    def get_num(val):
        if val is None: return 0
        if isinstance(val, (int, float)): return val
        if isinstance(val, str):
            try: return float(val.replace("%", "").replace(",", ".").replace("+", ""))
            except: return 0
        return 0

    radar_labels = ["Yield (TIR)", "Duración", "Volatilidad", "Ratio Rent/Riesgo", "Retorno 1A"]
    metric_extractors = {
        "Yield (TIR)": lambda x: x.get('yield_to_maturity'),
        "Duración": lambda x: x.get('duracion_efectiva'),
        "Volatilidad": lambda x: x.get('volatility_3y'),
        "Ratio Rent/Riesgo": lambda x: x.get('return_per_risk_3y'),
        "Retorno 1A": lambda x: x.get('yield_1y'),
    }
    
    max_vals = {m: 0.1 for m in radar_labels}
    etfs_metrics = {}

    for e in ordered_etfs:
        isin = e['isin']
        vals = {}
        for m in radar_labels:
            raw = metric_extractors[m](e)
            val = get_num(raw)
            vals[m] = val
            if val > max_vals[m]: max_vals[m] = val
        etfs_metrics[isin] = vals

    # --- 3. GRÁFICOS RADAR ---
    cols_radar = st.columns(len(ordered_etfs))
    custom_colors = ['#1f77b4', '#d62728', '#2ca02c', '#9467bd']

    for idx, e in enumerate(ordered_etfs):
        isin = e['isin']
        name = (e.get('nombreEtf') or 'Desc')[:15]
        
        r_vals = []
        hover_txt = []
        for m in radar_labels:
            limit = max_vals[m]
            val = etfs_metrics[isin][m]
            norm = val / limit if limit > 0 else 0
            r_vals.append(norm)
            hover_txt.append(f"{m}: {val}")
        
        r_vals.append(r_vals[0])
        theta = radar_labels + [radar_labels[0]]
        color = custom_colors[idx % len(custom_colors)]

        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=r_vals,
            theta=theta,
            fill='toself',
            name=name,
            line=dict(color=color),
            hovertemplate="%{text}<extra></extra>",
            text=hover_txt + [hover_txt[0]]
        ))
        
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, showticklabels=False, range=[0, 1])),
            height=300,
            margin=dict(t=30, b=30, l=30, r=30),
            showlegend=False,
            title=dict(text=name, font=dict(size=14), x=0.5)
        )
        
        with cols_radar[idx]:
            st.plotly_chart(fig, width="stretch")

    # --- 4. COMPARATIVA POR PERIODOS ---
    st.divider()
    st.markdown("##### 📈 Comparativa de Rentabilidades Separadas")

    col_1a, col_3a, col_5a = st.columns(3)

    for idx, (col, period, key) in enumerate(zip([col_1a, col_3a, col_5a], ["1 Año", "3 Años", "5 Años"], ["yield_1y", "yield_3y", "yield_5y"])):
        with col:
            st.markdown(f"**Rentabilidad {period}**")
            data_period = {}
            for e in ordered_etfs:
                name = (e.get('nombreEtf') or 'Desc')[:15] + "..."
                data_period[name] = get_num(e.get(key))
            
            if any(v != 0 for v in data_period.values()):
                st.bar_chart(pd.Series(data_period), color="#4a6fa5")
            else:
                st.info(f"Sin datos para {period}")

else:
    st.info("👆 Selecciona ETFs arriba para comparar.")
