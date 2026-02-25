import streamlit as st
import pandas as pd
from pymongo import MongoClient
import math
import plotly.graph_objects as go
from styles import apply_styles

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(layout="wide")
apply_styles()
st.title("📋 Listado Global de ETFs")

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
# CARGA DATOS (Resumen para listado)
# ==========================================================
etfs_cursor = etfs_collection.find({}, {
    "_id": 0,
    "isin": 1,
    "nombreEtf": 1,
    "tipoEtf": 1,
    "riesgo": 1,
    "ter": 1,
    "yield_1y": 1,
    "calidad_crediticia": 1
})

df = pd.DataFrame(list(etfs_cursor))

if df.empty:
    st.warning("No hay ETFs disponibles en la base de datos.")
    st.stop()

# Eliminar duplicados por ISIN (Limpieza de seguridad)
df = df.drop_duplicates(subset=["isin"]).reset_index(drop=True)

# Limpieza de datos básicos
df["ter"] = df["ter"].fillna("N/A")
df["tipoEtf"] = df["tipoEtf"].fillna("Sin Categoría")

# ==========================================================
# SECCIÓN 1: FILTROS (Encima de la Tabla)
# ==========================================================
filter_container = st.container()
with filter_container:
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    
    with col_f1:
        isin_input = st.text_input("🔍 Buscar por ISIN o Nombre", placeholder="Escriba ISIN o nombre...").strip()
    
    with col_f2:
        tipo_options = ["Todos"] + sorted([t for t in df["tipoEtf"].dropna().unique().tolist() if t])
        tipo_filter = st.selectbox("📊 Tipo ETF", tipo_options)
    
    with col_f3:
        # Rentabilidad Filter (Categorized yield_1y)
        def parse_yield(y):
            if not y or y == "N/A": return 0.0
            try: return float(str(y).name.replace("%", "").replace("+", ""))
            except: 
                try: return float(str(y).replace("%", "").replace("+", ""))
                except: return 0.0

        df["rent_val"] = df["yield_1y"].apply(parse_yield)
        rent_options = ["Todos", "Sinceramente Positiva (>0%)", "Alta (>3%)", "Muy Alta (>5%)", "Negativa (<0%)"]
        rent_filter = st.selectbox("📈 Rentabilidad (1A)", rent_options)
    
    with col_f4:
        # Riesgo Filter (1/7, 2/7, etc)
        riesgo_options = ["Todos"] + sorted([r for r in df["riesgo"].dropna().unique().tolist() if r])
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

st.markdown(f"### 📋 Listado de ETFs ({len(filtered_df)} encontrados)")

# ==========================================================
# GESTIÓN DE PAGINACIÓN Y SELECCIÓN (ÚNICA)
# ==========================================================
if "selected_etf_isin" not in st.session_state:
    st.session_state.selected_etf_isin = None

if "page_number_etfs" not in st.session_state:
    st.session_state.page_number_etfs = 1

if "rows_per_page_etfs" not in st.session_state:
    st.session_state.rows_per_page_etfs = 10

rows_per_page = st.session_state.rows_per_page_etfs
total_rows = len(filtered_df)
total_pages = max(1, math.ceil(total_rows / rows_per_page))

if st.session_state.page_number_etfs > total_pages:
    st.session_state.page_number_etfs = 1

current_page = st.session_state.page_number_etfs
start_idx = (current_page - 1) * rows_per_page
end_idx = start_idx + rows_per_page

page_df = filtered_df.iloc[start_idx:end_idx].copy()
page_df.insert(0, "Seleccionar", page_df["isin"] == st.session_state.selected_etf_isin)

# ==========================================================
# SECCIÓN 2: TABLA DE RESULTADOS
# ==========================================================
column_config = {
    "Seleccionar": st.column_config.CheckboxColumn(
        "Ver Detalle",
        help="Marca para ver el detalle. Solo uno a la vez.",
        default=False
    ),
    "nombreEtf": st.column_config.TextColumn("Nombre ETF", width="large"),
    "isin": st.column_config.TextColumn("ISIN", width="medium"),
    "tipoEtf": st.column_config.TextColumn("Categoría", width="medium"),
    "riesgo": st.column_config.TextColumn("Riesgo", width="small"),
    "ter": st.column_config.TextColumn("TER", width="small"),
    "yield_1y": st.column_config.TextColumn("Rent. 1A", width="small")
}

row_height = 35
header_height = 38
table_height = header_height + (rows_per_page * row_height)

edited_page_df = st.data_editor(
    page_df,
    width="stretch",
    hide_index=True,
    height=table_height,
    column_config=column_config,
    disabled=["isin", "nombreEtf", "tipoEtf", "ter", "riesgo", "yield_1y"],
    key=f"editor_etfs_{current_page}_{rows_per_page}"
)

# Lógica Selección Única (Adaptada de fondos)
current_selection_mask = edited_page_df["Seleccionar"]
selected_rows = edited_page_df[current_selection_mask]

if not selected_rows.empty:
    new_selected_isin = selected_rows.iloc[-1]["isin"]
    if new_selected_isin != st.session_state.selected_etf_isin:
        st.session_state.selected_etf_isin = new_selected_isin
        st.rerun()
    
    if len(selected_rows) > 1:
        candidates = selected_rows[selected_rows["isin"] != st.session_state.selected_etf_isin]
        if not candidates.empty:
            st.session_state.selected_etf_isin = candidates.iloc[0]["isin"]
        else:
            st.session_state.selected_etf_isin = selected_rows.iloc[0]["isin"]
        st.rerun()

elif st.session_state.selected_etf_isin is not None:
    if st.session_state.selected_etf_isin in page_df["isin"].values:
        st.session_state.selected_etf_isin = None
        st.rerun()

# ==========================================================
# CONTROLES DEBAJO DE TABLA
# ==========================================================
left_col, spacer_col, right_col = st.columns([2, 5, 5])

with left_col:
    new_value_etfs = st.selectbox(
        "Filas por página",
        [10, 15, 20, 30, 50],
        index=[10, 15, 20, 30, 50].index(st.session_state.rows_per_page_etfs),
        key="rows_selector_etfs",
        label_visibility="collapsed"
    )
    if new_value_etfs != st.session_state.rows_per_page_etfs:
        st.session_state.rows_per_page_etfs = new_value_etfs
        st.session_state.page_number_etfs = 1
        st.rerun()

# ---------- Paginación derecha ----------
with right_col:
    pag_cols = st.columns(8)

    if pag_cols[0].button("⏮", disabled=(current_page == 1), key=f"first_etfs_{current_page}"):
        st.session_state.page_number_etfs = 1
        st.rerun()

    if pag_cols[1].button("◀", disabled=(current_page == 1), key=f"prev_etfs_{current_page}"):
        st.session_state.page_number_etfs -= 1
        st.rerun()

    max_visible = 3
    p_start = max(1, current_page - 1)
    p_end = min(total_pages, p_start + max_visible - 1)

    col_index = 2
    for p in range(p_start, p_end + 1):
        if p == current_page:
            pag_cols[col_index].markdown(
                f"<div class='page-active'>{p}</div>",
                unsafe_allow_html=True
            )
        else:
            if pag_cols[col_index].button(str(p), key=f"page_etfs_{p}_{current_page}"):
                st.session_state.page_number_etfs = p
                st.rerun()
        col_index += 1

    if pag_cols[col_index].button("▶", disabled=(current_page == total_pages), key=f"next_etfs_{current_page}"):
        st.session_state.page_number_etfs += 1
        st.rerun()
    col_index += 1

    if pag_cols[col_index].button("⏭", disabled=(current_page == total_pages), key=f"last_etfs_{current_page}"):
        st.session_state.page_number_etfs = total_pages
        st.rerun()

# ==========================================================
# INFO RANGO
# ==========================================================
start_row_lbl = (current_page - 1) * rows_per_page + 1
end_row_lbl = min(current_page * rows_per_page, total_rows)

st.markdown(
    f"<div class='pagination-info' style='text-align:right;'>"
    f"Mostrando {start_row_lbl}-{end_row_lbl} de {total_rows} ETFs"
    f"</div>",
    unsafe_allow_html=True
)

# ==========================================================
# SECCIÓN 3: DETALLE ETFS
# ==========================================================
if st.session_state.selected_etf_isin:
    isin_detail = st.session_state.selected_etf_isin
    etf_doc = etfs_collection.find_one({"isin": isin_detail}, {"_id": 0})
    
    if etf_doc:
        st.markdown(f"## 🔎 {etf_doc.get('nombreEtf', 'Sin Nombre')}")
        st.caption(f"ISIN: {etf_doc.get('isin')} | Categoría: {etf_doc.get('tipoEtf', 'N/A')} | JustETF Link: [Open](https://www.justetf.com/en/etf-profile.html?isin={isin_detail})")
        
        st.divider()

        tab_fund, tab_riesgo, tab_rent, tab_extra = st.tabs([
            "🏛️ Fundamental", 
            "⚠️ Riesgo",
            "📈 Rentabilidad", 
            "➕ Detalles"
        ])
        
        with tab_fund:
            st.markdown("##### 🔍 Métricas Financieras")
            c1, c2, c3 = st.columns(3)
            with c1:
                val = etf_doc.get('yield_to_maturity')
                st.metric("Yield to Maturity (YTM)", f"{val}%" if val else "N/A", help="Rentabilidad estimada si se mantienen activos.")
            with c2:
                val = etf_doc.get('duracion_efectiva')
                st.metric("Duración Efectiva", f"{val} años" if val else "N/A")
            with c3:
                val = etf_doc.get('calidad_crediticia')
                st.metric("Calidad Crediticia", str(val) if val else "N/A")

            st.markdown("---")
            c4, c5 = st.columns(2)
            with c4:
                st.write("**Réplica:**", etf_doc.get('replication_method', 'N/A'))
                st.write("**Coste (TER):**", etf_doc.get('ter', 'N/A'))
            with c5:
                st.write("**Política Divs:**", etf_doc.get('dividend_policy', 'N/A'))
                st.write("**Tamaño Fondo:**", etf_doc.get('fund_size', 'N/A'))

        with tab_riesgo:
            st.markdown("##### 📉 Métricas de Riesgo (3 Años)")
            cr1, cr2, cr3 = st.columns(3)
            with cr1:
                val = etf_doc.get('volatility_3y')
                st.metric("Volatilidad (3A)", val if val else "N/A")
            with cr2:
                val = etf_doc.get('max_drawdown_3y')
                st.metric("Max Drawdown (3A)", val if val else "N/A")
            with cr3:
                val = etf_doc.get('return_per_risk_3y')
                st.metric("Ratio Rent/Riesgo (Sharpe)", val if val else "N/A")

            # Gauges (Estilo Premium)
            col_g1, col_g2 = st.columns(2)
            
            # Limpiar valor de volatilidad para el gauge (ej: "5.23%" -> 5.23)
            vol_raw = etf_doc.get('volatility_3y', "0")
            if vol_raw and isinstance(vol_raw, str):
                try: vol_val = float(vol_raw.replace("%", "").replace("+", ""))
                except: vol_val = 0
            else: vol_val = float(vol_raw or 0)

            with col_g1:
                st.caption(f"**Nivel de Riesgo (Volatilidad)**: {vol_raw}")
                fig_vol = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = vol_val,
                    gauge = {
                        'axis': {'range': [0, 20]}, 
                        'bar': {'color': "#d9534f"},
                        'steps': [
                            {'range': [0, 3], 'color': "#d4edda"},
                            {'range': [3, 8], 'color': "#fff3cd"},
                            {'range': [8, 20], 'color': "#f8d7da"}
                        ]
                    }
                ))
                fig_vol.update_layout(height=220, margin=dict(l=20, r=20, t=30, b=20), paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_vol, width="stretch")

            with col_g2:
                # Duración en gauge
                dur_val = etf_doc.get('duracion_efectiva', 0)
                st.caption(f"**Sensibilidad Tipos (Duración)**: {dur_val} años")
                fig_dur = go.Figure(go.Indicator(
                    mode = "gauge+number",
                    value = dur_val or 0,
                    gauge = {
                        'axis': {'range': [0, 10]}, 
                        'bar': {'color': "#4a6fa5"},
                        'steps': [
                            {'range': [0, 2], 'color': "#e6f3ff"},
                            {'range': [2, 5], 'color': "#b3d9ff"},
                            {'range': [5, 10], 'color': "#80bfff"}
                        ]
                    }
                ))
                fig_dur.update_layout(height=220, margin=dict(l=20, r=20, t=30, b=20), paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_dur, width="stretch")

        with tab_rent:
            st.markdown("##### 📅 Rendimientos Históricos")
            
            # Construir tabla de retornos
            retornos = {
                "Periodo": ["1 Año", "3 Años", "5 Años"],
                "Rentabilidad": [
                    etf_doc.get('yield_1y', 'N/A'),
                    etf_doc.get('yield_3y', 'N/A'),
                    etf_doc.get('yield_5y', 'N/A')
                ]
            }
            df_ret = pd.DataFrame(retornos)
            st.table(df_ret)
            
            # Gráfico de barras simple
            def clean_pct(x):
                if not x or x == 'N/A': return 0
                try: return float(str(x).replace("%", "").replace("+", ""))
                except: return 0
            
            bar_data = {
                "1A": clean_pct(etfs_collection.find_one({"isin": isin_detail}).get('yield_1y')),
                "3A": clean_pct(etfs_collection.find_one({"isin": isin_detail}).get('yield_3y')),
                "5A": clean_pct(etfs_collection.find_one({"isin": isin_detail}).get('yield_5y'))
            }
            st.bar_chart(pd.Series(bar_data), color="#4a6fa5")

        with tab_extra:
            st.markdown("##### 🛠️ Datos Adicionales")
            col_e1, col_e2 = st.columns(2)
            with col_e1:
                st.write("**Vencimiento Efectivo:**", f"{etf_doc.get('vencimiento_efectivo')} años")
                st.write("**Cupón Medio:**", etf_doc.get('cupon_medio', 'N/A'))
            with col_e2:
                st.write("**Última Actualización:**", etf_doc.get('last_update_justetf', 'N/A'))
                st.write("**Métricas Bonos al:**", etf_doc.get('fecha_datos_bonos', 'N/A'))

    else:
        st.error(f"No se pudo cargar la información del ETF {isin_detail}")
else:
    st.info("👆 Selecciona un ETF (casilla) para ver su análisis detallado.")
