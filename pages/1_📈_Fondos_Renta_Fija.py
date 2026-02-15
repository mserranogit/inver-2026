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
st.title("📈 Fondos de Renta Fija - Consulta")

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
fondos_collection = db["fondos"]

# ==========================================================
# CARGA DATOS (Resumen para listado)
# ==========================================================
fondos_cursor = fondos_collection.find({}, {
    "_id": 0,
    "isin": 1,
    "nombre": 1,
    "tipo_rf": 1,
    "tramo_rf": 1,
    "duration.avg_effective_duration": 1,
    "sensibilidad_tipos.nivel": 1
})

df = pd.json_normalize(list(fondos_cursor))

if df.empty:
    st.warning("No hay fondos disponibles en la base de datos.")
    st.stop()

# Renombrar columnas
df.rename(columns={
    "duration.avg_effective_duration": "duration",
    "sensibilidad_tipos.nivel": "sensibilidad"
}, inplace=True)

# ==========================================================
# SECCIÓN 1: FILTROS
# ==========================================================
st.sidebar.header("🔎 Filtros")

isin_input = st.sidebar.text_input("Buscar por ISIN").strip()

tipo_options = ["Todos"] + sorted(df["tipo_rf"].dropna().unique().tolist())
tramo_options = ["Todos"] + sorted(df["tramo_rf"].dropna().unique().tolist())
sensibilidad_options = ["Todos"] + sorted(df["sensibilidad"].dropna().unique().tolist())

tipo_filter = st.sidebar.selectbox("Tipo RF", tipo_options)
tramo_filter = st.sidebar.selectbox("Tramo RF", tramo_options)
sensibilidad_filter = st.sidebar.selectbox("Sensibilidad", sensibilidad_options)

# Aplicar Filtros
filtered_df = df.copy()

if isin_input:
    filtered_df = filtered_df[filtered_df["isin"].str.contains(isin_input, case=False, na=False)]

if tipo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tipo_rf"] == tipo_filter]

if tramo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tramo_rf"] == tramo_filter]

if sensibilidad_filter != "Todos":
    filtered_df = filtered_df[filtered_df["sensibilidad"] == sensibilidad_filter]

st.markdown(f"### 📋 Listado de Fondos ({len(filtered_df)} encontrados)")


# ==========================================================
# GESTIÓN DE PAGINACIÓN Y SELECCIÓN (ÚNICA)
# ==========================================================
if "selected_fund_isin" not in st.session_state:
    st.session_state.selected_fund_isin = None

if "page_number_listado" not in st.session_state:
    st.session_state.page_number_listado = 1
    
rows_per_page = 10
total_rows = len(filtered_df)
total_pages = max(1, math.ceil(total_rows / rows_per_page))

if st.session_state.page_number_listado > total_pages:
    st.session_state.page_number_listado = 1

current_page = st.session_state.page_number_listado
start_idx = (current_page - 1) * rows_per_page
end_idx = start_idx + rows_per_page

page_df = filtered_df.iloc[start_idx:end_idx].copy()
page_df.insert(0, "Seleccionar", page_df["isin"] == st.session_state.selected_fund_isin)


# ==========================================================
# SECCIÓN 2: TABLA DE RESULTADOS
# ==========================================================
column_config = {
    "Seleccionar": st.column_config.CheckboxColumn(
        "Ver Detalle",
        help="Marca para ver el detalle. Solo uno a la vez.",
        default=False
    ),
    "nombre": st.column_config.TextColumn("Nombre Fondo", width="large"),
    "isin": st.column_config.TextColumn("ISIN", width="medium"),
    "tipo_rf": st.column_config.TextColumn("Tipo", width="medium"),
    "duration": st.column_config.NumberColumn("Duración", format="%.2f años"),
    "sensibilidad": st.column_config.TextColumn("Sensibilidad")
}

edited_page_df = st.data_editor(
    page_df,
    use_container_width=True,
    hide_index=True,
    column_config=column_config,
    disabled=["isin", "nombre", "tipo_rf", "tramo_rf", "duration", "sensibilidad"],
    key=f"editor_listado_{current_page}"
)

# Lógica Selección Única
current_selection_mask = edited_page_df["Seleccionar"]
selected_rows = edited_page_df[current_selection_mask]

if not selected_rows.empty:
    new_selected_isin = selected_rows.iloc[-1]["isin"]
    if new_selected_isin != st.session_state.selected_fund_isin:
        st.session_state.selected_fund_isin = new_selected_isin
        st.rerun()
    
    if len(selected_rows) > 1:
        candidates = selected_rows[selected_rows["isin"] != st.session_state.selected_fund_isin]
        if not candidates.empty:
            st.session_state.selected_fund_isin = candidates.iloc[0]["isin"]
        else:
            st.session_state.selected_fund_isin = selected_rows.iloc[0]["isin"]
        st.rerun()

elif st.session_state.selected_fund_isin is not None:
    if st.session_state.selected_fund_isin in page_df["isin"].values:
        st.session_state.selected_fund_isin = None
        st.rerun()

# Paginación UI
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    cols = st.columns(5)
    if cols[0].button("⏮", key="p_first"): st.session_state.page_number_listado = 1; st.rerun()
    if cols[1].button("◀", key="p_prev") and current_page > 1: st.session_state.page_number_listado -= 1; st.rerun()
    cols[2].markdown(f"<div style='text-align:center; padding-top:5px'>Pág. {current_page}/{total_pages}</div>", unsafe_allow_html=True)
    if cols[3].button("▶", key="p_next") and current_page < total_pages: st.session_state.page_number_listado += 1; st.rerun()
    if cols[4].button("⏭", key="p_last"): st.session_state.page_number_listado = total_pages; st.rerun()

st.markdown("---")


# ==========================================================
# SECCIÓN 3: DETALLE DETALLADO
# ==========================================================
if st.session_state.selected_fund_isin:
    isin_detail = st.session_state.selected_fund_isin
    fondo_doc = fondos_collection.find_one({"isin": isin_detail}, {"_id": 0})
    
    if fondo_doc:
        st.markdown(f"## 🔎 {fondo_doc.get('nombre', 'Sin Nombre')}")
        
        divisa = fondo_doc.get('currency', {}).get('base_currency', 'EUR')
        st.caption(f"ISIN: {fondo_doc.get('isin')} | Categoría: {fondo_doc.get('categoria', 'N/A')} | Divisa: {divisa}")
        
        st.divider()

        # Tabs reorganizados
        tab_fundamental, tab_riesgo, tab_rentabilidad, tab_cartera = st.tabs([
            "🏛️ Fundamental", 
            "⚠️ Riesgo",
            "📈 Rentabilidad", 
            "🌍 Composición"
        ])
        
        # ------------------------------------------------------
        # TAB 1: FUNDAMENTAL
        # ------------------------------------------------------
        with tab_fundamental:
            st.markdown("##### 🔍 Métricas de Cartera")
            
            dur_data = fondo_doc.get('duration', {}) or {}
            
            c1, c2, c3 = st.columns(3)
            with c1:
                val = dur_data.get('yield_to_maturity')
                label_val = f"{val}%" if val is not None else "N/A"
                st.metric("Rentabilidad Esperada (YTM)", label_val, help="TIR anual estimada a vencimiento.")
            with c2:
                val = dur_data.get('avg_effective_duration')
                label_val = f"{val} años" if val is not None else "N/A"
                st.metric("Duración Efectiva", label_val, help="Sensibilidad a tipos.")
            with c3:
                val = dur_data.get('avg_credit_quality')
                st.metric("Calidad Crediticia", str(val) if val else "N/A", help="Solvencia media.")

            st.info("💡 **YTM (Yield)** es el mejor predictor de retorno a largo plazo en Renta Fija.", icon="ℹ️")

        # ------------------------------------------------------
        # TAB 2: RIESGO (Visual)
        # ------------------------------------------------------
        with tab_riesgo:
            st.markdown("##### 📉 Perfil de Volatilidad")
            
            riesgo = fondo_doc.get('riesgo', {}) or {}
            dur_val = fondo_doc.get('duration', {}).get('avg_effective_duration')
            
            # Datos Riesgo
            r3y = riesgo.get('for3Year', {})
            r1y = riesgo.get('for1Year', {})
            has_3y = r3y and r3y.get('volatility') is not None
            actual_risk = r3y if has_3y else r1y
            vol_val = actual_risk.get('volatility')
            sharpe_val = actual_risk.get('sharpe')
            periodo_lbl = "3 Años" if has_3y else "1 Año"
            
            c_r1, c_r2 = st.columns(2)
            with c_r1:
                st.metric(f"Volatilidad ({periodo_lbl})", f"{vol_val}%" if vol_val is not None else "N/A")
            with c_r2:
                st.metric(f"Ratio Sharpe ({periodo_lbl})", f"{sharpe_val}" if sharpe_val is not None else "N/A", help="Rentabilidad extra por unidad de riesgo.")
            
            st.divider()
            
            # Gráficos Gauges
            col_g1, col_g2 = st.columns(2)
            
            with col_g1:
                st.caption(f"**Nivel de Sensibilidad (Duración)**: {dur_val} años")
                if dur_val is not None:
                    fig_dur = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = dur_val,
                        gauge = {
                            'axis': {'range': [None, 10]}, 
                            'bar': {'color': "#4a6fa5"},
                            'steps': [
                                {'range': [0, 2], 'color': "#e6f3ff"},
                                {'range': [2, 5], 'color': "#b3d9ff"},
                                {'range': [5, 10], 'color': "#80bfff"}
                            ]
                        }
                    ))
                    fig_dur.update_layout(height=200, margin=dict(l=20, r=20, t=10, b=10))
                    st.plotly_chart(fig_dur, use_container_width=True)

            with col_g2:
                st.caption(f"**Nivel de Riesgo (Volatilidad {periodo_lbl})**: {vol_val}%")
                if vol_val is not None:
                    fig_vol = go.Figure(go.Indicator(
                        mode = "gauge+number",
                        value = vol_val,
                        gauge = {
                            'axis': {'range': [None, 15]}, 
                            'bar': {'color': "#d9534f"},
                            'steps': [
                                {'range': [0, 3], 'color': "#d4edda"},
                                {'range': [3, 7], 'color': "#fff3cd"},
                                {'range': [7, 15], 'color': "#f8d7da"}
                            ]
                        }
                    ))
                    fig_vol.update_layout(height=200, margin=dict(l=20, r=20, t=10, b=10))
                    st.plotly_chart(fig_vol, use_container_width=True)

        # ------------------------------------------------------
        # TAB 3: RENTABILIDAD
        # ------------------------------------------------------
        with tab_rentabilidad:
            st.markdown("##### 📅 Histórico de Rentabilidades")
            hist = fondo_doc.get('rentabilidad', {}).get('historica', {})
            
            if hist:
                df_hist = pd.DataFrame([hist])
                st.dataframe(df_hist, use_container_width=True, hide_index=True)
                
                valid = {k: v for k, v in hist.items() if isinstance(v, (int, float))}
                if valid:
                    st.bar_chart(pd.Series(valid), color="#4a6fa5")
            else:
                st.info("Sin datos de rentabilidad.")

        # ------------------------------------------------------
        # TAB 4: COMPOSICIÓN
        # ------------------------------------------------------
        with tab_cartera:
            st.markdown("##### 🧩 ¿En qué invierte?")
            alloc = fondo_doc.get('allocation_map', {})
            
            if not alloc:
                st.info("No hay datos de desglose disponibles.")
            else:
                col_assets, col_sectors = st.columns(2)
                
                # 1. CLASES DE ACTIVO
                assets = alloc.get('globalAssetClasses', {})
                has_assets = False
                
                if assets and isinstance(assets, dict):
                    clean_assets = {k: v for k, v in assets.items() if isinstance(v, (int, float)) and v > 0.1}
                    if clean_assets:
                        has_assets = True
                        with col_assets:
                            st.subheader("Por Tipo de Activo")
                            df_assets = pd.DataFrame(list(clean_assets.items()), columns=["Tipo", "% Peso"])
                            st.dataframe(
                                df_assets.style.format({"% Peso": "{:.2f}%"}), 
                                hide_index=True, 
                                use_container_width=True
                            )

                # 2. SECTORES
                sectors = alloc.get('fixedIncomeSectors', {})
                has_sectors = False
                
                if sectors and isinstance(sectors, dict):
                    clean_sectors = {k: v for k, v in sectors.items() if isinstance(v, (int, float)) and v > 0.1}
                    
                    if clean_sectors:
                        has_sectors = True
                        with col_sectors:
                            st.subheader("Por Sector de Deuda")
                            sorted_sectors = sorted(clean_sectors.items(), key=lambda x: x[1], reverse=True)
                            
                            final_data = []
                            for k, v in sorted_sectors:
                                label = k
                                if "Government" in k: label = "Deuda Pública (Gobiernos)"
                                elif "Corporate" in k: label = "Deuda Corporativa (Empresas)"
                                elif "Securitized" in k: label = "Titulizaciones (Hipotecas/ABS)"
                                elif "Municipals" in k: label = "Municipales/Regionales"
                                elif "Cash" in k: label = "Liquidez/Derivados"
                                
                                final_data.append({"Sector": label, "% Peso": v})
                            
                            df_sec = pd.DataFrame(final_data)
                            st.dataframe(
                                df_sec.style.format({"% Peso": "{:.2f}%"}), 
                                use_container_width=True, 
                                hide_index=True
                            )
                
                if not has_assets and not has_sectors:
                    st.warning("⚠️ No disponemos del desglose detallado (sectores/activos) para este fondo en concreto.")

    else:
        st.error(f"No se pudo cargar la información del fondo {isin_detail}")
else:
    st.info("👆 Selecciona un fondo (casilla) para ver su análisis.")
