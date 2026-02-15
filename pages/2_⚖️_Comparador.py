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
st.title("⚖️ Comparador de Fondos")

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
# CARGA DATOS
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

isin_input = st.sidebar.text_input("Buscar por ISIN o Nombre").strip()

tipo_options = ["Todos"] + sorted(df["tipo_rf"].dropna().unique().tolist())
tramo_options = ["Todos"] + sorted(df["tramo_rf"].dropna().unique().tolist())
sensibilidad_options = ["Todos"] + sorted(df["sensibilidad"].dropna().unique().tolist())

tipo_filter = st.sidebar.selectbox("Tipo RF", tipo_options)
tramo_filter = st.sidebar.selectbox("Tramo RF", tramo_options)
sensibilidad_filter = st.sidebar.selectbox("Sensibilidad", sensibilidad_options)

# Aplicar Filtros
filtered_df = df.copy()

if isin_input:
    mask_isin = filtered_df["isin"].str.contains(isin_input, case=False, na=False)
    mask_nombre = filtered_df["nombre"].str.contains(isin_input, case=False, na=False)
    filtered_df = filtered_df[mask_isin | mask_nombre]

if tipo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tipo_rf"] == tipo_filter]

if tramo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tramo_rf"] == tramo_filter]

if sensibilidad_filter != "Todos":
    filtered_df = filtered_df[filtered_df["sensibilidad"] == sensibilidad_filter]


# ==========================================================
# GESTIÓN DE SELECCIÓN (Hasta 4)
# ==========================================================
if "compare_isins" not in st.session_state:
    st.session_state.compare_isins = []

if "page_num_comp" not in st.session_state:
    st.session_state.page_num_comp = 1

rows_per_page = 8
total_rows = len(filtered_df)
total_pages = max(1, math.ceil(total_rows / rows_per_page))

if st.session_state.page_num_comp > total_pages:
    st.session_state.page_num_comp = 1

current_page = st.session_state.page_num_comp
start_idx = (current_page - 1) * rows_per_page
end_idx = start_idx + rows_per_page

page_df = filtered_df.iloc[start_idx:end_idx].copy()

# Columna Checkbox Calculada
page_df.insert(0, "Comparar", page_df["isin"].isin(st.session_state.compare_isins))


# ==========================================================
# SECCIÓN 2: TABLA SELECCIÓN
# ==========================================================
st.markdown(f"### 📋 Selecciona fondos ({total_rows} encontrados)")
st.caption(f"Seleccionados: {len(st.session_state.compare_isins)} / 4 (Máximo)")

edited_df = st.data_editor(
    page_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Comparar": st.column_config.CheckboxColumn("Seleccionar", default=False),
        "nombre": st.column_config.TextColumn("Nombre", width="large"),
        "isin": st.column_config.TextColumn("ISIN", width="medium"),
        "duration": st.column_config.NumberColumn("Duración", format="%.2f años")
    },
    disabled=["isin", "nombre", "tipo_rf", "tramo_rf", "duration", "sensibilidad"],
    key=f"editor_cmp_{current_page}"
)

# Sincronización Estado
for idx, row in edited_df.iterrows():
    isin = row["isin"]
    is_checked = row["Comparar"]
    
    if is_checked and isin not in st.session_state.compare_isins:
        if len(st.session_state.compare_isins) < 4:
            st.session_state.compare_isins.append(isin)
        else:
            st.toast("⚠️ Máximo 4 fondos permitidos.", icon="🛑")
            st.rerun()
            
    elif not is_checked and isin in st.session_state.compare_isins:
        st.session_state.compare_isins.remove(isin)

# --- PANEL DE CONTROL TABLA ---
c_reset, c_spacer, c_pag = st.columns([2, 4, 3])

with c_reset:
    if st.button("🗑️ Limpiar Selección", use_container_width=False):
        st.session_state.compare_isins = []
        st.rerun()

with c_pag:
    cols = st.columns(5)
    if cols[0].button("⏮", key="c_first"): st.session_state.page_num_comp = 1; st.rerun()
    if cols[1].button("◀", key="c_prev") and current_page > 1: st.session_state.page_num_comp -= 1; st.rerun()
    cols[2].markdown(f"<div style='text-align:center; padding-top:5px'>{current_page}/{total_pages}</div>", unsafe_allow_html=True)
    if cols[3].button("▶", key="c_next") and current_page < total_pages: st.session_state.page_num_comp += 1; st.rerun()
    if cols[4].button("⏭", key="c_last"): st.session_state.page_num_comp = total_pages; st.rerun()

st.divider()

# ==========================================================
# SECCIÓN 3: COMPARATIVA (DETALLE)
# ==========================================================
selected_isins = st.session_state.compare_isins

if len(selected_isins) > 0:
    st.header("⚖️ Comparativa Cara a Cara")
    
    # Recuperar datos completos
    fondos_data = list(fondos_collection.find({"isin": {"$in": selected_isins}}, {"_id": 0}))
    
    # Ordenar
    f_map = {f['isin']: f for f in fondos_data}
    ordered_funds = [f_map[isin] for isin in selected_isins if isin in f_map]
    
    # --- 1. TABLA MÉTRICAS CLAVE ---
    st.subheader("📊 Datos Fundamentales")
    
    metrics_map = {
        "Categoría": lambda x: x.get("categoria", "-"),
        "Yield (TIR)": lambda x: f"{x.get('duration', {}).get('yield_to_maturity', '-')}%", 
        "Duración (Años)": lambda x: x.get('duration', {}).get('avg_effective_duration', '-'),
        "Calidad Crediticia": lambda x: x.get('duration', {}).get('avg_credit_quality', '-'),
        "Volatilidad (3A)": lambda x: f"{x.get('riesgo', {}).get('for3Year', {}).get('volatility', '-')}%",
        "Sharpe (3A)": lambda x: x.get('riesgo', {}).get('for3Year', {}).get('sharpe', '-'),
        "Rentabilidad 1 Año": lambda x: f"{x.get('rentabilidad', {}).get('historica', {}).get('1 Year', '-')}%"
    }

    table_data = []
    for m_label, m_func in metrics_map.items():
        row = {"Métrica": m_label}
        for f in ordered_funds:
            name = f.get('nombre', 'Sin Nombre')[:20] + "..." 
            try:
                val = m_func(f)
                if "None" in str(val) or val is None: val = "-"
                row[name] = val
            except:
                row[name] = "-"
        table_data.append(row)

    st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)

    # --- 2. GESTIÓN DE DATOS VISUALES ---
    st.subheader("🕸️ Perfil Visual (Individual)")
    
    def get_num(val):
        if isinstance(val, (int, float)): return val
        if isinstance(val, str):
            try: return float(val.replace("%", "").replace(",", "."))
            except: return 0
        return 0

    radar_labels = ["Yield (TIR)", "Duración", "Volatilidad", "Sharpe", "Retorno 1A"]
    metric_extractors = {
        "Yield (TIR)": lambda x: x.get('duration', {}).get('yield_to_maturity'),
        "Duración": lambda x: x.get('duration', {}).get('avg_effective_duration'),
        "Volatilidad": lambda x: x.get('riesgo', {}).get('for3Year', {}).get('volatility'),
        "Sharpe": lambda x: x.get('riesgo', {}).get('for3Year', {}).get('sharpe'),
        "Retorno 1A": lambda x: x.get('rentabilidad', {}).get('historica', {}).get('1 Year'),
    }
    
    # Calcular Máximos GLOBALES para mantener escala consistente entre gráficos
    max_vals = {m: 0.1 for m in radar_labels}
    funds_metrics = {}

    for f in ordered_funds:
        isin = f['isin']
        vals = {}
        for m in radar_labels:
            raw = metric_extractors[m](f)
            val = get_num(raw)
            vals[m] = val
            if val > max_vals[m]: max_vals[m] = val
        funds_metrics[isin] = vals

    # --- 3. GRÁFICOS RADAR EN FILA HORIZONTAL ---
    # Creamos N columnas según fondos seleccionados
    cols = st.columns(len(ordered_funds))
    
    # Colores Alto Contraste (Azul, Rojo, Verde, Púrpura)
    custom_colors = ['#1f77b4', '#d62728', '#2ca02c', '#9467bd']

    for idx, f in enumerate(ordered_funds):
        isin = f['isin']
        name = f.get('nombre', 'Desc')[:15]
        
        # Datos normalizados
        r_vals = []
        hover_txt = []
        for m in radar_labels:
            limit = max_vals[m]
            val = funds_metrics[isin][m]
            norm = val / limit if limit > 0 else 0
            r_vals.append(norm)
            hover_txt.append(f"{m}: {val}")
        
        # Cerrar loop
        r_vals.append(r_vals[0])
        theta = radar_labels + [radar_labels[0]]
        
        # Color único para este gráfico
        color = custom_colors[idx % len(custom_colors)]

        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=r_vals,
            theta=theta,
            fill='toself',
            name=name,
            line=dict(color=color),
            hovertemplate="%{text}<extra></extra>", # Solo texto limpio
            text=hover_txt + [hover_txt[0]]
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, showticklabels=False, range=[0, 1])
            ),
            height=300, # Un poco más pequeños para que quepan bien
            margin=dict(t=30, b=30, l=30, r=30),
            showlegend=False, # No hace falta leyenda, el título o columna ya lo dice
            title=dict(text=name, font=dict(size=14), x=0.5)
        )
        
        with cols[idx]:
            st.plotly_chart(fig, use_container_width=True)


    # --- 4. GRÁFICO BARRAS (Al final, ancho completo) ---
    st.divider()
    
    returns_list = [funds_metrics[f['isin']]["Retorno 1A"] for f in ordered_funds]
    has_returns = any([abs(r) > 0.01 for r in returns_list])
    
    target_metric = "Retorno 1A" if has_returns else "Yield (TIR)"
    title_chart = "📈 Rentabilidad 1 Año Comparada" if has_returns else "💰 Yield (TIR) Anual Esperada Comparada"
        
    st.markdown(f"##### {title_chart}")
    
    bar_dict = {}
    for f in ordered_funds:
        name = f.get('nombre', 'Desc')[:20]
        val = funds_metrics[f['isin']][target_metric]
        bar_dict[name] = val
        
    if bar_dict:
        st.bar_chart(pd.Series(bar_dict), color="#4a6fa5")
        if target_metric == "Yield (TIR)":
            st.caption("*Mostrando Yield (TIR) porque faltan datos de retorno histórico.*")
    else:
        st.info("Sin datos para comparar.")

else:
    st.info("👆 Selecciona fondos arriba para comparar.")
