import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, UTC
import math

from styles import apply_styles

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(layout="wide", page_title="Constructor de Cartera ETFs", page_icon="🧮")
apply_styles()
st.title("🧮 Constructor de Cartera ETFs")

if "success_msg_etfs" in st.session_state:
    st.success(st.session_state.success_msg_etfs)
    del st.session_state.success_msg_etfs

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
carteras_collection = db["carteras_etf"]

# ==========================================================
# CARGA DATOS
# ==========================================================
etfs = list(etfs_collection.find({}, {
    "_id": 0,
    "isin": 1,
    "nombreEtf": 1,
    "tipoEtf": 1,
    "ter": 1,
    "yield_1y": 1,
    "riesgo": 1,
    "duracion_efectiva": 1,
    "yield_to_maturity": 1
}))

df = pd.DataFrame(etfs)

if df.empty:
    st.warning("No hay ETFs disponibles en la base de datos.")
    st.stop()

# Helper para parsear rentabilidad
def parse_yield(y):
    if not y or y == "N/A": return 0.0
    try: return float(str(y).replace("%", "").replace("+", ""))
    except: return 0.0

df["rent_val"] = df["yield_1y"].apply(parse_yield)

# ==========================================================
# FILTROS (Encima de la Tabla)
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
        rent_options = ["Todos", "Sinceramente Positiva (>0%)", "Alta (>3%)", "Muy Alta (>5%)", "Negativa (<0%)"]
        rent_filter = st.selectbox("📈 Rentabilidad (1A)", rent_options)
    
    with col_f4:
        # Riesgo Filter (1/7, 2/7, etc)
        riesgo_options = ["Todos"] + sorted([r for r in df["riesgo"].dropna().unique().tolist() if r])
        riesgo_filter = st.selectbox("⚠️ Riesgo", riesgo_options)

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

st.markdown(f"### 📌 ETFs disponibles tras filtros: **{len(filtered_df)}**")
st.markdown("---")

# ==========================================================
# ESTADO PAGINACIÓN Y SELECCIÓN
# ==========================================================
total_rows = len(filtered_df)

if "page_number_etfs_constr" not in st.session_state:
    st.session_state.page_number_etfs_constr = 1

if "seleccion_global_etfs" not in st.session_state:
    st.session_state.seleccion_global_etfs = {}

if "rows_per_page_etfs_constr" not in st.session_state:
    st.session_state.rows_per_page_etfs_constr = 10

rows_per_page = st.session_state.rows_per_page_etfs_constr
total_pages = max(1, math.ceil(total_rows / rows_per_page))

if st.session_state.page_number_etfs_constr > total_pages:
    st.session_state.page_number_etfs_constr = 1

current = st.session_state.page_number_etfs_constr

start = (current - 1) * rows_per_page
end = start + rows_per_page

page_df = filtered_df.iloc[start:end].copy()
page_df.insert(0, "Seleccionar", page_df["isin"].isin(st.session_state.seleccion_global_etfs.keys()))

# ==========================================================
# TABLA PRINCIPAL
# ==========================================================
row_height = 35
header_height = 38
table_height = header_height + (rows_per_page * row_height)

edited_df = st.data_editor(
    page_df,
    width="stretch",
    hide_index=True,
    height=table_height,
    column_config={
        "Seleccionar": st.column_config.CheckboxColumn("Seleccionar", default=False),
        "nombreEtf": st.column_config.TextColumn("Nombre", width="large"),
        "isin": st.column_config.TextColumn("ISIN", width="medium"),
        "tipoEtf": st.column_config.TextColumn("Categoría", width="medium"),
        "riesgo": st.column_config.TextColumn("Riesgo", width="small"),
        "ter": st.column_config.TextColumn("TER", width="small"),
        "yield_1y": st.column_config.TextColumn("Rent. 1A", width="small"),
        "duracion_efectiva": st.column_config.NumberColumn("Dur. Efec.", format="%.2f"),
        "yield_to_maturity": st.column_config.NumberColumn("YTM", format="%.2f%%")
    },
    disabled=["isin", "nombreEtf", "tipoEtf", "riesgo", "ter", "yield_1y", "duracion_efectiva", "yield_to_maturity", "rent_val"],
    key=f"data_editor_etfs_{current}_{rows_per_page}"
)

# ==========================================================
# CONTROLES DEBAJO DE TABLA
# ==========================================================
left_col, spacer_col, right_col = st.columns([2, 6, 4])

with left_col:
    st.markdown(
        """
        <style>
        div[data-testid="stSelectbox"] > div {
            width: 140px !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    new_value = st.selectbox(
        "Filas por página",
        [10, 15, 20, 30, 50],
        index=[10, 15, 20, 30, 50].index(st.session_state.rows_per_page_etfs_constr),
        key="rows_selector_etfs_bottom",
        label_visibility="collapsed"
    )

    if new_value != st.session_state.rows_per_page_etfs_constr:
        st.session_state.rows_per_page_etfs_constr = new_value
        st.session_state.page_number_etfs_constr = 1
        st.rerun()

# ---------- Paginación derecha ----------
with right_col:
    pag_cols = st.columns(8)
    if pag_cols[0].button("⏮", disabled=(current == 1), key=f"first_etf_{current}"):
        st.session_state.page_number_etfs_constr = 1
        st.rerun()
    if pag_cols[1].button("◀", disabled=(current == 1), key=f"prev_etf_{current}"):
        st.session_state.page_number_etfs_constr -= 1
        st.rerun()

    max_visible = 3
    start_page = max(1, current - 1)
    end_page = min(total_pages, start_page + max_visible - 1)
    page_range = range(start_page, end_page + 1)

    col_index = 2
    for p in page_range:
        if p == current:
            pag_cols[col_index].markdown(f"<div class='page-active'>{p}</div>", unsafe_allow_html=True)
        else:
            if pag_cols[col_index].button(str(p), key=f"page_etf_{p}_{current}"):
                st.session_state.page_number_etfs_constr = p
                st.rerun()
        col_index += 1

    if pag_cols[col_index].button("▶", disabled=(current == total_pages), key=f"next_etf_{current}"):
        st.session_state.page_number_etfs_constr += 1
        st.rerun()
    col_index += 1
    if pag_cols[col_index].button("⏭", disabled=(current == total_pages), key=f"last_etf_{current}"):
        st.session_state.page_number_etfs_constr = total_pages
        st.rerun()

# ==========================================================
# INFO RANGO
# ==========================================================
start_row = (current - 1) * rows_per_page + 1
end_row = min(current * rows_per_page, total_rows)

st.markdown(
    f"<div class='pagination-info' style='text-align:right;'>"
    f"Mostrando {start_row}-{end_row} de {total_rows} ETFs"
    f"</div>",
    unsafe_allow_html=True
)

# ==========================================================
# ACTUALIZAR ESTADO GLOBAL
# ==========================================================
# Guardamos un diccionario con ISIN -> Datos básicos para el resumen
for _, row in edited_df.iterrows():
    isin = row["isin"]
    if row["Seleccionar"]:
        st.session_state.seleccion_global_etfs[isin] = {
            "nombre": row["nombreEtf"],
            "tipo": row["tipoEtf"],
            "ter": row["ter"],
            "yield": row["yield_1y"]
        }
    else:
        if isin in st.session_state.seleccion_global_etfs:
            del st.session_state.seleccion_global_etfs[isin]

selected_isins = list(st.session_state.seleccion_global_etfs.keys())
selected_count = len(selected_isins)

# ==========================================================
# RESUMEN DE SELECCIÓN
# ==========================================================
st.markdown("### 📊 Resumen Selección")
st.write(f"Total seleccionados: **{selected_count}**")

if selected_count > 0:
    # Agrupar por tipo para el resumen
    tipos = [data["tipo"] for data in st.session_state.seleccion_global_etfs.values()]
    tipo_counts = pd.Series(tipos).value_counts().to_dict()
    
    cols_res = st.columns(min(selected_count, 4))
    for i, isin in enumerate(selected_isins[:4]):
        data = st.session_state.seleccion_global_etfs[isin]
        with cols_res[i % 4]:
            st.info(f"**{data['nombre'][:30]}...**\n\n{data['tipo']}\n\nTER: {data['ter']}")
    
    if selected_count > 4:
        st.write(f"... y {selected_count - 4} más.")

    st.markdown("##### Desglose por Categoría")
    for tipo, count in tipo_counts.items():
        st.write(f"- **{tipo}**: {count}")

# ==========================================================
# ACCIONES (CREAR / RESET)
# ==========================================================
st.markdown("---")
col1, col_spacer, col2 = st.columns([2,6,2])

# ---------- Crear cartera ----------
with col1:
    if selected_count > 0:
        if st.button("💼 Crear Cartera ETFs", use_container_width=True, type="primary"):
            cartera_id = "CART-ETF-" + datetime.now().strftime("%Y%m%d-%H%M%S")
            
            # Formatear lista de activos
            fondos_lista = []
            for isin in selected_isins:
                info = st.session_state.seleccion_global_etfs[isin]
                fondos_lista.append({
                    "isin": str(isin),
                    "nombre": str(info["nombre"]),
                    "tipo": str(info["tipo"]),
                    "ter": str(info["ter"])
                })

            cartera_doc = {
                "cartera_id": cartera_id,
                "fecha_creacion": datetime.now(UTC),
                "origen": "E",  # E para ETF
                "nombre_personalizado": f"Cartera ETF {datetime.now().strftime('%d/%m/%Y')}",
                "etfs": fondos_lista
            }
            
            try:
                result = carteras_collection.insert_one(cartera_doc)
                if result.inserted_id:
                    st.session_state.success_msg_etfs = (
                        f"Cartera {cartera_id} creada correctamente con {len(fondos_lista)} ETFs ✅\n\n"
                        f"📂 ID Mongo: {str(result.inserted_id)}"
                    )
                    # Limpiar y recargar
                    st.session_state.seleccion_global_etfs = {}
                    st.session_state.page_number_etfs_constr = 1
                    st.rerun()
            except Exception as e:
                st.error(f"Error al crear la cartera: {e}")

# ---------- Reset ----------
with col2:
    if selected_count > 0:
        if st.button("🔄 Reset selección", use_container_width=True):
            st.session_state.seleccion_global_etfs = {}
            st.session_state.page_number_etfs_constr = 1
            st.rerun()
