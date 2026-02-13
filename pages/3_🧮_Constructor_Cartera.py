import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, UTC
import math

from styles import apply_styles

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(layout="wide")

apply_styles()
st.title("🧮 Constructor de Cartera")

# ==========================================================
# ESTILOS
# ==========================================================
st.markdown("""
<style>

/* Filtros sidebar */
section[data-testid="stSidebar"] label {
    color: white !important;
    font-weight: 500;
}

/* Texto info paginación */
.pagination-info {
    font-size: 14px;
    color: #6c757d;
    margin-top: 6px;
}

/* Número página activo */
.page-active {
    padding:6px 10px;
    background-color:#4a6fa5;
    color:white;
    border-radius:4px;
    text-align:center;
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
        authSource="admin"
    )
    return client["db-inver"]

db = get_db()
fondos_collection = db["fondos"]
carteras_collection = db["carteras"]

# ==========================================================
# CARGA DATOS
# ==========================================================
fondos = list(fondos_collection.find({}, {
    "_id": 0,
    "isin": 1,
    "nombre": 1,
    "tipo_rf": 1,
    "tramo_rf": 1,
    "duration.avg_effective_duration": 1,
    "sensibilidad_tipos.nivel": 1
}))

df = pd.json_normalize(fondos)

if df.empty:
    st.warning("No hay fondos disponibles.")
    st.stop()

df.rename(columns={
    "duration.avg_effective_duration": "duration",
    "sensibilidad_tipos.nivel": "sensibilidad"
}, inplace=True)

# ==========================================================
# FILTROS SIDEBAR
# ==========================================================
st.sidebar.header("🔎 Filtros")

tipo_filter = st.sidebar.selectbox(
    "Tipo RF",
    ["Todos"] + sorted(df["tipo_rf"].dropna().unique())
)

tramo_filter = st.sidebar.selectbox(
    "Tramo RF",
    ["Todos"] + sorted(df["tramo_rf"].dropna().unique())
)

sensibilidad_filter = st.sidebar.selectbox(
    "Sensibilidad",
    ["Todos"] + sorted(df["sensibilidad"].dropna().unique())
)


# Aplicar filtros
filtered_df = df.copy()

if tipo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tipo_rf"] == tipo_filter]

if tramo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tramo_rf"] == tramo_filter]

if sensibilidad_filter != "Todos":
    filtered_df = filtered_df[filtered_df["sensibilidad"] == sensibilidad_filter]


# ==========================================================
# INFO FILTROS
# ==========================================================
st.markdown(f"### 📌 Fondos disponibles tras filtros: **{len(filtered_df)}**")
st.markdown("---")

# ==========================================================
# PAGINACIÓN (ABAJO)
# ==========================================================
total_rows = len(filtered_df)

if "page_number" not in st.session_state:
    st.session_state.page_number = 1

# ========= FILAS POR PÁGINA =========
rows_per_page = st.session_state.get("rows_per_page", 10)

total_pages = max(1, math.ceil(total_rows / rows_per_page))

if st.session_state.page_number > total_pages:
    st.session_state.page_number = 1

current = st.session_state.page_number

start = (current - 1) * rows_per_page
end = start + rows_per_page

page_df = filtered_df.iloc[start:end].copy()
page_df.insert(0, "Seleccionar", False)

# ==========================================================
# TABLA
# ==========================================================
edited_df = st.data_editor(
    page_df,
    use_container_width=True,
    hide_index=True,
    disabled=["isin", "nombre", "tipo_rf", "tramo_rf", "duration", "sensibilidad"]
)

# ==========================================================
# PAGINACIÓN PROFESIONAL ABAJO
# ==========================================================
st.markdown("")

col_left, col_right = st.columns([3, 9])

# ---------- Combo filas por página ----------
with col_left:
    new_rows = st.selectbox(
        "Filas por página",
        [10, 15, 20, 30, 50],
        index=[10, 15, 20, 30, 50].index(rows_per_page),
        key="rows_selector"
    )
    st.session_state.rows_per_page = new_rows

# ---------- Navegación páginas ----------
with col_right:

    pag_cols = st.columns(12)

    if pag_cols[0].button("⏮", disabled=(current == 1)):
        st.session_state.page_number = 1

    if pag_cols[1].button("◀", disabled=(current == 1)):
        st.session_state.page_number -= 1

    max_visible = 5

    if total_pages <= max_visible:
        page_range = range(1, total_pages + 1)
    else:
        start_page = max(1, current - 2)
        end_page = min(total_pages, current + 2)

        if start_page == 1:
            end_page = max_visible
        if end_page == total_pages:
            start_page = total_pages - max_visible + 1

        page_range = range(start_page, end_page + 1)

    col_index = 2

    for p in page_range:
        if p == current:
            pag_cols[col_index].markdown(
                f"<div class='page-active'>{p}</div>",
                unsafe_allow_html=True
            )
        else:
            if pag_cols[col_index].button(str(p)):
                st.session_state.page_number = p
        col_index += 1

    if pag_cols[col_index].button("▶", disabled=(current == total_pages)):
        st.session_state.page_number += 1
    col_index += 1

    if pag_cols[col_index].button("⏭", disabled=(current == total_pages)):
        st.session_state.page_number = total_pages

# ---------- Info rango ----------
start_row = (current - 1) * rows_per_page + 1
end_row = min(current * rows_per_page, total_rows)

st.markdown(
    f"<div class='pagination-info'>"
    f"Mostrando {start_row}-{end_row} de {total_rows} fondos"
    f"</div>",
    unsafe_allow_html=True
)

# ==========================================================
# GESTIÓN SELECCIÓN
# ==========================================================
if "seleccion_global" not in st.session_state:
    st.session_state.seleccion_global = {}

for _, row in edited_df.iterrows():
    isin = row["isin"]
    if row["Seleccionar"]:
        st.session_state.seleccion_global[isin] = row["nombre"]
    elif isin in st.session_state.seleccion_global:
        del st.session_state.seleccion_global[isin]

selected_isins = list(st.session_state.seleccion_global.keys())
selected_count = len(selected_isins)

# ==========================================================
# RESUMEN SELECCIÓN
# ==========================================================
st.markdown("### 📊 Resumen Selección")
st.write(f"Total seleccionados: **{selected_count}**")

if selected_count > 0:
    selected_fondos = df[df["isin"].isin(selected_isins)]
    tramo_counts = selected_fondos["tramo_rf"].value_counts().to_dict()

    for tramo, count in tramo_counts.items():
        st.write(f"- {tramo}: {count}")

# ==========================================================
# CREAR CARTERA
# ==========================================================
if selected_count > 0:
    if st.button("💼 Crear Cartera"):

        cartera_id = "CART-" + datetime.now().strftime("%Y%m%d-%H%M%S")

        cartera_doc = {
            "cartera_id": cartera_id,
            "fecha_creacion": datetime.now(UTC),
            "fondos": [
                {"isin": isin, "nombre": st.session_state.seleccion_global[isin]}
                for isin in selected_isins
            ]
        }

        carteras_collection.insert_one(cartera_doc)

        st.success(f"Cartera {cartera_id} creada correctamente ✅")
        st.session_state.seleccion_global = {}
