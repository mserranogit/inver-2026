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

if "success_msg" in st.session_state:
    st.success(st.session_state.success_msg)
    del st.session_state.success_msg

# ==========================================================
# ESTILOS
# ==========================================================
# Estilos movidos a styles.py (apply_styles())

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
# FILTROS (Encima de la Tabla)
# ==========================================================
filter_container = st.container()
with filter_container:
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    
    with col_f1:
        isin_input = st.text_input("🔍 Buscar por ISIN", placeholder="Escriba ISIN...").strip()
    
    with col_f2:
        tipo_filter = st.selectbox(
            "📊 Tipo RF",
            ["Todos"] + sorted(df["tipo_rf"].dropna().unique())
        )
    
    with col_f3:
        tramo_filter = st.selectbox(
            "⏳ Tramo RF",
            ["Todos"] + sorted(df["tramo_rf"].dropna().unique())
        )
    
    with col_f4:
        sensibilidad_filter = st.selectbox(
            "⚖️ Sensibilidad",
            ["Todos"] + sorted(df["sensibilidad"].dropna().unique())
        )

filtered_df = df.copy()

if isin_input:
    filtered_df = filtered_df[filtered_df["isin"].str.contains(isin_input, case=False, na=False)]

if tipo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tipo_rf"] == tipo_filter]

if tramo_filter != "Todos":
    filtered_df = filtered_df[filtered_df["tramo_rf"] == tramo_filter]

if sensibilidad_filter != "Todos":
    filtered_df = filtered_df[filtered_df["sensibilidad"] == sensibilidad_filter]

st.markdown(f"### 📌 Fondos disponibles tras filtros: **{len(filtered_df)}**")
st.markdown("---")

# ==========================================================
# ESTADO PAGINACIÓN
# ==========================================================
total_rows = len(filtered_df)

if "page_number" not in st.session_state:
    st.session_state.page_number = 1

if "seleccion_global" not in st.session_state:
    st.session_state.seleccion_global = {}

if "rows_per_page" not in st.session_state:
    st.session_state.rows_per_page = 10

rows_per_page = st.session_state.rows_per_page
total_pages = max(1, math.ceil(total_rows / rows_per_page))

if st.session_state.page_number > total_pages:
    st.session_state.page_number = 1

current = st.session_state.page_number

start = (current - 1) * rows_per_page
end = start + rows_per_page

page_df = filtered_df.iloc[start:end].copy()
page_df.insert(0, "Seleccionar", page_df["isin"].isin(st.session_state.seleccion_global.keys()))

# ==========================================================
# TABLA
# ==========================================================
row_height = 35
header_height = 38
table_height = header_height + (rows_per_page * row_height)

edited_df = st.data_editor(
    page_df,
    width="stretch",
    hide_index=True,
    height=table_height,
    disabled=["isin", "nombre", "tipo_rf", "tramo_rf", "duration", "sensibilidad"],
    key=f"data_editor_{current}_{rows_per_page}"
)

# ==========================================================
# CONTROLES DEBAJO DE TABLA (MISMA FILA)
# ==========================================================
left_col, spacer_col, right_col = st.columns([2, 6, 4])

# ---------- Combo pequeño izquierda ----------
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
        index=[10, 15, 20, 30, 50].index(st.session_state.rows_per_page),
        key="rows_selector_bottom",
        label_visibility="collapsed"
    )

    if new_value != st.session_state.rows_per_page:
        st.session_state.rows_per_page = new_value
        st.session_state.page_number = 1
        st.rerun()

# ---------- Paginación derecha ----------
with right_col:

    pag_cols = st.columns(8)

    if pag_cols[0].button("⏮", disabled=(current == 1), key=f"first_{current}"):
        st.session_state.page_number = 1
        st.rerun()

    if pag_cols[1].button("◀", disabled=(current == 1), key=f"prev_{current}"):
        st.session_state.page_number -= 1
        st.rerun()

    max_visible = 3
    start_page = max(1, current - 1)
    end_page = min(total_pages, start_page + max_visible - 1)

    page_range = range(start_page, end_page + 1)

    col_index = 2

    for p in page_range:
        if p == current:
            pag_cols[col_index].markdown(
                f"<div class='page-active'>{p}</div>",
                unsafe_allow_html=True
            )
        else:
            if pag_cols[col_index].button(str(p), key=f"page_{p}_{current}"):
                st.session_state.page_number = p
                st.rerun()
        col_index += 1

    if pag_cols[col_index].button("▶", disabled=(current == total_pages), key=f"next_{current}"):
        st.session_state.page_number += 1
        st.rerun()
    col_index += 1

    if pag_cols[col_index].button("⏭", disabled=(current == total_pages), key=f"last_{current}"):
        st.session_state.page_number = total_pages
        st.rerun()

# ==========================================================
# INFO RANGO
# ==========================================================
start_row = (current - 1) * rows_per_page + 1
end_row = min(current * rows_per_page, total_rows)

st.markdown(
    f"<div class='pagination-info' style='text-align:right;'>"
    f"Mostrando {start_row}-{end_row} de {total_rows} fondos"
    f"</div>",
    unsafe_allow_html=True
)

# ==========================================================
# ACTUALIZAR ESTADO GLOBAL SEGÚN TABLA
# ==========================================================


for _, row in edited_df.iterrows():
    isin = row["isin"]

    if row["Seleccionar"]:
        st.session_state.seleccion_global[isin] = row["nombre"]
    else:
        if isin in st.session_state.seleccion_global:
            del st.session_state.seleccion_global[isin]

selected_isins = list(st.session_state.seleccion_global.keys())
selected_count = len(selected_isins)

# ==========================================================
# RESUMEN
# ==========================================================
st.markdown("### 📊 Resumen Selección")
st.write(f"Total seleccionados: **{selected_count}**")

if selected_count > 0:
    selected_fondos = df[df["isin"].isin(selected_isins)]
    tramo_counts = selected_fondos["tramo_rf"].value_counts().to_dict()

    for tramo, count in tramo_counts.items():
        st.write(f"- {tramo}: {count}")

# ==========================================================
# CREAR / RESET CARTERA
# ==========================================================
st.markdown("---")

col1, col_spacer, col2 = st.columns([2,6,2])

# ---------- Crear cartera ----------
with col1:
    if selected_count > 0:
        if st.button("💼 Crear Cartera"):

            cartera_id = "CART-" + datetime.now().strftime("%Y%m%d-%H%M%S")

            # Asegurar tipos nativos de Python para Mongo
            fondos_lista = []
            for isin in selected_isins:
                fondos_lista.append({
                    "isin": str(isin),
                    "nombre": str(st.session_state.seleccion_global[isin])
                })

            cartera_doc = {
                "cartera_id": cartera_id,
                "fecha_creacion": datetime.now(UTC),
                "origen": "M",
                "fondos": fondos_lista
            }
            
            try:
                result = carteras_collection.insert_one(cartera_doc)

                if result.inserted_id:
                    # Guardar mensaje para mostrar tras recarga
                    # Obtener info de conexión para depuración
                    try:
                        conn_info = carteras_collection.database.client.address
                        db_name = carteras_collection.database.name
                    except:
                        conn_info = "Desconocido"
                        db_name = "Desconocido"
                        
                    st.session_state.success_msg = (
                        f"Cartera {cartera_id} creada correctamente con {len(fondos_lista)} fondos ✅\n\n"
                        f"📂 ID Mongo: {str(result.inserted_id)}\n"
                        f"💽 BD: {db_name} @ {conn_info}"
                    )
                    
                    # LIMPIAR SELECCIÓN
                    st.session_state.seleccion_global = {}
                    st.session_state.page_number = 1
                    st.rerun()
            except Exception as e:
                st.error(f"Error al crear la cartera: {e}")

# ---------- Reset ----------
with col2:
    if selected_count > 0:
        if st.button("🔄 Reset selección"):

            st.session_state.seleccion_global = {}
            st.session_state.page_number = 1
            st.rerun()
