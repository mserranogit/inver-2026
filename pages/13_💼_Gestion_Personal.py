import streamlit as st
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import date, datetime
from styles import apply_styles

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(layout="wide", page_title="Gestión de Mi Cartera", page_icon="💼")
apply_styles()

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
mi_cartera = db["mi_cartera"]

# ==========================================================
# FUNCIONES CRUD
# ==========================================================
def fetch_data(tipo):
    docs = list(mi_cartera.find({"tipo": tipo}))
    for d in docs:
        d["id_str"] = str(d["_id"])
    return docs

def save_data(data_dict, doc_id=None):
    if doc_id:
        mi_cartera.update_one({"_id": ObjectId(doc_id)}, {"$set": data_dict})
    else:
        mi_cartera.insert_one(data_dict)

def delete_data(doc_id):
    mi_cartera.delete_one({"_id": ObjectId(doc_id)})

# ==========================================================
# INTERFAZ
# ==========================================================
st.title("💼 Gestión de Mi Cartera")

fondos_data = fetch_data("FON")
etfs_data = fetch_data("ETF")

if "crud_action" not in st.session_state:
    st.session_state.crud_action = None
if "crud_type" not in st.session_state:
    st.session_state.crud_type = None

def render_table_and_buttons(data_list, title, tipo):
    st.subheader(title)
    selected_item = None
    
    if not data_list:
        st.info(f"No hay {title.lower()} guardados.")
    else:
        df = pd.DataFrame(data_list)
        df.insert(0, "Seleccionar", False)
        
        # Formatear fechas para mejor visualización
        df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"]).dt.strftime("%d/%m/%Y")
        df["fecha_fin"] = pd.to_datetime(df["fecha_fin"]).dt.strftime("%d/%m/%Y").fillna("Sin fin")
        
        # Reordenar y limpiar
        cols = ["Seleccionar", "id_str", "ISIN", "nombre", "capital_invertido", "importe_gastos", "rentabilidad_esperada", "fecha_inicio", "fecha_fin"]
        df = df[[c for c in cols if c in df.columns]]
        
        edited_df = st.data_editor(
            df,
            column_config={
                "id_str": None,
                "Seleccionar": st.column_config.CheckboxColumn("Sel.", default=False),
                "ISIN": "ISIN",
                "nombre": "Nombre",
                "capital_invertido": st.column_config.NumberColumn("C. Invertido", format="%.2f"),
                "importe_gastos": st.column_config.NumberColumn("Gastos", format="%.2f"),
                "rentabilidad_esperada": st.column_config.NumberColumn("% Rent.", format="%.2f %%"),
                "fecha_inicio": "Inicio",
                "fecha_fin": "Fin"
            },
            disabled=["id_str", "ISIN", "nombre", "capital_invertido", "importe_gastos", "rentabilidad_esperada", "fecha_inicio", "fecha_fin"],
            hide_index=True,
            key=f"tabla_{title}"
        )
        
        selected_rows = edited_df[edited_df["Seleccionar"]]
        if not selected_rows.empty:
            sel_id = selected_rows.iloc[0]["id_str"]
            for d in data_list:
                if d["id_str"] == sel_id:
                    selected_item = d
                    break
    
    # Botones
    st.write("")
    _, b1, b2, b3, _ = st.columns([3, 1, 1, 1, 3])
    
    if b1.button("Nueva alta", key=f"btn_n_{tipo}", use_container_width=True):
        st.session_state.crud_action = "Nuevo"
        st.session_state.crud_type = tipo
        st.session_state.selected_item = None
        st.rerun()
        
    if b2.button("Editar", key=f"btn_e_{tipo}", disabled=(selected_item is None), use_container_width=True):
        st.session_state.crud_action = "Editar"
        st.session_state.crud_type = tipo
        st.session_state.selected_item = selected_item
        st.rerun()
            
    if b3.button("Eliminar", key=f"btn_d_{tipo}", disabled=(selected_item is None), use_container_width=True):
        st.session_state.crud_action = "Eliminar"
        st.session_state.crud_type = tipo
        st.session_state.selected_item = selected_item
        st.rerun()

tab_fon, tab_etf = st.tabs(["🏛️ Fondos", "📊 ETFs"])
with tab_fon:
    render_table_and_buttons(fondos_data, "Fondos", "FON")
with tab_etf:
    render_table_and_buttons(etfs_data, "ETFs", "ETF")

st.divider()

if st.session_state.crud_action:
    action = st.session_state.crud_action
    item_type = st.session_state.crud_type
    
    if action == "Nuevo":
        f_data = {}
    else:
        f_data = st.session_state.selected_item or {}
        
    st.markdown(f"### 🛠 Formulario: {action} - {'Fondo' if item_type == 'FON' else 'ETF'}")
    
    with st.form("form_mi_cartera"):
        if action == "Eliminar":
            st.error(f"¿Estás seguro que deseas eliminar el registro de {f_data.get('nombre', '')}?")
        
        c_i1, c_i2 = st.columns([1, 2])
        v_isin = c_i1.text_input("ISIN", value=f_data.get("ISIN", ""), disabled=(action == "Eliminar"))
        v_nombre = c_i2.text_input("Nombre", value=f_data.get("nombre", ""), disabled=(action == "Eliminar"))
        
        c_n1, c_n2, c_n3 = st.columns(3)
        v_cap = c_n1.number_input("Capital Invertido", value=float(f_data.get("capital_invertido", 0.0) or 0.0), disabled=(action == "Eliminar"))
        v_gas = c_n2.number_input("Importe Gastos", value=float(f_data.get("importe_gastos", 0.0) or 0.0), disabled=(action == "Eliminar"))
        v_rent = c_n3.number_input("% Rentabilidad Esperada", value=float(f_data.get("rentabilidad_esperada", 0.0) or 0.0), disabled=(action == "Eliminar"))
        
        c_d1, c_d2 = st.columns(2)
        
        # Safe Date Parsing
        fi = f_data.get("fecha_inicio")
        if pd.isna(fi) or fi is None: fi = date.today()
        elif isinstance(fi, datetime): fi = fi.date()
        elif isinstance(fi, pd.Timestamp): fi = fi.date()
            
        ff = f_data.get("fecha_fin")
        has_ff = not (pd.isna(ff) or ff is None)
        if isinstance(ff, datetime): ff = ff.date()
        elif isinstance(ff, pd.Timestamp): ff = ff.date()
        
        v_fi = c_d1.date_input("Fecha Inicio", value=fi, disabled=(action == "Eliminar"))
        with c_d2:
            tiene_fin = st.checkbox("Tiene Fecha Fin", value=has_ff, disabled=(action == "Eliminar"))
            if tiene_fin:
                v_ff = st.date_input("Fecha Fin", value=ff if has_ff else date.today(), disabled=(action == "Eliminar"))
            else:
                v_ff = None
                
        st.write("")
        btn_c1, btn_c2, _ = st.columns([1, 1, 4])
        
        if btn_c1.form_submit_button("Confirmar", type="primary"):
            if action == "Eliminar":
                if "id_str" in f_data:
                    delete_data(f_data["id_str"])
                    st.success("Registro eliminado.")
            else:
                data_dict = {
                    "tipo": item_type,
                    "ISIN": v_isin,
                    "nombre": v_nombre,
                    "capital_invertido": v_cap,
                    "importe_gastos": v_gas,
                    "rentabilidad_esperada": v_rent,
                    "fecha_inicio": datetime.combine(v_fi, datetime.min.time()) if v_fi else None,
                    "fecha_fin": datetime.combine(v_ff, datetime.min.time()) if v_ff else None
                }
                save_data(data_dict, f_data.get("id_str"))
                st.success(f"{'Actualizado' if action == 'Editar' else 'Creado'} con éxito.")
                
            st.session_state.crud_action = None
            st.session_state.selected_item = None
            st.rerun()
            
        if btn_c2.form_submit_button("Cancelar"):
            st.session_state.crud_action = None
            st.session_state.selected_item = None
            st.rerun()
