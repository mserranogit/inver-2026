import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from styles import apply_styles

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(layout="wide", page_title="Gestión de Carteras", page_icon="📁")
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
fondos_collection = db["fondos"]
etfs_collection = db["etfs"]
carteras_fondos = db["carteras_fondos"]
carteras_etf = db["carteras_etf"]

# ==========================================================
# ESTILOS
# ==========================================================
st.markdown("""
<style>
.edit-card {
    background: rgba(255, 255, 255, 0.05);
    padding: 25px;
    border-radius: 15px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    margin-top: 20px;
}
.table-header {
    background-color: #e9ecef;
    padding: 10px;
    font-weight: bold;
    color: #4a6fa5;
    border: 1px solid #dee2e6;
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
    height: 45px;
}
.vertical-line {
    border: 1px solid #dee2e6;
    border-top: none;
    height: 45px;
    padding: 0 10px;
    display: flex;
    align-items: center;
    background-color: white;
}
/* Estilo para los botones que actúan como celdas */
.stButton > button {
    height: 45px !important;
    width: 100% !important;
    border-radius: 0 !important;
    border: 1px solid #dee2e6 !important;
    border-top: none !important;
    border-left: none !important;
    background-color: white !important;
    font-size: 18px !important;
    margin: 0 !important;
}
.stButton > button:hover {
    background-color: #f8f9fa !important;
}
/* Estilos para el editor (edit-card) */
.edit-card [data-testid="stColumn"] {
    border-right: 1px solid #dee2e6;
    border-left: 1px solid #dee2e6;
    margin-left: -1px; /* Evita bordes dobles */
    padding: 10px !important;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 50px;
}
.edit-card [data-testid="stColumn"]:first-child {
    border-left: 1px solid #dee2e6;
}
/* Alineación de widgets en columnas del editor */
.edit-card div[data-testid="column"] {
    display: flex;
    align-items: center;
    justify-content: center;
}
/* Alinear nombre a la izquierda */
.edit-card [data-testid="stColumn"]:nth-child(2) {
    justify-content: flex-start !important;
}
.stCheckbox > label {
    font-weight: 500;
}
</style>
""", unsafe_allow_html=True)

# ==========================================================
# FUNCIONES AUXILIARES
# ==========================================================
def format_date(dt):
    if isinstance(dt, datetime):
        return dt.strftime("%d/%m/%Y %H:%M")
    return str(dt)

def save_portfolio(type_p, doc_id, assets):
    col = carteras_fondos if type_p == "Fondos" else carteras_etf
    field = "fondos" if type_p == "Fondos" else "etfs"
    
    # Recalcular pesos para asegurar que sumen 1
    total_weights = sum(a['peso'] for a in assets)
    if total_weights > 0:
        for a in assets:
            a['peso'] = a['peso'] / total_weights
            
    col.update_one({"_id": doc_id}, {"$set": {field: assets}})

def get_asset_metrics(isin, type_p):
    """Obtiene métricas de un activo desde su colección correspondiente."""
    col = fondos_collection if type_p == "Fondos" else etfs_collection
    doc = col.find_one({"isin": isin})
    if not doc:
        return {"ytm": 0.0, "vol": 0.0, "dur": 0.0, "tramo": "N/A"}
    
    if type_p == "Fondos":
        return {
            "ytm": doc.get("duration", {}).get("yield_to_maturity", 0.0) or 0.0,
            "vol": doc.get("riesgo", {}).get("for3Year", {}).get("volatility", 0.0) or doc.get("riesgo", {}).get("for1Year", {}).get("volatility", 0.0) or 0.0,
            "dur": doc.get("duration", {}).get("avg_effective_duration", 0.0) or 0.0,
            "tramo": doc.get("tramo_rf", "N/A")
        }
    else: # ETFs
        return {
            "ytm": doc.get("yield_to_maturity", 0.0) or 0.0,
            "vol": doc.get("volatility_3y", 0.0) or 0.0,
            "dur": doc.get("duracion_efectiva", 0.0) or 0.0,
            "tramo": doc.get("tipoEtf", "N/A")
        }

def calculate_portfolio_totals(assets, type_p):
    """Calcula métricas ponderadas para una lista de activos."""
    total_ytm = 0.0
    total_vol = 0.0
    total_dur = 0.0
    total_weight = sum(a.get("peso", 0) for a in assets)
    
    if total_weight == 0:
        return {"ytm": 0, "vol": 0, "dur": 0}
        
    for a in assets:
        m = get_asset_metrics(a["isin"], type_p)
        # Normalizamos peso si no suma 1 en el borrador
        p_rel = a.get("peso", 0) / total_weight
        total_ytm += m["ytm"] * p_rel
        total_vol += m["vol"] * p_rel
        total_dur += m["dur"] * p_rel
        
    return {"ytm": total_ytm, "vol": total_vol, "dur": total_dur}

# ==========================================================
# INTERFAZ
# ==========================================================
st.title("📁 Gestión de Carteras")
st.markdown("Administra, edita y depura tus carteras de inversión guardadas.")

tab_f, tab_e = st.tabs(["🏛️ Carteras de Fondos", "📊 Carteras de ETFs"])

# --- LÓGICA COMPARTIDA DE EDICIÓN ---
if "editing_portfolio" not in st.session_state:
    st.session_state.editing_portfolio = None # {type, doc_id, assets, original_doc}

def start_editing(type_p, doc):
    field = "fondos" if type_p == "Fondos" else "etfs"
    st.session_state.editing_portfolio = {
        "type": type_p,
        "doc_id": doc["_id"],
        "assets": [dict(a) for a in doc.get(field, [])],
        "original_doc": doc
    }
    st.session_state.should_scroll_to_edit = True

def cancel_editing():
    st.session_state.editing_portfolio = None

# ==========================================================
# TAB: FONDOS
# ==========================================================
with tab_f:
    docs = list(carteras_fondos.find().sort("fecha_creacion", -1))
    if not docs:
        st.info("No hay carteras de fondos guardadas.")
    else:
        # Tabla de carteras
        data = []
        for d in docs:
            # Obtener perfil y región de varias posibles fuentes (Auto vs Manual)
            p = d.get("perfil") or d.get("metas", {}).get("perfil")
            r = d.get("region") or d.get("metas", {}).get("region")
            
            # Si es manual y no tiene estos campos, ponemos Manual
            if d.get("origen") == "M" and not p:
                info_perf = "Manual"
            else:
                p_text = p if p else "N/A"
                r_text = f" ({r})" if r else ""
                info_perf = f"{p_text}{r_text}"

            data.append({
                "ID": d.get("cartera_id", "N/A"),
                "Fecha": format_date(d.get("fecha_creacion")),
                "PerfReg": info_perf,
                "Origen": "Auto" if d.get("origen") == "A" else "Manual",
                "Activos": len(d.get("fondos", [])),
                "obj": d
            })
        
        df_list = pd.DataFrame(data)
        st.subheader("📋 Listado de Carteras de Fondos")
        
        # Cabecera
        h_cols = st.columns([2.5, 2.5, 3, 1.5, 1, 1])
        with h_cols[0]: st.markdown('<div class="table-header">ID Cartera</div>', unsafe_allow_html=True)
        with h_cols[1]: st.markdown('<div class="table-header" style="border-left:none">Fecha</div>', unsafe_allow_html=True)
        with h_cols[2]: st.markdown('<div class="table-header" style="border-left:none">Perfil / Región</div>', unsafe_allow_html=True)
        with h_cols[3]: st.markdown('<div class="table-header" style="border-left:none">Activos</div>', unsafe_allow_html=True)
        with h_cols[4]: st.markdown('<div class="table-header" style="border-left:none; border-right:none">Edit</div>', unsafe_allow_html=True)
        with h_cols[5]: st.markdown('<div class="table-header" style="border-left:none">Del</div>', unsafe_allow_html=True)

        for idx, row in df_list.iterrows():
            cols = st.columns([2.5, 2.5, 3, 1.5, 1, 1], gap="small")
            
            with cols[0]: st.markdown(f'<div class="vertical-line"><strong>{row["ID"]}</strong></div>', unsafe_allow_html=True)
            with cols[1]: st.markdown(f'<div class="vertical-line" style="border-left:none">{row["Fecha"]}</div>', unsafe_allow_html=True)
            with cols[2]: st.markdown(f'<div class="vertical-line" style="border-left:none">{row["PerfReg"]}</div>', unsafe_allow_html=True)
            with cols[3]: st.markdown(f'<div class="vertical-line" style="border-left:none">{row["Activos"]}</div>', unsafe_allow_html=True)
            
            with cols[4]:
                if st.button("✏️", key=f"edit_f_{idx}", help="Editar", use_container_width=True):
                    start_editing("Fondos", row['obj'])

            with cols[5]:
                if st.button("🗑️", key=f"del_f_{idx}", help="Borrar", use_container_width=True):
                    carteras_fondos.delete_one({"_id": row['obj']["_id"]})
                    st.rerun()

# ==========================================================
# TAB: ETFs
# ==========================================================
with tab_e:
    docs_e = list(carteras_etf.find().sort("fecha_creacion", -1))
    if not docs_e:
        st.info("No hay carteras de ETFs guardadas.")
    else:
        data_e = []
        for d in docs_e:
            p_e = d.get("perfil") or d.get("metas", {}).get("perfil")
            r_e = d.get("region") or d.get("metas", {}).get("region")
            
            if d.get("origen") == "M" and not p_e:
                info_perf_e = "Manual"
            else:
                p_text_e = p_e if p_e else "N/A"
                r_text_e = f" ({r_e})" if r_e else ""
                info_perf_e = f"{p_text_e}{r_text_e}"

            data_e.append({
                "ID": d.get("cartera_id", "N/A"),
                "Fecha": format_date(d.get("fecha_creacion")),
                "PerfReg": info_perf_e,
                "Origen": "Auto" if d.get("origen") == "A" else "Manual",
                "Activos": len(d.get("etfs", [])),
                "obj": d
            })
        
        df_list_e = pd.DataFrame(data_e)
        st.subheader("📋 Listado de Carteras de ETFs")
        
        # Cabecera
        he_cols = st.columns([2.5, 2.5, 3, 1.5, 1, 1])
        with he_cols[0]: st.markdown('<div class="table-header">ID Cartera</div>', unsafe_allow_html=True)
        with he_cols[1]: st.markdown('<div class="table-header" style="border-left:none">Fecha</div>', unsafe_allow_html=True)
        with he_cols[2]: st.markdown('<div class="table-header" style="border-left:none">Perfil / Región</div>', unsafe_allow_html=True)
        with he_cols[3]: st.markdown('<div class="table-header" style="border-left:none">Activos</div>', unsafe_allow_html=True)
        with he_cols[4]: st.markdown('<div class="table-header" style="border-left:none; border-right:none">Edit</div>', unsafe_allow_html=True)
        with he_cols[5]: st.markdown('<div class="table-header" style="border-left:none">Del</div>', unsafe_allow_html=True)

        for idx, row in df_list_e.iterrows():
            cols_e = st.columns([2.5, 2.5, 3, 1.5, 1, 1], gap="small")
            
            with cols_e[0]: st.markdown(f'<div class="vertical-line"><strong>{row["ID"]}</strong></div>', unsafe_allow_html=True)
            with cols_e[1]: st.markdown(f'<div class="vertical-line" style="border-left:none">{row["Fecha"]}</div>', unsafe_allow_html=True)
            with cols_e[2]: st.markdown(f'<div class="vertical-line" style="border-left:none">{row["PerfReg"]}</div>', unsafe_allow_html=True)
            with cols_e[3]: st.markdown(f'<div class="vertical-line" style="border-left:none">{row["Activos"]}</div>', unsafe_allow_html=True)
            
            with cols_e[4]:
                if st.button("✏️", key=f"edit_e_{idx}", help="Editar", use_container_width=True):
                    start_editing("ETFs", row['obj'])

            with cols_e[5]:
                if st.button("🗑️", key=f"del_e_{idx}", help="Borrar", use_container_width=True):
                    carteras_etf.delete_one({"_id": row['obj']["_id"]})
                    st.rerun()

# ==========================================================
# FORMULARIO DE EDICIÓN (MODAL-LIKE)
# ==========================================================
if st.session_state.editing_portfolio:
    edit_state = st.session_state.editing_portfolio
    type_p = edit_state["type"]
    
    # Ancla para el scroll
    st.markdown('<div id="edit_form_anchor"></div>', unsafe_allow_html=True)
    
    # Inyectar JS para scroll suave si acabamos de empezar a editar
    if st.session_state.get("should_scroll_to_edit", False):
        st.components.v1.html(
            """
            <script>
                var element = window.parent.document.getElementById("edit_form_anchor");
                if (element) {
                    element.scrollIntoView({behavior: "smooth", block: "start"});
                }
            </script>
            """,
            height=0
        )
        st.session_state.should_scroll_to_edit = False

    st.markdown("---")
    st.markdown(f'<div class="edit-card">', unsafe_allow_html=True)
    st.subheader(f"🛠️ Editando Cartera: {edit_state['original_doc'].get('cartera_id')}")
    
    assets = edit_state["assets"]
    to_delete = []
    
    st.write("##### Selección de activos a modificar/eliminar:")
    
    # Cabecera de la tabla de edición
    he_c1, he_c2, he_c3, he_c4 = st.columns([1, 5, 2, 2], gap="small")
    with he_c1: st.markdown('<div class="table-header">Baja</div>', unsafe_allow_html=True)
    with he_c2: st.markdown('<div class="table-header">Activo / ISIN</div>', unsafe_allow_html=True)
    with he_c3: st.markdown('<div class="table-header">% de Peso</div>', unsafe_allow_html=True)
    with he_c4: st.markdown('<div class="table-header">Tramo</div>', unsafe_allow_html=True)

    # Tabla de edición de activos
    for i, asset in enumerate(assets):
        c1, c2, c3, c4 = st.columns([1, 5, 2, 2], gap="small")
        
        with c1:
            # Centrado manual mediante sub-columnas para mayor fiabilidad
            _, sub_c, _ = st.columns([1, 2, 1])
            with sub_c:
                is_del = st.checkbox(" ", key=f"chk_{i}", help="Eliminar", label_visibility="collapsed")
        if is_del: to_delete.append(i)
        
        with c2:
            st.markdown(f'<strong>{asset.get("nombre")}</strong><br><small>{asset.get("isin")}</small>', unsafe_allow_html=True)
        
        with c3:
            new_peso = st.number_input("Peso (%)", 0.0, 100.0, float(asset.get('peso', 0)*100), key=f"w_{i}", step=0.1, label_visibility="collapsed")
            asset['peso'] = new_peso / 100
            
        # Recuperar tramo si es N/A o no existe
        disp_tramo = asset.get('tramo')
        if not disp_tramo or disp_tramo == "N/A":
            m_info = get_asset_metrics(asset['isin'], type_p)
            disp_tramo = m_info["tramo"]
            asset['tramo'] = disp_tramo

        with c4:
            st.markdown(f'<div style="text-align:center; width:100%">{disp_tramo}</div>', unsafe_allow_html=True)
        
        st.divider() # Línea de separación entre activos

    st.divider()
    
    # Alta de nuevo activo
    st.write("##### ➕ Añadir nuevo activo")
    col_add1, col_add2 = st.columns([4, 1])
    
    # Selector de búsqueda
    search_col = fondos_collection if type_p == "Fondos" else etfs_collection
    search_name_key = "nombre" if type_p == "Fondos" else "nombreEtf"
    
    all_available = list(search_col.find({}, {search_name_key: 1, "isin": 1, "tramo_rf": 1, "tipoEtf": 1}).limit(500))
    options = {f"{a.get(search_name_key)} ({a.get('isin')})": a for a in all_available}
    
    selected_new = col_add1.selectbox("Buscar activo para añadir:", ["---"] + list(options.keys()))
    if col_add2.button("Añadir"):
        if selected_new != "---":
            base_data = options[selected_new]
            new_asset = {
                "isin": base_data["isin"],
                "nombre": base_data.get(search_name_key),
                "peso": 0.0,
                "tramo": base_data.get("tramo_rf") or base_data.get("tipoEtf") or "N/A"
            }
            edit_state["assets"].append(new_asset)
            st.rerun()

    # --- NUEVA SECCIÓN: COMPARATIVA ANTES VS DESPUÉS ---
    st.divider()
    st.write("##### 📊 Comparativa: Antes vs Después")
    
    # Preparamos las listas
    final_assets = [a for idx, a in enumerate(assets) if idx not in to_delete]
    
    # Calculamos métricas
    with st.spinner("Calculando impacto..."):
        m_before = calculate_portfolio_totals(edit_state["original_doc"].get("fondos" if type_p == "Fondos" else "etfs", []), type_p)
        m_after = calculate_portfolio_totals(final_assets, type_p)
    
    # Mostrar métricas en columnas
    mc1, mc2, mc3 = st.columns(3)
    
    def metric_diff(label, val_old, val_new, suffix="%"):
        delta = val_new - val_old
        color = "normal"
        if label == "Volatilidad":
             color = "inverse" # Menos vol es mejor
        
        mc1.metric(f"{label} (Origen)", f"{val_old:.2f}{suffix}")
        mc2.metric(f"{label} (Nueva)", f"{val_new:.2f}{suffix}", delta=f"{delta:.2f}{suffix}", delta_color=color)

    with mc1: st.write("**Métrica**")
    with mc2: st.write("**Original**")
    with mc3: st.write("**Tras Modificación**")
    
    # Filas de comparación manual (Streamlit metric no permite tabla de deltas nativa fácil en horizontal)
    col_m1, col_m2, col_m3 = st.columns(3)
    with col_m1:
        st.markdown(f"**Rentabilidad (YTM)**")
        st.markdown(f"**Volatilidad**")
        st.markdown(f"**Duración (Años)**")
    
    with col_m2:
        st.markdown(f"{m_before['ytm']:.2f}%")
        st.markdown(f"{m_before['vol']:.2f}%")
        st.markdown(f"{m_before['dur']:.2f}")

    with col_m3:
        # Rentabilidad: Verde si sube
        d_ytm = m_after['ytm'] - m_before['ytm']
        c_ytm = "green" if d_ytm >= 0 else "red"
        st.markdown(f":{c_ytm}[{m_after['ytm']:.2f}% ({'+' if d_ytm>=0 else ''}{d_ytm:.2f}%)]")
        
        # Volatilidad: Verde si baja
        d_vol = m_after['vol'] - m_before['vol']
        c_vol = "green" if d_vol <= 0 else "red"
        st.markdown(f":{c_vol}[{m_after['vol']:.2f}% ({'+' if d_vol>=0 else ''}{d_vol:.2f}%)]")
        
        # Duración: Neutral/Azul
        d_dur = m_after['dur'] - m_before['dur']
        st.markdown(f"**{m_after['dur']:.2f}** ({'+' if d_dur>=0 else ''}{d_dur:.2f})")

    st.divider()

    # Botones de Acción
    st.write("")
    bcol1, bcol2, bcol3 = st.columns([2, 2, 6])
    
    if bcol1.button("💾 Guardar Cambios", type="primary", use_container_width=True):
        # Eliminar marcados
        final_assets = [a for idx, a in enumerate(assets) if idx not in to_delete]
        save_portfolio(type_p, edit_state["doc_id"], final_assets)
        st.success("Cartera actualizada con éxito.")
        st.session_state.editing_portfolio = None
        st.rerun()
        
    if bcol2.button("❌ Cancelar", use_container_width=True):
        cancel_editing()
        st.rerun()
        
    st.markdown('</div>', unsafe_allow_html=True)
