import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, UTC
import math
import plotly.graph_objects as go

from styles import apply_styles

# ==========================================================
# CONFIGURACIÓN
# ==========================================================
st.set_page_config(layout="wide", page_title="Constructor Automático de Carteras", page_icon="🤖")
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
curvas_collection = db["curvas_tipos"]
carteras_collection = db["carteras_fondos"]

# ==========================================================
# ESTILOS ADICIONALES
# ==========================================================
st.markdown("""
<style>
.metric-card {
    background: rgba(255, 255, 255, 0.05);
    padding: 20px;
    border-radius: 10px;
    border-left: 5px solid #4a6fa5;
    margin-bottom: 10px;
}
.category-header {
    background: #1e293b;
    color: #f1f5f9;
    padding: 10px 15px;
    border-radius: 5px;
    font-weight: bold;
    margin-top: 20px;
    margin-bottom: 10px;
}
</style>
""", unsafe_allow_html=True)

# ==========================================================
# LÓGICA DE ESTADO (RESET)
# ==========================================================
if "reset_counter" not in st.session_state:
    st.session_state.reset_counter = 0

def reset_todo():
    """Limpia todo, parámetros y cartera."""
    st.session_state.reset_counter += 1
    keys_to_clear = ["cartera_auto", "params_auto", "movs_auto", "pesos_auto"]
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]

def borrar_propuesta():
    """Solo borra la cartera propuesta cuando cambian los parámetros."""
    keys_to_clear = ["cartera_auto", "params_auto", "movs_auto", "pesos_auto"]
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]

# Sufijo para las claves de los widgets
suffix = f"_{st.session_state.reset_counter}"

# ==========================================================
# LÓGICA DE NEGOCIO / CÁLCULOS
# ==========================================================

REGION_MAP = {
    "Europa 🇪🇺": "EUR",
    "USA 🇺🇸": "US",
    "Japón 🇯🇵": "JP",
    "China 🇨🇳": "CN",
    "Global 🌐": "GLOBAL"
}

TRAMO_MAP = {
    "Monetario": "very_short",
    "Corto Plazo": "short",
    "Medio Plazo": "intermediate",
    "Largo Plazo": "long"
}

TRAMO_MAP_INV = {v: k for k, v in TRAMO_MAP.items()}

PESOS_DEFAULT = {
    "Conservador": {"very_short": 50, "short": 40, "intermediate": 10, "long": 0},
    "Moderado": {"very_short": 20, "short": 30, "intermediate": 40, "long": 10}
}

def get_latest_curve(pais_code="EUR"):
    ultimo = curvas_collection.find_one(sort=[("_id", -1)])
    if not ultimo:
        return None
    
    if pais_code == "GLOBAL":
        all_paises = ultimo.get("paises", [])
        if not all_paises:
            return None
        global_curve = {"codigo": "GLOBAL", "nombre": "Global", "emoji": "🌐", "plazos": []}
        plazos_data = {}
        for p in all_paises:
            for pl in p.get("plazos", []):
                pl_cod = pl["plazo"]
                if pl_cod not in plazos_data:
                    plazos_data[pl_cod] = {"actuals": [], "prevs": {}}
                plazos_data[pl_cod]["actuals"].append(pl["rendimiento_actual"])
                for yr, val in pl.get("previsiones", {}).items():
                    if yr not in plazos_data[pl_cod]["prevs"]:
                        plazos_data[pl_cod]["prevs"][yr] = []
                    plazos_data[pl_cod]["prevs"][yr].append(val)
        for pl_cod, data in plazos_data.items():
            avg_actual = sum(data["actuals"]) / len(data["actuals"])
            avg_prevs = {yr: sum(vals)/len(vals) for yr, vals in data["prevs"].items()}
            global_curve["plazos"].append({"plazo": pl_cod, "rendimiento_actual": avg_actual, "previsiones": avg_prevs})
        return global_curve

    for p in ultimo.get("paises", []):
        if p["codigo"] == pais_code:
            return p
    return None

def predecir_movimiento_tipos(curve, horizon_years=3):
    if not curve: return {}
    ahora_anno = datetime.now().year
    target_year = str(ahora_anno + min(horizon_years, 3)) 
    movimientos = {}
    for p in curve.get("plazos", []):
        plazo = p["plazo"]
        actual = p["rendimiento_actual"]
        proyectado = p.get("previsiones", {}).get(target_year)
        if proyectado is not None:
            movimientos[plazo] = {"actual": actual, "proyectado": proyectado, "delta": actual - proyectado}
    mapeo = {
        "very_short": movimientos.get("3M") or movimientos.get("6M"),
        "short": movimientos.get("2Y") or movimientos.get("1Y"),
        "intermediate": movimientos.get("5Y"),
        "long": movimientos.get("10Y") or movimientos.get("30Y")
    }
    return {k: v for k, v in mapeo.items() if v}

def get_dynamic_atrevido_weights(movimientos):
    duraciones_standard = {"very_short": 0.2, "short": 2.0, "intermediate": 4.0, "long": 10.0}
    scores = {}
    for tramo, m in movimientos.items():
        dur = duraciones_standard.get(tramo, 1.0)
        scores[tramo] = m["actual"] + (m["delta"] * dur)
    sorted_tramos = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    pesos = {t: 0 for t in ["very_short", "short", "intermediate", "long"]}
    pesos[sorted_tramos[0][0]] = 50
    if len(sorted_tramos) > 1: pesos[sorted_tramos[1][0]] = 30
    if len(sorted_tramos) > 2: pesos[sorted_tramos[2][0]] = 20
    return pesos

# ==========================================================
# INTERFAZ DE USUARIO
# ==========================================================
st.title("🤖 Constructor Automático de Cartera")
st.markdown("Crea una cartera optimizada basada en previsiones de tipos de interés y métricas de calidad.")

# --- SECCIÓN 1: PARÁMETROS ---
with st.container():
    st.subheader("⚙️ Parámetros de Configuración")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        perfil = st.selectbox("Perfil de Inversión", ["Conservador", "Moderado", "Atrevido"], 
                             key=f"perfil_auto{suffix}", on_change=borrar_propuesta)
        horizonte = st.slider("Horizonte (Años)", 1, 5, 3, 
                             key=f"horizonte_auto{suffix}", on_change=borrar_propuesta)
    
    with col2:
        n_fondos = st.number_input("Máximo Fondos Total (N)", 3, 20, 10, 
                                  key=f"n_fondos_auto{suffix}", on_change=borrar_propuesta)
        m_fondos = st.number_input("Máximo por Categoría (M)", 1, 5, 2, 
                                  key=f"m_fondos_auto{suffix}", on_change=borrar_propuesta)
        
    with col3:
        region_label = st.selectbox("Región de Referencia", list(REGION_MAP.keys()), index=0, 
                                   key=f"region_auto{suffix}", on_change=borrar_propuesta)
        region = REGION_MAP[region_label]
        criterio_base = st.radio("Optimizar Eficiencia basada en:", ["Rent. Proyectada (YTM)", "Rent. Pasada (1A)"], 
                                 index=0, key=f"criterio_auto{suffix}", on_change=borrar_propuesta)

    with col4:
        st.write("") 
        st.write("")
        generar = st.button("🚀 Generar Propuesta", type="primary", use_container_width=True)
        st.button("🔄 Reset Parámetros", use_container_width=True, on_click=reset_todo)

    # --- SECCIÓN: PESOS PERSONALIZABLES ---
    with st.expander("⚖️ Ajuste de Pesos por Tramo (Opcional)", expanded=False):
        st.info("Ajusta los porcentajes por defecto para este perfil. Deben sumar 100%.")
        
        # Calcular defaults según perfil
        if perfil == "Atrevido":
            curva_temp = get_latest_curve(region)
            movs_temp = predecir_movimiento_tipos(curva_temp, horizonte)
            defaults = get_dynamic_atrevido_weights(movs_temp)
        else:
            defaults = PESOS_DEFAULT[perfil]

        cw1, cw2, cw3, cw4 = st.columns(4)
        with cw1: p_vs = st.slider("Monetario %", 0, 100, defaults["very_short"], key=f"w_vs{suffix}")
        with cw2: p_s = st.slider("Corto Plazo %", 0, 100, defaults["short"], key=f"w_s{suffix}")
        with cw3: p_i = st.slider("Medio Plazo %", 0, 100, defaults["intermediate"], key=f"w_i{suffix}")
        with cw4: p_l = st.slider("Largo Plazo %", 0, 100, defaults["long"], key=f"w_l{suffix}")
        
        total_p = p_vs + p_s + p_i + p_l
        if total_p != 100:
            st.warning(f"⚠️ El total suma {total_p}%. Debe ser 100%.")
        else:
            st.success("✅ Total 100% configurado.")
            
        pesos_ajustados = {
            "very_short": p_vs / 100,
            "short": p_s / 100,
            "intermediate": p_i / 100,
            "long": p_l / 100
        }

# --- PROCESAMIENTO ---
if generar:
    if total_p != 100:
        st.error("Los pesos deben sumar 100% para generar la propuesta.")
        st.stop()

    with st.spinner("🔍 Consultando base de datos y optimizando cartera..."):
        # 1. Obtener Curva
        curva = get_latest_curve(region)
        if not curva:
            st.error(f"No hay curvas para {region}.")
            st.stop()
            
        movs = predecir_movimiento_tipos(curva, horizonte)
        pesos = pesos_ajustados
        
        # 2. Obtener Fondos
        tramos_interes = [t for t, p in pesos.items() if p > 0]
        query = {"tramo_rf": {"$in": tramos_interes}}
        
        if region == "EUR":
            query["categoria"] = {"$regex": "EUR|Euro", "$options": "i"}
        elif region == "US":
            query["categoria"] = {"$regex": "USD|U.S.|US ", "$options": "i"}
        elif region == "JP":
            query["categoria"] = {"$regex": "JPY|Yen|Japan", "$options": "i"}
        elif region == "CN":
            query["categoria"] = {"$regex": "CNY|China", "$options": "i"}
        
        fondos_raw = list(fondos_collection.find(query, {
            "_id": 0, "isin": 1, "nombre": 1, "tramo_rf": 1,
            "duration.yield_to_maturity": 1,
            "duration.avg_effective_duration": 1,
            "riesgo.for3Year.volatility": 1,
            "riesgo.for1Year.volatility": 1,
            "rentabilidad.historica.y1": 1
        }))
        
        df_fondos = pd.json_normalize(fondos_raw)
        if df_fondos.empty:
            st.warning("No se encontraron fondos suficientes.")
            st.stop()
            
        col_mapping = {
            "duration.yield_to_maturity": "ytm",
            "duration.avg_effective_duration": "duracion",
            "riesgo.for3Year.volatility": "vol",
            "rentabilidad.historica.y1": "rent1y"
        }
        
        for old_col, new_col in col_mapping.items():
            if old_col in df_fondos.columns:
                df_fondos.rename(columns={old_col: new_col}, inplace=True)
            elif new_col not in df_fondos.columns:
                df_fondos[new_col] = 0.0
        
        df_fondos["ytm"] = df_fondos["ytm"].fillna(0.0)
        df_fondos["vol"] = df_fondos["vol"].fillna(0.0)
        df_fondos["rent1y"] = df_fondos["rent1y"].fillna(0.0)
        df_fondos["duracion"] = df_fondos["duracion"].fillna(0.0)
        
        # 3. SELECCIÓN AUTOMÁTICA
        seleccionados = []
        rent_col = "ytm" if "Proyectada" in criterio_base else "rent1y"

        for tramo in tramos_interes:
            peso_tramo = pesos[tramo]
            subset = df_fondos[df_fondos["tramo_rf"] == tramo].copy()
            if subset.empty: continue

            subset["eficiencia"] = subset[rent_col] / subset["vol"].replace(0, 0.05)
            subset = subset.sort_values(by=["eficiencia", rent_col], ascending=[False, False])
            
            top_tramo = subset.head(m_fondos).copy()
            if not top_tramo.empty:
                top_tramo["peso"] = peso_tramo / len(top_tramo)
                seleccionados.append(top_tramo)
                
        if not seleccionados:
            st.error("No se pudo construir la selección.")
            st.stop()
            
        df_final = pd.concat(seleccionados)
        if len(df_final) > n_fondos:
            df_final = df_final.head(n_fondos)
            df_final["peso"] = df_final["peso"] / df_final["peso"].sum()
            
        st.session_state.cartera_auto = df_final
        st.session_state.params_auto = {"perfil": perfil, "horizonte": horizonte, "region": region}
        st.session_state.movs_auto = movs
        st.session_state.pesos_auto = pesos

# Lógica de visualización persistente
if "cartera_auto" in st.session_state:
    df_final = st.session_state.cartera_auto
    movs = st.session_state.movs_auto
    pesos = st.session_state.pesos_auto

    st.markdown("---")
    st.subheader("📋 Propuesta de Selección")
    
    for tramo_db in df_final["tramo_rf"].unique():
        st.markdown(f'<div class="category-header">📦 {TRAMO_MAP_INV.get(tramo_db)} ({df_final[df_final["tramo_rf"] == tramo_db]["peso"].sum()*100:.1f}%)</div>', unsafe_allow_html=True)
        sub = df_final[df_final["tramo_rf"] == tramo_db].copy()
        viz = sub[["isin", "nombre", "ytm", "vol", "duracion", "peso"]].copy()
        viz["peso"] = viz["peso"].map(lambda x: f"{x*100:.2f}%")
        viz["ytm"] = viz["ytm"].map(lambda x: f"{x:.2f}%")
        st.table(viz)

    # Métricas Consolidadas
    st.subheader("📊 Métricas Consolidadas")
    total_ytm = (df_final["ytm"] * df_final["peso"]).sum()
    total_vol = (df_final["vol"] * df_final["peso"]).sum()
    total_dur = (df_final["duracion"] * df_final["peso"]).sum()
    
    rent_proyectada = 0
    for _, row in df_final.iterrows():
        t = row["tramo_rf"]
        d_mov = movs.get(t, {"delta": 0})["delta"]
        rent_proyectada += row["peso"] * (row["ytm"] + (d_mov * row["duracion"]) / horizonte)
        
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.markdown(f'<div class="metric-card"><h4>Rent. Esperada</h4><h2>{rent_proyectada:.2f}%</h2></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><h4>YTM Medio</h4><h2>{total_ytm:.2f}%</h2></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="metric-card"><h4>Volatilidad Media</h4><h2>{total_vol:.2f}%</h2></div>', unsafe_allow_html=True)
    with c4: st.markdown(f'<div class="metric-card"><h4>Duración Media</h4><h2>{total_dur:.2f} yr</h2></div>', unsafe_allow_html=True)
    with c5:
        be = total_ytm / total_dur if total_dur > 0.1 else 9.9
        st.markdown(f'<div class="metric-card"><h4>Break-even Tipos</h4><h2>{be:.2f}%</h2></div>', unsafe_allow_html=True)

    fig = go.Figure(data=[go.Pie(labels=df_final["nombre"], values=df_final["peso"], hole=.4)])
    fig.update_layout(title_text="Distribución de la Cartera", height=450)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col_acc1, col_acc2, col_acc3 = st.columns([4, 2, 4])
    with col_acc2:
        if st.button("✅ Confirmar y Guardar", type="primary", use_container_width=True):
            cartera_id = "AUTO-" + datetime.now().strftime("%Y%m%d-%H%M%S")
            fondos_lista = [{"isin": row["isin"], "nombre": row["nombre"], "peso": float(row["peso"]), "tramo": row["tramo_rf"]} for _, row in df_final.iterrows()]
            doc = {
                "cartera_id": cartera_id, "fecha_creacion": datetime.now(UTC), "origen": "A",
                "perfil": perfil, "region": region, "horizonte": horizonte,
                "metricas": {"rent": rent_proyectada, "ytm": total_ytm, "vol": total_vol, "dur": total_dur},
                "fondos": fondos_lista
            }
            try:
                carteras_collection.insert_one(doc)
                st.balloons()
                st.success(f"Cartera {cartera_id} guardada.")
                reset_todo()
                st.rerun()
            except Exception as e: st.error(f"Error: {e}")
else:
    st.info("Configura los parámetros y pulsa 'Generar Propuesta' para que el algoritmo trabaje por ti.")
