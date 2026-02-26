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
st.set_page_config(layout="wide", page_title="Constructor Automático ETFs", page_icon="🤖")
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
etfs_collection = db["etfs"]
curvas_collection = db["curvas_tipos"]
carteras_etf_collection = db["carteras_etf"]

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
if "reset_counter_etfs" not in st.session_state:
    st.session_state.reset_counter_etfs = 0

def reset_todo_etfs():
    st.session_state.reset_counter_etfs += 1
    keys_to_clear = ["cartera_auto_etfs", "params_auto_etfs", "movs_auto_etfs", "pesos_auto_etfs"]
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]

def borrar_propuesta_etfs():
    keys_to_clear = ["cartera_auto_etfs", "params_auto_etfs", "movs_auto_etfs", "pesos_auto_etfs"]
    for k in keys_to_clear:
        if k in st.session_state:
            del st.session_state[k]

suffix = f"_{st.session_state.reset_counter_etfs}"

# ==========================================================
# LÓGICA DE NEGOCIO
# ==========================================================
REGION_MAP = {
    "Europa 🇪🇺": "EUR",
    "USA 🇺🇸": "US",
    "Japón 🇯🇵": "JP",
    "China 🇨🇳": "CN",
    "Global 🌐": "GLOBAL"
}

TRAMO_MAP_INV = {
    "very_short": "Monetario / Ultra Corto",
    "short": "Corto Plazo",
    "intermediate": "Medio Plazo",
    "long": "Largo Plazo"
}

PESOS_DEFAULT = {
    "Conservador": {"very_short": 50, "short": 40, "intermediate": 10, "long": 0},
    "Moderado": {"very_short": 20, "short": 30, "intermediate": 40, "long": 10}
}

def get_latest_curve(pais_code="EUR"):
    ultimo = curvas_collection.find_one(sort=[("_id", -1)])
    if not ultimo: return None
    
    if pais_code == "GLOBAL":
        all_paises = ultimo.get("paises", [])
        if not all_paises: return None
        global_curve = {"codigo": "GLOBAL", "nombre": "Global", "emoji": "🌐", "plazos": []}
        plazos_data = {}
        for p in all_paises:
            for pl in p.get("plazos", []):
                pl_cod = pl["plazo"]
                if pl_cod not in plazos_data: plazos_data[pl_cod] = {"actuals": [], "prevs": {}}
                plazos_data[pl_cod]["actuals"].append(pl["rendimiento_actual"])
                for yr, val in pl.get("previsiones", {}).items():
                    if yr not in plazos_data[pl_cod]["prevs"]: plazos_data[pl_cod]["prevs"][yr] = []
                    plazos_data[pl_cod]["prevs"][yr].append(val)
        for pl_cod, data in plazos_data.items():
            avg_actual = sum(data["actuals"]) / len(data["actuals"])
            avg_prevs = {yr: sum(vals)/len(vals) for yr, vals in data["prevs"].items()}
            global_curve["plazos"].append({"plazo": pl_cod, "rendimiento_actual": avg_actual, "previsiones": avg_prevs})
        return global_curve

    for p in ultimo.get("paises", []):
        if p["codigo"] == pais_code: return p
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

def clasificar_etf_tramo(doc):
    tipo = doc.get("tipoEtf", "")
    dur = doc.get("duracion_efectiva")
    if dur is None:
        if tipo == "Mercado Monetario": return "very_short"
        return "intermediate" 
    if dur < 0.5: return "very_short"
    if dur < 3.0: return "short"
    if dur < 5.0: return "intermediate"
    return "long"

def clean_num(x):
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    try: return float(str(x).replace("%", "").replace("+", "").replace(",", "."))
    except: return 0.0

# ==========================================================
# INTERFAZ
# ==========================================================
st.title("🤖 Constructor Automático de Cartera ETFs")
st.markdown("Algoritmo de selección optimizada de ETFs basado en rentabilidad/riesgo y previsiones de tipos.")

with st.container():
    st.subheader("⚙️ Parámetros de Configuración")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        perfil = st.selectbox("Perfil de Inversión", ["Conservador", "Moderado", "Atrevido"], 
                             key=f"perfil_auto_etfs{suffix}", on_change=borrar_propuesta_etfs)
        horizonte = st.slider("Horizonte (Años)", 1, 5, 3, 
                             key=f"horizonte_auto_etfs{suffix}", on_change=borrar_propuesta_etfs)
    
    with col2:
        n_etfs = st.number_input("Máximo ETFs Total", 2, 12, 6, 
                                  key=f"n_etfs_auto{suffix}", on_change=borrar_propuesta_etfs)
        m_etfs = st.number_input("Máximo por Categoría", 1, 3, 2, 
                                  key=f"m_etfs_auto{suffix}", on_change=borrar_propuesta_etfs)
        
    with col3:
        region_label = st.selectbox("Región de Referencia", list(REGION_MAP.keys()), index=0, 
                                   key=f"region_auto_etfs{suffix}", on_change=borrar_propuesta_etfs)
        region = REGION_MAP[region_label]
        criterio_base = st.radio("Optimizar Eficiencia basada en:", ["Rent. Proyectada (YTM)", "Rent. Pasada (1A)"], 
                                 index=0, key=f"criterio_auto_etfs{suffix}", on_change=borrar_propuesta_etfs)

    with col4:
        st.write("")
        st.write("")
        generar = st.button("🚀 Generar Propuesta ETFs", type="primary", use_container_width=True)
        st.button("🔄 Reset Parámetros", use_container_width=True, on_click=reset_todo_etfs)

    # --- SECCIÓN: PESOS PERSONALIZABLES ---
    with st.expander("⚖️ Ajuste de Pesos por Tramo (Opcional)", expanded=False):
        st.info("Ajusta los porcentajes por defecto para este perfil. Deben sumar 100%.")
        
        # Calcular defaults según perfil
        if perfil == "Atrevido":
            # Para atrevido necesitamos los movimientos para el default dinámico
            curva_temp = get_latest_curve(region)
            movs_temp = predecir_movimiento_tipos(curva_temp, horizonte)
            defaults = get_dynamic_atrevido_weights(movs_temp)
        else:
            defaults = PESOS_DEFAULT[perfil]

        cw1, cw2, cw3, cw4 = st.columns(4)
        with cw1: p_vs = st.slider("Ultra Corto %", 0, 100, defaults["very_short"], key=f"w_vs{suffix}")
        with cw2: p_s = st.slider("Corto Plazo %", 0, 100, defaults["short"], key=f"w_s{suffix}")
        with cw3: p_i = st.slider("Medio Plazo %", 0, 100, defaults["intermediate"], key=f"w_i{suffix}")
        with cw4: p_l = st.slider("Largo Plazo %", 0, 100, defaults["long"], key=f"w_l{suffix}")
        
        total_p = p_vs + p_s + p_i + p_l
        if total_p != 100:
            st.warning(f"⚠️ El total suma {total_p}%. Debe ser 100% para resultados correctos.")
        else:
            st.success("✅ Total 100% correctamente configurado.")
            
        pesos_ajustados = {
            "very_short": p_vs / 100,
            "short": p_s / 100,
            "intermediate": p_i / 100,
            "long": p_l / 100
        }

if generar:
    if total_p != 100:
        st.error("No se puede generar la propuesta: Los pesos deben sumar 100%.")
        st.stop()

    with st.spinner("🤖 El algoritmo está analizando el mercado de ETFs..."):
        curva = get_latest_curve(region)
        if not curva:
            st.error(f"No hay curvas para {region}.")
            st.stop()
            
        movs = predecir_movimiento_tipos(curva, horizonte)
        # Usamos los pesos_ajustados definidos en el expander
        pesos = pesos_ajustados
        
        # Filtro geográfico
        query = {"tipoEtf": {"$in": ["Mercado Monetario", "Renta Fija"]}}
        if region != "GLOBAL":
            query["nombreEtf"] = {"$regex": region, "$options": "i"} if region != "EUR" else {"$regex": "EUR|Euro", "$options": "i"}
            if region == "US": query["nombreEtf"] = {"$regex": "USD|U.S.|USA", "$options": "i"}

        etfs_raw = list(etfs_collection.find(query, {"_id":0}))
        if not etfs_raw:
            etfs_raw = list(etfs_collection.find({"tipoEtf": {"$in": ["Mercado Monetario", "Renta Fija"]}}, {"_id":0}))

        data = []
        for e in etfs_raw:
            tramo = clasificar_etf_tramo(e)
            if tramo in pesos and pesos[tramo] > 0:
                ytm = clean_num(e.get("yield_to_maturity"))
                vol = clean_num(e.get("volatility_3y"))
                rent1y = clean_num(e.get("yield_1y"))
                dur = clean_num(e.get("duracion_efectiva"))
                
                rent_target = ytm if "Proyectada" in criterio_base else rent1y
                eficiencia = rent_target / (vol if vol > 0.05 else 0.05)
                
                data.append({
                    "isin": e["isin"], "nombre": e["nombreEtf"], "tramo": tramo,
                    "ytm": ytm, "vol": vol, "rent1y": rent1y, "duracion": dur,
                    "eficiencia": eficiencia, "ter": e.get("ter", "N/A")
                })
        
        df_all = pd.DataFrame(data)
        if df_all.empty:
            st.warning("No se encontraron ETFs para los criterios.")
            st.stop()
            
        seleccionados = []
        for t in df_all["tramo"].unique():
            subset = df_all[df_all["tramo"] == t].sort_values("eficiencia", ascending=False).head(m_etfs)
            if not subset.empty:
                subset["peso"] = pesos[t] / len(subset)
                seleccionados.append(subset)
        
        df_final = pd.concat(seleccionados).head(n_etfs)
        df_final["peso"] = df_final["peso"] / df_final["peso"].sum()
        
        st.session_state.cartera_auto_etfs = df_final
        st.session_state.movs_auto_etfs = movs
        st.session_state.pesos_auto_etfs = pesos

if "cartera_auto_etfs" in st.session_state:
    df_final = st.session_state.cartera_auto_etfs
    movs = st.session_state.movs_auto_etfs
    pesos = st.session_state.pesos_auto_etfs
    
    st.divider()
    st.subheader("📋 Propuesta de Cartera ETFs")
    
    for tramo_db in df_final["tramo"].unique():
        peso_tramo = df_final[df_final["tramo"] == tramo_db]["peso"].sum()
        st.markdown(f'<div class="category-header">📦 {TRAMO_MAP_INV.get(tramo_db)} ({peso_tramo*100:.1f}%)</div>', unsafe_allow_html=True)
        sub = df_final[df_final["tramo"] == tramo_db].copy()
        viz = sub[["isin", "nombre", "ytm", "vol", "ter", "peso"]].copy()
        viz["peso"] = viz["peso"].map(lambda x: f"{x*100:.1f}%")
        viz["ytm"] = viz["ytm"].map(lambda x: f"{x:.2f}%")
        st.table(viz)

    # Métricas Consolidadas
    st.subheader("📊 Métricas de Cartera")
    total_ytm = (df_final["ytm"] * df_final["peso"]).sum()
    total_vol = (df_final["vol"] * df_final["peso"]).sum()
    total_dur = (df_final["duracion"] * df_final["peso"]).sum()
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(f'<div class="metric-card"><h4>YTM Medio</h4><h2>{total_ytm:.2f}%</h2></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><h4>Volatilidad Media</h4><h2>{total_vol:.2f}%</h2></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="metric-card"><h4>Duración Media</h4><h2>{total_dur:.2f} yr</h2></div>', unsafe_allow_html=True)
    with c4:
        be = total_ytm / total_dur if total_dur >0.1 else 9.9
        st.markdown(f'<div class="metric-card"><h4>Soporte Tipos</h4><h2>{be:.2f}%</h2></div>', unsafe_allow_html=True)

    # Pie Chart
    fig = go.Figure(data=[go.Pie(labels=df_final["nombre"], values=df_final["peso"], hole=.3)])
    fig.update_layout(title="Distribución de Activos", height=400)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col_c1, col_c2, col_c3 = st.columns([4,2,4])
    with col_c2:
        if st.button("✅ Confirmar y Guardar Cartera", type="primary", use_container_width=True):
            cartera_id = "AUTO-ETF-" + datetime.now().strftime("%Y%m%d-%H%M%S")
            etfs_lista = []
            for _, row in df_final.iterrows():
                etfs_lista.append({"isin": row["isin"], "nombre": row["nombre"], "peso": float(row["peso"]), "tramo": row["tramo"]})
            
            doc = {
                "cartera_id": cartera_id,
                "fecha_creacion": datetime.now(UTC),
                "origen": "A",
                "tipo": "ETF",
                "metas": {"perfil": perfil, "region": region},
                "metricas": {"ytm": total_ytm, "vol": total_vol, "dur": total_dur},
                "etfs": etfs_lista
            }
            try:
                carteras_etf_collection.insert_one(doc)
                st.balloons()
                st.success(f"Cartera {cartera_id} guardada con éxito.")
                del st.session_state.cartera_auto_etfs
                st.rerun()
            except Exception as e: st.error(f"Error: {e}")
else:
    st.info("Configura los parámetros y pulsa 'Generar Propuesta' para construir tu cartera de ETFs automática.")
