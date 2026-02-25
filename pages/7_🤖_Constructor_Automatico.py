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
carteras_collection = db["carteras"]

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

def get_latest_curve(pais_code="EUR"):
    ultimo = curvas_collection.find_one(sort=[("_id", -1)])
    if not ultimo:
        return None
    
    if pais_code == "GLOBAL":
        # Calculamos una curva promedio de todas las regiones disponibles
        all_paises = ultimo.get("paises", [])
        if not all_paises:
            return None
        
        # Estructura base cogiendo el primero (para tener plazos)
        global_curve = {
            "codigo": "GLOBAL",
            "nombre": "Global",
            "emoji": "🌐",
            "plazos": []
        }
        
        # Mapear plazos comunes
        plazos_data = {} # plazo -> [actuals], [prevs_per_year]
        
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
            global_curve["plazos"].append({
                "plazo": pl_cod,
                "rendimiento_actual": avg_actual,
                "previsiones": avg_prevs
            })
        return global_curve

    for p in ultimo.get("paises", []):
        if p["codigo"] == pais_code:
            return p
    return None

TRAMO_MAP = {
    "Monetario": "very_short",
    "Corto Plazo": "short",
    "Medio Plazo": "intermediate",
    "Largo Plazo": "long"
}

def predecir_movimiento_tipos(curve, horizon_years=3):
    """
    Calcula el movimiento esperado de tipos para cada tramo.
    """
    if not curve:
        return {}
    
    ahora_anno = datetime.now().year
    target_year = str(ahora_anno + min(horizon_years, 3)) 
    
    movimientos = {}
    for p in curve.get("plazos", []):
        plazo = p["plazo"]
        actual = p["rendimiento_actual"]
        proyectado = p.get("previsiones", {}).get(target_year)
        
        if proyectado is not None:
            delta = actual - proyectado
            movimientos[plazo] = {
                "actual": actual,
                "proyectado": proyectado,
                "delta": delta
            }
    
    # Mapeo a categorías de fondos (usando claves internas de la BD)
    mapeo = {
        "very_short": movimientos.get("3M") or movimientos.get("6M"),
        "short": movimientos.get("2Y") or movimientos.get("1Y"),
        "intermediate": movimientos.get("5Y"),
        "long": movimientos.get("10Y") or movimientos.get("30Y")
    }
    
    return {k: v for k, v in mapeo.items() if v}

def calcular_pesos_por_perfil(perfil, movimientos):
    """
    Define los pesos de la cartera según el perfil de riesgo.
    """
    # Mapeo de perfiles a pesos (usando claves internas de la BD)
    if perfil == "Conservador":
        return {"very_short": 0.50, "short": 0.40, "intermediate": 0.10, "long": 0.0}
    elif perfil == "Moderado":
        return {"very_short": 0.20, "short": 0.30, "intermediate": 0.40, "long": 0.10}
    else: # Atrevido
        duraciones_standard = {"very_short": 0.2, "short": 2.0, "intermediate": 5.0, "long": 12.0}
        scores = {}
        for tramo, m in movimientos.items():
            dur = duraciones_standard.get(tramo, 1.0)
            scores[tramo] = m["actual"] + (m["delta"] * dur)
        
        sorted_tramos = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        pesos = {t: 0.0 for t in ["very_short", "short", "intermediate", "long"]}
        
        # Repartir pesos según los mejores scores
        pesos[sorted_tramos[0][0]] = 0.50
        if len(sorted_tramos) > 1: pesos[sorted_tramos[1][0]] = 0.30
        if len(sorted_tramos) > 2: pesos[sorted_tramos[2][0]] = 0.20
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
        # Siempre usamos Ratio de Eficiencia, el usuario elige la base
        criterio_base = st.radio("Optimizar Eficiencia basada en:", ["Rent. Proyectada (YTM)", "Rent. Pasada (1A)"], 
                                 index=0, key=f"criterio_auto{suffix}", on_change=borrar_propuesta)

    with col4:
        st.write("") # Espaciador
        st.write("")
        generar = st.button("🚀 Generar Propuesta", type="primary", width="stretch")
        
        # Botón Reset en parámetros usando callback
        st.button("🔄 Reset Parámetros", width="stretch", on_click=reset_todo)

# --- PROCESAMIENTO ---
if generar:
    with st.spinner("🔍 Consultando base de datos y optimizando cartera..."):
        # 1. Obtener Curva
        curva = get_latest_curve(region)
        if not curva:
            st.error(f"No se han encontrado datos de curvas para la región {region}. Por favor, actualiza los datos en la página de Curvas.")
            st.stop()
            
        movs = predecir_movimiento_tipos(curva, horizonte)
        pesos = calcular_pesos_por_perfil(perfil, movs)
        
        # 2. Obtener Fondos (Query explícita a BD con filtro de región)
        tramos_interes = [t for t, p in pesos.items() if p > 0]
        
        # Filtro geográfico basado en el código de región
        query = {"tramo_rf": {"$in": tramos_interes}}
        
        if region == "EUR":
            query["categoria"] = {"$regex": "EUR|Euro", "$options": "i"}
        elif region == "US":
            query["categoria"] = {"$regex": "USD|U.S.|US ", "$options": "i"}
        elif region == "JP":
            query["categoria"] = {"$regex": "JPY|Yen|Japan", "$options": "i"}
        elif region == "CN":
            query["categoria"] = {"$regex": "CNY|China", "$options": "i"}
        # Si es GLOBAL, no filtramos por categoría geográfica para permitir de todo
        
        fondos_raw = list(fondos_collection.find(query, {
            "_id": 0,
            "isin": 1,
            "nombre": 1,
            "tramo_rf": 1,
            "duration.yield_to_maturity": 1,
            "duration.avg_effective_duration": 1,
            "riesgo.for3Year.volatility": 1,
            "riesgo.for1Year.volatility": 1,
            "rentabilidad.historica.y1": 1
        }))
        
        df_fondos = pd.json_normalize(fondos_raw)
        
        if df_fondos.empty:
            st.warning("No se encontraron fondos suficientes en las categorías seleccionadas.")
            st.stop()
            
        # Renombrar para facilitar con seguridad
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
        
        # Rellenar nulos en las columnas clave
        if "ytm" in df_fondos.columns:
            df_fondos["ytm"] = df_fondos["ytm"].fillna(0.0)
        
        if "vol" in df_fondos.columns:
            # Fallback a volatilidad de 1 año si no hay de 3 años
            if "riesgo.for1Year.volatility" in df_fondos.columns:
                df_fondos["vol"] = df_fondos["vol"].fillna(df_fondos["riesgo.for1Year.volatility"])
            df_fondos["vol"] = df_fondos["vol"].fillna(0.0)
        
        if "rent1y" in df_fondos.columns:
            df_fondos["rent1y"] = df_fondos["rent1y"].fillna(0.0)
        
        if "duracion" in df_fondos.columns:
            df_fondos["duracion"] = df_fondos["duracion"].fillna(0.0)
        
        # 3. SELECCIÓN AUTOMÁTICA
        seleccionados = []
        
        # Determinar columna de rentabilidad base
        rent_col = "ytm" if "Proyectada" in criterio_base else "rent1y"

        for tramo in tramos_interes:
            peso_tramo = pesos[tramo]
            n_tramo = m_fondos # Max fondos por tramo
            
            subset = df_fondos[df_fondos["tramo_rf"] == tramo].copy()
            
            if subset.empty:
                continue

            # SIEMPRE calculamos Eficiencia (Ratio)
            # Evitamos división por cero con .replace(0, 0.001)
            subset["eficiencia"] = subset[rent_col] / subset["vol"].replace(0, 0.001)
            
            # Ordenación Principal: Ratio de Eficiencia (DESC)
            # Desempate: Rentabilidad (DESC)
            subset = subset.sort_values(by=["eficiencia", rent_col], ascending=[False, False])
            
            # Tomamos los top m
            top_tramo = subset.head(n_tramo).copy()
            
            if not top_tramo.empty:
                # Re-ajustar peso individual dentro del tramo
                top_tramo["peso"] = peso_tramo / len(top_tramo)
                seleccionados.append(top_tramo)
                
        if not seleccionados:
            st.error("No se pudo construir la selección con los criterios actuales.")
            st.stop()
            
        df_final = pd.concat(seleccionados)
        
        # Limitar al total de N fondos si nos hemos pasado (priorizando los de mayor peso/categoría)
        if len(df_final) > n_fondos:
            df_final = df_final.head(n_fondos)
            # Recalcular pesos para que sumen 100%
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

    # --- SECCIÓN 2: TABLA DE SELECCIÓN ---
    st.markdown("---")
    st.subheader("📋 Propuesta de Selección")
    
    # Invertir mapa para visualización
    TRAMO_MAP_INV = {v: k for k, v in TRAMO_MAP.items()}

    # Agrupar por categoría para mostrar
    for tramo_db in df_final["tramo_rf"].unique():
        nombre_tramo = TRAMO_MAP_INV.get(tramo_db, tramo_db)
        st.markdown(f'<div class="category-header">📦 {nombre_tramo} ({pesos.get(tramo_db, 0)*100:.1f}% de la cartera)</div>', unsafe_allow_html=True)
        
        cols_mostrar = ["isin", "nombre", "ytm", "vol", "duracion"]
        if "eficiencia" in df_final.columns:
            cols_mostrar.append("eficiencia")
        cols_mostrar.append("peso")
        
        sub_tramo = df_final[df_final["tramo_rf"] == tramo_db][cols_mostrar].copy()
        sub_tramo["peso"] = sub_tramo["peso"].map(lambda x: f"{x*100:.2f}%")
        sub_tramo["ytm"] = sub_tramo["ytm"].map(lambda x: f"{x:.2f}%")
        sub_tramo["vol"] = sub_tramo["vol"].map(lambda x: f"{x:.2f}%")
        if "eficiencia" in sub_tramo.columns:
            sub_tramo["eficiencia"] = sub_tramo["eficiencia"].map(lambda x: f"{x:.2f}")
        
        st.table(sub_tramo)

    # --- SECCIÓN 3: MÉTRICAS TOTALES ---
    st.markdown("---")
    st.subheader("📊 Métricas Consolidadas")
    
    # Cálculos totales
    total_ytm = (df_final["ytm"] * df_final["peso"]).sum()
    total_vol = (df_final["vol"] * df_final["peso"]).sum()
    total_dur = (df_final["duracion"] * df_final["peso"]).sum()
    
    # Cálculo de "Rentabilidad Estimada" (Yield + Ganancia Capital Proyectada)
    # R_est = Sum ( Peso * (YTM + Delta_Tramo * Duracion_Fondo / Horizonte) )
    rent_proyectada = 0
    for _, row in df_final.iterrows():
        tramo = row["tramo_rf"]
        delta_mov = movs.get(tramo, {"delta": 0})["delta"]
        # Ganancia capital total en el periodo / horizonte
        ganancia_cap_anual = (delta_mov * row["duracion"]) / horizonte
        rent_proyectada += row["peso"] * (row["ytm"] + ganancia_cap_anual)
        
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f'<div class="metric-card"><h4>Rent. Esperada (Total)</h4><h2>{rent_proyectada:.2f}%</h2><p>Cupón + Previsión</p></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><h4>YTM Medio</h4><h2>{total_ytm:.2f}%</h2><p>Rendimiento cupones</p></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><h4>Volatilidad Media</h4><h2>{total_vol:.2f}%</h2><p>Riesgo histórico</p></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-card"><h4>Duración Media</h4><h2>{total_dur:.2f} yr</h2><p>Sensibilidad tipos</p></div>', unsafe_allow_html=True)
    with c5:
        # Ratio Margen de Seguridad (Break-even)
        breakeven = total_ytm / total_dur if total_dur > 0.1 else 9.99
        be_label = f"{breakeven:.2f}%" if total_dur > 0.1 else "Máximo"
        st.markdown(f'<div class="metric-card" style="border-left-color: #f59e0b;"><h4>Margen Seguridad</h4><h2>{be_label}</h2><p>Subida tipos soportada</p></div>', unsafe_allow_html=True)

    # Gráfico de pesos - MÁS GRANDE
    st.write("")
    labels_es = [TRAMO_MAP_INV.get(t, t) for t in df_final["tramo_rf"]]
    fig = go.Figure(data=[go.Pie(
        labels=labels_es, 
        values=df_final["peso"], 
        hole=.4,
        marker=dict(colors=['#4a6fa5', '#6366f1', '#8b5cf6', '#a855f7']),
        textinfo='label+percent'
    )])
    fig.update_layout(
        title_text="Distribución Estratégica de la Cartera", 
        template="plotly_dark", 
        height=480, # Tamaño aumentado
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
    )
    st.plotly_chart(fig, width="stretch")

    # --- NUEVA SECCIÓN: ANÁLISIS DETALLADO DEL FONDO ---
    st.markdown("---")
    st.subheader("🔍 Análisis Detallado de Fondos Elegidos")
    
    lista_fondos_nombres = {f"{row['nombre']} ({row['isin']})": row['isin'] for _, row in df_final.iterrows()}
    fondo_seleccionado_label = st.selectbox("Selecciona un fondo para ver su radiografía completa:", 
                                           list(lista_fondos_nombres.keys()), 
                                           key=f"detalles_fondo{suffix}")
    
    if fondo_seleccionado_label:
        isin_det = lista_fondos_nombres[fondo_seleccionado_label]
        f_doc = fondos_collection.find_one({"isin": isin_det}, {"_id": 0})
        
        if f_doc:
            tab_fund, tab_risk, tab_perf, tab_alloc = st.tabs([
                "🏛️ Fundamental", "⚠️ Riesgo", "📈 Rentabilidad", "🌍 Composición"
            ])
            
            with tab_fund:
                dur_data = f_doc.get('duration', {}) or {}
                c_f1, c_f2, c_f3 = st.columns(3)
                with c_f1:
                    st.metric("YTM (Rent. Esperada)", f"{dur_data.get('yield_to_maturity')}%")
                with c_f2:
                    st.metric("Duración Efectiva", f"{dur_data.get('avg_effective_duration')} años")
                with c_f3:
                    st.metric("Calidad Crediticia", f_doc.get('duration', {}).get('avg_credit_quality', "N/A"))
                st.info("💡 Mayor YTM indica mayor potencial de retorno, pero vigila la duración ante subidas de tipos.")

            with tab_risk:
                riesgo = f_doc.get('riesgo', {}) or {}
                r3y = riesgo.get('for3Year', {})
                vol_v = r3y.get('volatility') or riesgo.get('for1Year', {}).get('volatility')
                sha_v = r3y.get('sharpe') or riesgo.get('for1Year', {}).get('sharpe')
                
                cr1, cr2 = st.columns(2)
                with cr1: st.metric("Volatilidad", f"{vol_v}%")
                with cr2: st.metric("Ratio Sharpe", f"{sha_v}")
                
                # Gauge de Riesgo simplificado
                if vol_v:
                    fig_v = go.Figure(go.Indicator(
                        mode = "gauge+number", value = vol_v,
                        title = {'text': "Volatilidad %"},
                        gauge = {'axis': {'range': [0, 15]}, 'bar': {'color': "#d9534f"}}
                    ))
                    fig_v.update_layout(height=250, margin=dict(l=20, r=20, t=40, b=20))
                    st.plotly_chart(fig_v, width="stretch")

            with tab_perf:
                hist = f_doc.get('rentabilidad', {}).get('historica', {})
                if hist:
                    st.write("Rentabilidades anuales históricas:")
                    valid_h = {k: v for k, v in hist.items() if isinstance(v, (int, float))}
                    if valid_h:
                        st.bar_chart(pd.Series(valid_h), color="#4a6fa5")
                        st.table(pd.DataFrame([hist]))
                else: st.info("Sin datos históricos detallados.")

            with tab_alloc:
                alloc = f_doc.get('allocation_map', {})
                ca1, ca2 = st.columns(2)
                with ca1:
                    assets = alloc.get('globalAssetClasses', {})
                    if assets:
                        st.write("**Por Activo**")
                        st.dataframe(pd.DataFrame(list(assets.items()), columns=["Tipo", "%"]), hide_index=True)
                with ca2:
                    sectors = alloc.get('fixedIncomeSectors', {})
                    if sectors:
                        st.write("**Por Sector**")
                        st.dataframe(pd.DataFrame(list(sectors.items()), columns=["Sector", "%"]), hide_index=True)

    # --- SECCIÓN 4: ACCIONES FINALIZAR ---
    st.markdown("---")
    
    col_acc1, col_acc2, col_acc3, col_acc4 = st.columns([3.5, 1.5, 1.5, 3.5])
    
    with col_acc2:
        confirmar = st.button("✅ Confirmar", type="primary", width="stretch")
    
    with col_acc3:
        st.button("🔄 Reset", width="stretch", on_click=reset_todo)
        
    if confirmar:
        cartera_id = "AUTO-" + datetime.now().strftime("%Y%m%d-%H%M%S")
        
        # Preparar documento
        fondos_lista = []
        for _, row in df_final.iterrows():
            fondos_lista.append({
                "isin": str(row["isin"]),
                "nombre": str(row["nombre"]),
                "peso": float(row["peso"]),
                "tramo": str(row["tramo_rf"])
            })
            
        cartera_doc = {
            "cartera_id": cartera_id,
            "fecha_creacion": datetime.now(UTC),
            "origen": "A", # Automático
            "perfil": perfil,
            "region": region,
            "horizonte": horizonte,
            "metricas_proyectadas": {
                "rent_total": float(rent_proyectada),
                "ytm": float(total_ytm),
                "volatilidad": float(total_vol),
                "duracion": float(total_dur)
            },
            "fondos": fondos_lista
        }
        
        try:
            result = carteras_collection.insert_one(cartera_doc)
            if result.inserted_id:
                st.balloons()
                st.success(f"Cartera automática {cartera_id} guardada con éxito.")
                keys_to_clear = ["cartera_auto", "params_auto"]
                for k in keys_to_clear:
                    if k in st.session_state:
                        del st.session_state[k]
                st.rerun()
        except Exception as e:
            st.error(f"Error al guardar: {e}")

else:
    st.info("Configura los parámetros arriba y pulsa 'Generar Propuesta' para que el algoritmo seleccione los mejores fondos para ti.")
