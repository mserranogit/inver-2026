#!/usr/bin/env python3
"""
Script para extraer datos adicionales de Morningstar usando mstarpy
Objetivo: Analizar fondos con rentabilidad 3-4% y riesgo bajo (1-2/7)
"""

import json
import pandas as pd
from datetime import datetime, timedelta
import time

# NOTA: Este script requiere que instales mstarpy:
# pip install mstarpy

try:
    from mstarpy import Funds
    MSTARPY_DISPONIBLE = True
except ImportError:
    MSTARPY_DISPONIBLE = False
    print("⚠️  mstarpy no está instalado. Instálalo con: pip install mstarpy")

# Lista de ISINs candidatos (fondos con rentabilidad 2.5%+ y riesgo 1-2/7)
FONDOS_CANDIDATOS = [
    "FR001400CFA4",  # OSTRUM Credit Ultra Short R/C - 2.99%
    "FI0008804463",  # EVLI Euro Liquidity B - 2.90%
    "LU0080237943",  # DWS Euro Ultra Short Fixed Income NC - 2.77%
    "LU0034353002",  # DWS Floating Rate Notes LC - 2.75%
    "LU1965927921",  # DWS ESG Floating Rate Notes LC - 2.70%
    "FR0013346079",  # Groupama Ultra Short Term _ NC - 2.54%
    "LU0293294277",  # Allianz Enhanced Short Term Euro AT - 2.51%
]

def extraer_datos_fondo(isin: str, pais: str = "es") -> dict:
    """
    Extrae todos los datos relevantes de un fondo usando mstarpy
    
    Args:
        isin: Código ISIN del fondo
        pais: Código de país para Morningstar (default: "es" para España)
    
    Returns:
        Diccionario con todos los datos del fondo
    """
    if not MSTARPY_DISPONIBLE:
        return {"error": "mstarpy no disponible"}
    
    try:
        print(f"\n🔍 Consultando {isin}...")
        
        # Crear objeto del fondo
        fondo = Funds(term=isin)
        
        # Extraer datos básicos
        datos = {
            "isin": isin,
            "nombre": None,
            "categoria_morningstar": None,
            "rating_estrellas": None,
            "analyst_rating": None,
            "sustainability_rating": None,
        }
        
        # 1. DATOS BÁSICOS Y RATINGS
        try:
            datos["nombre"] = fondo.name
            datos["categoria_morningstar"] = fondo.categoryName
            datos["rating_estrellas"] = fondo.starRating
            datos["analyst_rating"] = getattr(fondo, 'analystRating', None)
            datos["sustainability_rating"] = getattr(fondo, 'sustainabilityRating', None)
        except Exception as e:
            print(f"   ⚠️  Error obteniendo datos básicos: {e}")
        
        # 2. RENTABILIDADES HISTÓRICAS
        try:
            trading_data = fondo.trading_data()
            
            datos["rent_ytd"] = trading_data.get("ytdReturn", None)
            datos["rent_1y"] = trading_data.get("return1y", None)
            datos["rent_3y_anual"] = trading_data.get("return3y", None)
            datos["rent_5y_anual"] = trading_data.get("return5y", None)
            datos["rent_10y_anual"] = trading_data.get("return10y", None)
            datos["rent_desde_inicio"] = trading_data.get("returnSinceInception", None)
            
        except Exception as e:
            print(f"   ⚠️  Error obteniendo rentabilidades: {e}")
        
        # 3. MÉTRICAS DE RIESGO
        try:
            risk_data = fondo.risk_data()
            
            datos["volatilidad_3y"] = risk_data.get("volatility_3y", None)
            datos["volatilidad_5y"] = risk_data.get("volatility_5y", None)
            datos["sharpe_ratio_3y"] = risk_data.get("sharpeRatio_3y", None)
            datos["sharpe_ratio_5y"] = risk_data.get("sharpeRatio_5y", None)
            datos["sortino_ratio_3y"] = risk_data.get("sortinoRatio_3y", None)
            datos["max_drawdown_3y"] = risk_data.get("maxDrawdown_3y", None)
            datos["beta_3y"] = risk_data.get("beta_3y", None)
            datos["alfa_3y"] = risk_data.get("alpha_3y", None)
            
        except Exception as e:
            print(f"   ⚠️  Error obteniendo datos de riesgo: {e}")
        
        # 4. TAMAÑO Y GESTIÓN
        try:
            datos["aum_millones"] = fondo.nav  # Assets under management
            datos["fecha_inicio"] = fondo.inceptionDate
            datos["gestor"] = getattr(fondo, 'manager', None)
            datos["familia_fondos"] = fondo.fundFamily
            
        except Exception as e:
            print(f"   ⚠️  Error obteniendo datos de gestión: {e}")
        
        # 5. COSTES
        try:
            datos["ter"] = fondo.ongoingCharge  # Total Expense Ratio
            datos["comision_entrada"] = getattr(fondo, 'entryCharge', None)
            datos["comision_salida"] = getattr(fondo, 'exitCharge', None)
            datos["comision_gestion"] = getattr(fondo, 'managementFee', None)
            
        except Exception as e:
            print(f"   ⚠️  Error obteniendo costes: {e}")
        
        # 6. PORTFOLIO DATA (composición)
        try:
            portfolio_data = fondo.portfolio_data()
            
            datos["duracion_media"] = portfolio_data.get("duration", None)
            datos["top_10_holdings"] = portfolio_data.get("top10Holdings", None)
            datos["num_posiciones"] = portfolio_data.get("numberOfHoldings", None)
            
            # Distribución sectorial/crediticia si está disponible
            datos["distribucion_sectorial"] = portfolio_data.get("sectorBreakdown", None)
            datos["calidad_crediticia"] = portfolio_data.get("creditQuality", None)
            
        except Exception as e:
            print(f"   ⚠️  Error obteniendo datos de portfolio: {e}")
        
        # 7. BENCHMARK Y COMPARACIÓN
        try:
            datos["benchmark"] = fondo.benchmark
            datos["tracking_error"] = getattr(fondo, 'trackingError', None)
            datos["information_ratio"] = getattr(fondo, 'informationRatio', None)
            
        except Exception as e:
            print(f"   ⚠️  Error obteniendo datos de benchmark: {e}")
        
        # 8. SERIES HISTÓRICAS (últimos 3 años)
        try:
            fecha_fin = datetime.now()
            fecha_inicio = fecha_fin - timedelta(days=3*365)
            
            historial = fondo.historical(
                start_date=fecha_inicio.strftime("%Y-%m-%d"),
                end_date=fecha_fin.strftime("%Y-%m-%d"),
                frequency="monthly"
            )
            
            datos["historial_mensual"] = historial  # DataFrame con precio y rentabilidad mensual
            
        except Exception as e:
            print(f"   ⚠️  Error obteniendo historial: {e}")
        
        print(f"   ✅ Datos extraídos exitosamente")
        return datos
        
    except Exception as e:
        print(f"   ❌ Error general: {e}")
        return {"isin": isin, "error": str(e)}

def calcular_score_fondo(datos: dict) -> float:
    """
    Calcula un score para el fondo basado en los criterios:
    - Rentabilidad consistente 3-4%
    - Riesgo bajo (volatilidad, Sharpe alto)
    - Rating bueno
    - Costes bajos
    
    Returns:
        Score de 0 a 100
    """
    score = 0
    max_score = 100
    
    # 1. RENTABILIDAD (30 puntos)
    # Buscar rentabilidades cercanas a 3-4%
    rent_3y = datos.get("rent_3y_anual")
    if rent_3y is not None:
        if 3.0 <= rent_3y <= 4.0:
            score += 30
        elif 2.5 <= rent_3y < 3.0:
            score += 20
        elif 4.0 < rent_3y <= 4.5:
            score += 25
        elif 2.0 <= rent_3y < 2.5:
            score += 10
    
    # 2. SHARPE RATIO (25 puntos)
    # Mayor Sharpe = mejor rentabilidad ajustada al riesgo
    sharpe = datos.get("sharpe_ratio_3y")
    if sharpe is not None:
        if sharpe >= 1.5:
            score += 25
        elif sharpe >= 1.0:
            score += 20
        elif sharpe >= 0.5:
            score += 15
        elif sharpe >= 0:
            score += 10
    
    # 3. VOLATILIDAD (20 puntos)
    # Menor volatilidad = mejor para perfil conservador
    volatilidad = datos.get("volatilidad_3y")
    if volatilidad is not None:
        if volatilidad <= 1.0:
            score += 20
        elif volatilidad <= 2.0:
            score += 15
        elif volatilidad <= 3.0:
            score += 10
        elif volatilidad <= 5.0:
            score += 5
    
    # 4. RATING MORNINGSTAR (15 puntos)
    rating = datos.get("rating_estrellas")
    if rating is not None:
        if rating >= 4:
            score += 15
        elif rating >= 3:
            score += 10
        elif rating >= 2:
            score += 5
    
    # 5. COSTES (10 puntos)
    ter = datos.get("ter")
    if ter is not None:
        if ter <= 0.3:
            score += 10
        elif ter <= 0.5:
            score += 7
        elif ter <= 0.7:
            score += 5
        elif ter <= 1.0:
            score += 3
    
    return score

def analizar_todos_fondos():
    """
    Analiza todos los fondos candidatos y genera un ranking
    """
    if not MSTARPY_DISPONIBLE:
        print("\n❌ No se puede ejecutar el análisis sin mstarpy instalado")
        return
    
    print("="*100)
    print("🚀 ANÁLISIS COMPLETO CON MSTARPY - Fondos Rentabilidad 3-4% / Riesgo Bajo")
    print("="*100)
    
    resultados = []
    
    for isin in FONDOS_CANDIDATOS:
        datos = extraer_datos_fondo(isin)
        
        # Calcular score
        score = calcular_score_fondo(datos)
        datos["score"] = score
        
        resultados.append(datos)
        
        # Pequeña pausa para no saturar la API
        time.sleep(1)
    
    # Convertir a DataFrame y ordenar por score
    df = pd.DataFrame(resultados)
    df = df.sort_values("score", ascending=False)
    
    # Mostrar ranking
    print("\n" + "="*100)
    print("🏆 RANKING DE FONDOS (ordenados por score)")
    print("="*100)
    
    for idx, row in df.iterrows():
        print(f"\n{row.get('score', 0):.0f}/100 - {row.get('nombre', 'N/A')}")
        print(f"        ISIN: {row['isin']}")
        print(f"        Rating: {row.get('rating_estrellas', 'N/A')} ⭐")
        print(f"        Rent 3y: {row.get('rent_3y_anual', 'N/A')}")
        print(f"        Sharpe 3y: {row.get('sharpe_ratio_3y', 'N/A')}")
        print(f"        Volatilidad 3y: {row.get('volatilidad_3y', 'N/A')}%")
        print(f"        TER: {row.get('ter', 'N/A')}%")
    
    # Guardar resultados completos
    df.to_csv('/mnt/user-data/outputs/analisis_completo_mstarpy.csv', index=False, encoding='utf-8-sig')
    print(f"\n\n💾 Análisis completo guardado en: analisis_completo_mstarpy.csv")
    
    return df

def generar_informe_ejemplo():
    """
    Genera un informe de ejemplo mostrando qué tipo de análisis se puede hacer
    """
    print("\n" + "="*100)
    print("📝 EJEMPLO DE ANÁLISIS QUE PUEDES HACER CON LOS DATOS DE MSTARPY")
    print("="*100)
    
    ejemplo = """
Una vez tengas los datos de mstarpy, podrás hacer análisis como:

1️⃣  CONSISTENCIA DE RENTABILIDAD:
    • Comparar rent_1y, rent_3y, rent_5y
    • Ver si el fondo tiene rentabilidades estables año a año
    • Identificar fondos que mantienen 3-4% consistentemente vs fondos volátiles

2️⃣  ANÁLISIS RIESGO/RENTABILIDAD:
    • Gráfico de dispersión: Sharpe Ratio vs Rentabilidad
    • Identificar fondos con mejor Sharpe (máxima rentabilidad con mínimo riesgo)
    • Comparar volatilidad vs rentabilidad obtenida

3️⃣  ANÁLISIS DE COSTES:
    • Rentabilidad neta = Rentabilidad bruta - TER
    • Comparar fondos: ¿vale la pena pagar más comisión por mejor gestión?
    • Identificar fondos con mejores rentabilidades netas

4️⃣  CALIDAD DE LA GESTIÓN:
    • Tracking error bajo = gestor sigue bien su estrategia
    • Information ratio alto = gestor añade valor vs benchmark
    • Alfa positivo = gestor supera al mercado

5️⃣  ANÁLISIS DE PORTFOLIO:
    • Para fondos de renta fija: duración media
    • Calidad crediticia: ¿invierte en AAA o tiene más riesgo?
    • Diversificación: número de posiciones, concentración top 10

6️⃣  BACKTESTING:
    • Con historial_mensual, simular rentabilidad histórica
    • Calcular máximas caídas (drawdown) reales
    • Ver cómo se comportó el fondo en crisis (ej. COVID-19 2020)

═══════════════════════════════════════════════════════════════════════════════

🎯 OBJETIVO FINAL: Identificar 2-3 fondos que:
    ✓ Rentabilidad consistente 3-4% en últimos 3-5 años
    ✓ Sharpe Ratio > 1.0 (buena rentabilidad ajustada al riesgo)
    ✓ Volatilidad < 2% (realmente bajo riesgo)
    ✓ Rating Morningstar ≥ 3 estrellas
    ✓ TER < 0.5% (costes razonables)
    ✓ AUM suficiente para buena liquidez (>50M EUR)

═══════════════════════════════════════════════════════════════════════════════
"""
    print(ejemplo)

if __name__ == "__main__":
    if MSTARPY_DISPONIBLE:
        # Ejecutar análisis completo
        analizar_todos_fondos()
    else:
        # Mostrar información sobre qué se puede hacer
        print("\n⚠️  Este script requiere mstarpy. Instálalo con: pip install mstarpy")
        print("A continuación, se muestra qué tipo de análisis podrías hacer:\n")
        generar_informe_ejemplo()
        
        print("\n" + "="*100)
        print("📋 FONDOS CANDIDATOS PARA ANALIZAR:")
        print("="*100)
        for isin in FONDOS_CANDIDATOS:
            print(f"  • {isin}")
