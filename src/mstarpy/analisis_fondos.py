#!/usr/bin/env python3
"""
Análisis de fondos de inversión para criterios:
- Rentabilidad anual objetivo: 3-4%
- Riesgo bajo: 1-2/7
"""

import json
import pandas as pd
from typing import List, Dict


def cargar_fondos(ruta_json: str) -> List[Dict]:
    """Carga los datos de fondos desde el JSON"""
    with open(ruta_json, 'r', encoding='utf-8') as f:
        return json.load(f)


def limpiar_porcentaje(valor: str) -> float:
    """Convierte string de porcentaje a float"""
    try:
        return float(valor.replace('%', '').replace(',', '.').strip())
    except:
        return 0.0


def extraer_riesgo(riesgo_str: str) -> int:
    """Extrae el nivel de riesgo del formato '1/7'"""
    try:
        return int(riesgo_str.split('/')[0])
    except:
        return 0


def analizar_fondos(fondos: List[Dict]) -> pd.DataFrame:
    """Analiza los fondos y devuelve un DataFrame ordenado"""
    datos = []

    for fondo in fondos:
        nivel_riesgo = extraer_riesgo(fondo.get('riesgo', '0/7'))
        rent_2025 = limpiar_porcentaje(fondo.get('ren-2025', '0%'))
        comision = limpiar_porcentaje(fondo.get('comision', '0%'))
        rent_ytd = limpiar_porcentaje(fondo.get('ren-ytd', '0%'))

        # Calcular rentabilidad neta aproximada (después de comisiones)
        rent_neta_aprox = rent_2025 - comision

        datos.append({
            'Nombre': fondo.get('nombre', 'N/A'),
            'ISIN': fondo.get('isin', 'N/A'),
            'Tipo': fondo.get('tipoFondo', 'N/A'),
            'Subtipo': fondo.get('subtipoFondo', 'N/A'),
            'Riesgo': nivel_riesgo,
            'Rent 2025 (%)': rent_2025,
            'Rent YTD (%)': rent_ytd,
            'Comisión (%)': comision,
            'Rent Neta Aprox (%)': rent_neta_aprox,
            'Cumple Riesgo': nivel_riesgo <= 2,
            'Cumple Rentabilidad': 3.0 <= rent_2025 <= 4.0
        })

    df = pd.DataFrame(datos)
    return df


def filtrar_fondos_objetivo(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra fondos que cumplen ambos criterios"""
    return df[(df['Cumple Riesgo']) & (df['Cumple Rentabilidad'])].copy()


def fondos_cercanos(df: pd.DataFrame) -> pd.DataFrame:
    """Fondos que casi cumplen (rentabilidad 2.5-4.5%, riesgo 1-2)"""
    return df[
        (df['Cumple Riesgo']) &
        (df['Rent 2025 (%)'] >= 2.5) &
        (df['Rent 2025 (%)'] <= 4.5)
        ].copy()


def generar_informe(df: pd.DataFrame):
    """Genera un informe completo del análisis"""
    print("=" * 100)
    print("ANÁLISIS DE FONDOS - CRITERIOS: Rentabilidad 3-4% anual | Riesgo 1-2/7")
    print("=" * 100)

    total_fondos = len(df)
    fondos_riesgo_bajo = len(df[df['Cumple Riesgo']])

    print(f"\n📊 RESUMEN GENERAL:")
    print(f"   • Total de fondos analizados: {total_fondos}")
    print(f"   • Fondos con riesgo 1-2/7: {fondos_riesgo_bajo}")

    # Fondos que cumplen criterios exactos
    fondos_objetivo = filtrar_fondos_objetivo(df)
    print(f"\n✅ FONDOS QUE CUMPLEN AMBOS CRITERIOS (Rent 3-4%, Riesgo 1-2): {len(fondos_objetivo)}")

    if len(fondos_objetivo) > 0:
        print("\n" + "─" * 100)
        for idx, fondo in fondos_objetivo.iterrows():
            print(f"\n🎯 {fondo['Nombre'][:60]}")
            print(f"   ISIN: {fondo['ISIN']} | Tipo: {fondo['Tipo']}")
            if pd.notna(fondo['Subtipo']):
                print(f"   Subtipo: {fondo['Subtipo']}")
            print(f"   Riesgo: {fondo['Riesgo']}/7")
            print(f"   Rentabilidad 2025: {fondo['Rent 2025 (%)']:.2f}%")
            print(f"   Rentabilidad YTD: {fondo['Rent YTD (%)']:.2f}%")
            print(f"   Comisión: {fondo['Comisión (%)']:.2f}%")
            print(f"   Rentabilidad Neta Aprox: {fondo['Rent Neta Aprox (%)']:.2f}%")
    else:
        print("   ⚠️  No se encontraron fondos que cumplan exactamente ambos criterios")

    # Fondos cercanos a los criterios
    fondos_cerca = fondos_cercanos(df)
    fondos_cerca_unicos = fondos_cerca[~fondos_cerca['ISIN'].isin(fondos_objetivo['ISIN'])]

    print(f"\n\n📌 FONDOS CERCANOS A LOS CRITERIOS (Rent 2.5-4.5%, Riesgo 1-2): {len(fondos_cerca_unicos)}")

    if len(fondos_cerca_unicos) > 0:
        # Ordenar por rentabilidad 2025 descendente
        fondos_cerca_unicos = fondos_cerca_unicos.sort_values('Rent 2025 (%)', ascending=False)
        print("\n" + "─" * 100)

        for idx, fondo in fondos_cerca_unicos.head(10).iterrows():
            print(f"\n📍 {fondo['Nombre'][:60]}")
            print(f"   ISIN: {fondo['ISIN']} | Tipo: {fondo['Tipo']}")
            if pd.notna(fondo['Subtipo']):
                print(f"   Subtipo: {fondo['Subtipo']}")
            print(f"   Riesgo: {fondo['Riesgo']}/7")
            print(f"   Rentabilidad 2025: {fondo['Rent 2025 (%)']:.2f}%")
            print(f"   Rentabilidad YTD: {fondo['Rent YTD (%)']:.2f}%")
            print(f"   Comisión: {fondo['Comisión (%)']:.2f}%")
            print(f"   Rentabilidad Neta Aprox: {fondo['Rent Neta Aprox (%)']:.2f}%")

            # Indicar por qué no cumple exactamente
            if fondo['Rent 2025 (%)'] < 3.0:
                print(f"   ⚠️  Rentabilidad ligeramente baja ({fondo['Rent 2025 (%)']:.2f}% < 3%)")
            elif fondo['Rent 2025 (%)'] > 4.0:
                print(f"   ⚠️  Rentabilidad superior al rango ({fondo['Rent 2025 (%)']:.2f}% > 4%)")

    # Estadísticas adicionales
    print("\n\n" + "=" * 100)
    print("📈 ESTADÍSTICAS DE FONDOS CON RIESGO 1-2/7:")
    print("=" * 100)

    fondos_bajo_riesgo = df[df['Cumple Riesgo']].copy()

    if len(fondos_bajo_riesgo) > 0:
        print(f"\n   Rentabilidad 2025:")
        print(f"   • Media: {fondos_bajo_riesgo['Rent 2025 (%)'].mean():.2f}%")
        print(f"   • Mediana: {fondos_bajo_riesgo['Rent 2025 (%)'].median():.2f}%")
        print(f"   • Máxima: {fondos_bajo_riesgo['Rent 2025 (%)'].max():.2f}%")
        print(f"   • Mínima: {fondos_bajo_riesgo['Rent 2025 (%)'].min():.2f}%")

        print(f"\n   Comisiones:")
        print(f"   • Media: {fondos_bajo_riesgo['Comisión (%)'].mean():.2f}%")
        print(f"   • Mediana: {fondos_bajo_riesgo['Comisión (%)'].median():.2f}%")
        print(f"   • Máxima: {fondos_bajo_riesgo['Comisión (%)'].max():.2f}%")
        print(f"   • Mínima: {fondos_bajo_riesgo['Comisión (%)'].min():.2f}%")

        print(f"\n   Distribución por tipo:")
        tipo_dist = fondos_bajo_riesgo['Tipo'].value_counts()
        for tipo, count in tipo_dist.items():
            print(f"   • {tipo}: {count} fondos ({count / len(fondos_bajo_riesgo) * 100:.1f}%)")


def mostrar_datos_adicionales_mstarpy():
    """Muestra qué datos adicionales se necesitan obtener con mstarpy"""
    print("\n\n" + "=" * 100)
    print("🔍 DATOS ADICIONALES A OBTENER CON MSTARPY PARA MEJOR ANÁLISIS")
    print("=" * 100)

    print("""
Para hacer una selección más informada de fondos con rentabilidad 3-4% y riesgo bajo,
deberías obtener estos datos adicionales usando mstarpy:

📊 DATOS CRÍTICOS PARA TU ANÁLISIS:

1️⃣  RENTABILIDADES HISTÓRICAS (para evaluar consistencia):
    • Rentabilidad a 3 años (anualizada)
    • Rentabilidad a 5 años (anualizada)
    • Rentabilidad año a año (últimos 3-5 años)
    ➜ Esto te ayuda a ver si el fondo es consistente o tuvo un año excepcional

2️⃣  MÉTRICAS DE RIESGO AVANZADAS:
    • Volatilidad (desviación estándar) - confirmar que el riesgo es realmente bajo
    • Sharpe Ratio - rentabilidad ajustada al riesgo (cuanto mayor, mejor)
    • Máxima caída histórica (drawdown) - peor pérdida que has podido tener
    ➜ El nivel de riesgo 1-2/7 es una escala, pero necesitas ver la volatilidad real

3️⃣  RATING MORNINGSTAR:
    • Estrellas Morningstar (1-5 estrellas)
    • Analyst Rating (Gold, Silver, Bronze...)
    ➜ Validación externa de la calidad del fondo

4️⃣  TAMAÑO Y LIQUIDEZ:
    • AUM (Assets Under Management) - patrimonio del fondo
    • Flujos de entrada/salida de capital
    ➜ Fondos muy pequeños pueden tener problemas de liquidez

5️⃣  DATOS DE GESTIÓN:
    • Nombre del gestor
    • Años gestionando el fondo
    • Fecha de inicio del fondo
    ➜ Experiencia del gestor y track record

6️⃣  COSTES REALES COMPLETOS:
    • TER (Total Expense Ratio) - todos los gastos
    • Comisiones de entrada/salida
    ➜ Tu JSON tiene "comisión" pero puede haber otros costes ocultos

7️⃣  COMPOSICIÓN DE CARTERA (para fondos de renta fija):
    • Duración media de la cartera
    • Calidad crediticia (% AAA, AA, A, BBB...)
    • Top 10 posiciones
    ➜ Entender QUÉ estás comprando realmente

8️⃣  BENCHMARK Y COMPARACIÓN:
    • Índice de referencia del fondo
    • Rentabilidad vs benchmark
    • Percentil en su categoría Morningstar
    ➜ Ver si el gestor añade valor o solo sigue el mercado

═══════════════════════════════════════════════════════════════════════════════

💡 RECOMENDACIÓN PARA EL SCRIPT CON MSTARPY:

Para cada fondo con ISIN, el script debería:

1. Buscar el fondo por ISIN en Morningstar
2. Extraer los datos críticos listados arriba
3. Calcular un "score" basado en tus criterios:
   - Consistencia de rentabilidad (3-4% en varios años)
   - Volatilidad baja confirmada
   - Sharpe Ratio alto
   - Rating Morningstar bueno
   - Costes bajos

4. Generar un ranking de los mejores fondos según tu perfil

═══════════════════════════════════════════════════════════════════════════════

🎯 EJEMPLO DE CÓDIGO MSTARPY (estructura básica):

```python
from mstarpy import Funds

# Para cada ISIN de tu lista
isin = "LU0293294277"  # Ejemplo: Allianz Enhanced Short Term Euro

# Crear objeto del fondo
fondo = Funds(term=isin, country="es")  # country="es" para Morningstar España

# Obtener datos
datos_principales = fondo.trading_data()
historial = fondo.historical(start_date="2020-01-01", end_date="2025-01-30")
portfolio = fondo.portfolio_data()
analisis_riesgo = fondo.risk_data()

# Extraer métricas específicas
rating_estrellas = fondo.starRating
sharpe_ratio = analisis_riesgo['sharpeRatio_3y']
volatilidad = analisis_riesgo['volatility_3y']
max_drawdown = analisis_riesgo['maxDrawdown_3y']
```

═══════════════════════════════════════════════════════════════════════════════
""")


def main():
    # Cargar y analizar fondos
    ruta_json = '../../assets/json/fondos_open_R1.json'
    fondos = cargar_fondos(ruta_json)
    df = analizar_fondos(fondos)

    # Generar informe
    generar_informe(df)

    # Mostrar datos adicionales necesarios
    mostrar_datos_adicionales_mstarpy()

    # Guardar resultados en CSV para análisis posterior
    fondos_objetivo = filtrar_fondos_objetivo(df)
    fondos_cercanos_df = fondos_cercanos(df)

    if len(fondos_objetivo) > 0 or len(fondos_cercanos_df) > 0:
        # Combinar ambos DataFrames
        resultado = pd.concat([fondos_objetivo, fondos_cercanos_df]).drop_duplicates(subset=['ISIN'])
        resultado = resultado.sort_values('Rent 2025 (%)', ascending=False)

        # Guardar
        ruta_salida = '../../assets/csv/fondos_seleccionados.csv'
        resultado.to_csv(ruta_salida, index=False, encoding='utf-8-sig')
        print(f"\n\n💾 Resultados guardados en: fondos_seleccionados.csv")
        print(f"   Total de fondos en el archivo: {len(resultado)}")
        print(f"   Estos son los fondos candidatos para análisis con mstarpy")


if __name__ == "__main__":
    main()
