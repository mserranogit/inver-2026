import mstarpy
import json
import os

def test_mstarpy_funds(isin_list):
    results = []
    for isin in isin_list:
        print(f"--- Probando mstarpy con ISIN: {isin} ---")
        try:
            # Buscar el fondo en Morningstar
            # Usamos country='es' para asegurarnos de obtener datos locales si es necesario
            fund = mstarpy.Funds(term=isin, country="es")
            
            # Obtener datos básicos
            name = fund.name
            print(f"Nombre: {name}")
            
            # 1. Intentar obtener Holdings (Composición)
            print("Buscando Holdings...")
            holdings = fund.holdings()
            
            # 2. Intentar obtener Rentabilidades (Performance)
            print("Buscando Rentabilidades...")
            perf = fund.performance()
            
            # 3. Intentar obtener Ratios de Riesgo (Volatilidad, etc.)
            print("Buscando Ratios de Riesgo...")
            risk = fund.risk()

            data = {
                "isin": isin,
                "nombre_mstar": name,
                "tiene_holdings": holdings is not None and not (isinstance(holdings, list) and len(holdings) == 0),
                "tiene_performance": perf is not None,
                "tiene_risk": risk is not None,
                "detalles": {
                    "sector_weighting": fund.sector_weighting(),
                    "risk_ratios": risk.to_dict() if hasattr(risk, 'to_dict') else str(risk),
                    "top_holdings": holdings.head(10).to_dict() if hasattr(holdings, 'head') else str(holdings)
                }
            }
            results.append(data)
            
        except Exception as e:
            print(f"Error con {isin}: {e}")
            results.append({"isin": isin, "error": str(e)})

    return results

# ISINs de tu archivo
isins = ["LU0293294277", "FR0011387299", "LU0568620560"]
data_mstar = test_mstarpy_funds(isins)

# Guardar prueba
with open('assets/json/prueba_mstarpy.json', 'w', encoding='utf-8') as f:
    json.dump(data_mstar, f, indent=2, ensure_ascii=False)

print("✅ Prueba finalizada. Revisa assets/json/prueba_mstarpy.json")
