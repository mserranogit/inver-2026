import requests
from bs4 import BeautifulSoup
import time
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Configuración MongoDB
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'admin',
    'password': 'mike',
    'database': 'db-inver',
    'collection': 'etfs',
    'auth_source': 'admin'
}

def get_headers():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ]
    return {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    }

def scrape_justetf_details(isin):
    """Extrae detalles de JustETF para un ISIN dado"""
    url = f"https://www.justetf.com/en/etf-profile.html?isin={isin}"
    
    try:
        response = requests.get(url, headers=get_headers(), timeout=15)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        details = {
            "justetf_url": url,
            "ter": None,
            "dividend_policy": None,
            "replication_method": None,
            "fund_size": None,
            "yield_1y": None,
            "yield_3y": None,
            "yield_5y": None,
            "volatility_1y": None,
            "volatility_3y": None,
            "volatility_5y": None,
            "return_per_risk_1y": None,
            "return_per_risk_3y": None,
            "return_per_risk_5y": None,
            "max_drawdown_1y": None,
            "max_drawdown_3y": None,
            "max_drawdown_5y": None,
            "max_drawdown_inception": None,
            "last_update_justetf": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # Selectores usando data-testid (más estables)
        ter_elem = soup.select_one('[data-testid="etf-profile-header_ter-value"]')
        if ter_elem: details["ter"] = ter_elem.get_text(strip=True)

        div_elem = soup.select_one('[data-testid="etf-profile-header_distribution-policy-value"]')
        if div_elem: details["dividend_policy"] = div_elem.get_text(strip=True)

        repl_elem = soup.select_one('[data-testid="etf-profile-header_replication-value"]')
        if repl_elem: details["replication_method"] = repl_elem.get_text(strip=True)

        size_elem = soup.select_one('[data-testid="etf-profile-header_fund-size-value"]')
        if size_elem: details["fund_size"] = size_elem.get_text(strip=True)

        # Extraer Riesgo y Rentabilidad (Buscando por texto en las tablas)
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    val = cells[1].get_text(strip=True)
                    
                    # Rentabilidades
                    if "1 year" in label and not details["yield_1y"] and "volatility" not in label and "drawdown" not in label and "risk" not in label:
                        details["yield_1y"] = val
                    elif "3 years" in label and not details["yield_3y"] and "volatility" not in label and "drawdown" not in label and "risk" not in label:
                        details["yield_3y"] = val
                    elif "5 years" in label and not details["yield_5y"] and "volatility" not in label and "drawdown" not in label and "risk" not in label:
                        details["yield_5y"] = val
                        
                    # Volatilidades
                    elif "volatility 1 year" in label:
                        details["volatility_1y"] = val
                    elif "volatility 3 years" in label:
                        details["volatility_3y"] = val
                    elif "volatility 5 years" in label:
                        details["volatility_5y"] = val
                        
                    # Ratios de Rentabilidad / Riesgo (Equivalente Sharpe)
                    elif "return per risk 1 year" in label:
                        details["return_per_risk_1y"] = val
                    elif "return per risk 3 years" in label:
                        details["return_per_risk_3y"] = val
                    elif "return per risk 5 years" in label:
                        details["return_per_risk_5y"] = val

                    # Drawdowns
                    elif "maximum drawdown 1 year" in label:
                        details["max_drawdown_1y"] = val
                    elif "maximum drawdown 3 years" in label:
                        details["max_drawdown_3y"] = val
                    elif "maximum drawdown 5 years" in label:
                        details["max_drawdown_5y"] = val
                    elif "maximum drawdown since inception" in label:
                        details["max_drawdown_inception"] = val

        return details

    except Exception as e:
        print(f"Error scraping {isin}: {e}")
        return None

def main():
    # Conectar a MongoDB
    try:
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['auth_source']}"
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[MONGO_CONFIG['database']]
        collection = db[MONGO_CONFIG['collection']]
        
        # Obtener todos los ETFs que no tienen todavía los nuevos ratios
        query = {"return_per_risk_1y": {"$exists": False}}
        etfs = list(collection.find(query))
        
        print(f"Encontrados {len(etfs)} ETFs para enriquecer.")
        
        count = 0
        for etf in etfs:
            isin = etf.get('isin')
            if not isin or isin == "N/A":
                continue
                
            print(f"[{count+1}/{len(etfs)}] Procesando {isin} - {etf.get('nombreEtf')}...")
            
            details = scrape_justetf_details(isin)
            
            if details:
                collection.update_one({"_id": etf["_id"]}, {"$set": details})
                print(f"   ✅ OK (1y: {details['yield_1y']}, 3y: {details['yield_3y']}, 5y: {details['yield_5y']})")
                print(f"      Riesgo (Vol 3y: {details['volatility_3y']}, MaxDD 3y: {details['max_drawdown_3y']})")
            else:
                print(f"   ⚠️ No se pudieron obtener datos para {isin}")
            
            # Delay aleatorio para evitar baneos (JustETF es sensible)
            time.sleep(random.uniform(3, 7))
            count += 1
                
        client.close()
        
    except ConnectionFailure:
        print("❌ Error: No se pudo conectar a MongoDB.")
    except Exception as e:
        print(f"❌ Error general: {e}")

if __name__ == "__main__":
    main()
