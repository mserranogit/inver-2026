#!/usr/bin/env python3
"""
Programa MINIMALISTA para extraer SOLO datos esenciales de fondos de inversión
usando mstarpy v8.x y guardarlos en MongoDB

CAMPOS MÍNIMOS EXTRAÍDOS:
- Identificación: nombre, ISIN, código
- Rentabilidades: 1M, 3M, 6M, YTD, 1Y, 3Y, 5Y
- Riesgo: volatilidad, Sharpe ratio, rating Morningstar
- Básicos: categoría, divisa, patrimonio, TER
- Histórico: NAV mensual últimos 3 años

Autor: Asistente Claude
Fecha: 2026-02-05 (Versión MINIMAL)
"""

import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import sys

# Librerías necesarias
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
except ImportError:
    print("❌ Error: pymongo no está instalado")
    print("Instálalo con: pip install pymongo")
    sys.exit(1)

try:
    from mstarpy import Funds
except ImportError:
    print("❌ Error: mstarpy no está instalado")
    print("Instálalo con: pip install mstarpy")
    sys.exit(1)

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'admin',
    'password': 'mike',
    'database': 'db-inver',
    'collection': 'fondos',
    'auth_source': 'admin'
}

MSTARPY_CONFIG = {
    'delay_seconds': 2,
    'historical_years': 3
}

PROGRAM_CONFIG = {
    'json_input_path': '../../assets/json/fondos_prueba.json',
    'log_file_path': '../../assets/logs/fondos_extraction_log.txt'
}


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging(log_file: str):
    """Configura el sistema de logging"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)


# =============================================================================
# MONGODB
# =============================================================================

class MongoDBManager:
    """Gestor de conexión y operaciones con MongoDB"""

    def __init__(self, config: Dict):
        self.config = config
        self.client = None
        self.db = None
        self.collection = None
        self.logger = logging.getLogger(__name__)

    def connect(self) -> bool:
        """Establece conexión con MongoDB"""
        try:
            if self.config['username'] and self.config['password']:
                uri = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['auth_source']}"
            else:
                uri = f"mongodb://{self.config['host']}:{self.config['port']}"

            self.logger.info(f"🔌 Conectando a MongoDB...")

            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
            self.client.admin.command('ping')

            self.db = self.client[self.config['database']]
            self.collection = self.db[self.config['collection']]

            self.logger.info(f"✅ Conectado a MongoDB")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error al conectar a MongoDB: {e}")
            return False

    def insert_or_update_fondo(self, fondo_data: Dict) -> bool:
        """Inserta o actualiza un fondo en MongoDB"""
        try:
            isin = fondo_data.get('isin')
            if not isin:
                return False

            fondo_data['ultima_actualizacion'] = datetime.now()

            result = self.collection.update_one(
                {'isin': isin},
                {'$set': fondo_data},
                upsert=True
            )

            if result.upserted_id:
                self.logger.info(f"   ✅ Insertado")
            elif result.modified_count > 0:
                self.logger.info(f"   ✅ Actualizado")
            else:
                self.logger.info(f"   ℹ️  Sin cambios")

            return True

        except Exception as e:
            self.logger.error(f"   ❌ Error al guardar: {e}")
            return False

    def get_fondos_count(self) -> int:
        try:
            return self.collection.count_documents({})
        except:
            return 0

    def close(self):
        if self.client:
            self.client.close()
            self.logger.info("🔌 Conexión cerrada")


# =============================================================================
# EXTRACTOR MINIMALISTA
# =============================================================================

class MstarPyExtractorMinimal:
    """Extractor MINIMALISTA - solo datos esenciales"""

    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def extraer_datos_fondo(self, isin: str, datos_originales: Dict) -> Dict:
        """
        Extrae SOLO los datos esenciales del fondo

        DATOS EXTRAÍDOS:
        - Identificación: nombre, ISIN, código, tipo
        - Básicos: categoría, divisa, patrimonio
        - Rentabilidades: 1M, 3M, 6M, YTD, 1Y, 3Y, 5Y
        - Riesgo: volatilidad, Sharpe ratio, rating
        - Costes: TER
        - Histórico: NAV mensual
        """

        self.logger.info(f"\n{'=' * 80}")
        self.logger.info(f"🔍 {datos_originales.get('nombre', 'N/A')[:50]}")
        self.logger.info(f"   ISIN: {isin}")

        # Estructura mínima de datos
        datos_minimos = {
            # Datos del JSON original (para mantener compatibilidad)
            'isin': isin,
            'nombre': datos_originales.get('nombre'),
            'tipoFondo': datos_originales.get('tipoFondo'),
            'subtipoFondo': datos_originales.get('subtipoFondo'),
            'riesgo': datos_originales.get('riesgo'),

            # Datos de Morningstar (se rellenarán)
            'nombre_completo': None,
            'codigo': None,
            'tipo_activo': None,

            # Básicos
            'categoria': None,
            'divisa': None,
            'patrimonio': None,

            # Rentabilidades
            'rent_1m': None,
            'rent_3m': None,
            'rent_6m': None,
            'rent_ytd': None,
            'rent_1y': None,
            'rent_3y': None,
            'rent_5y': None,

            # Riesgo
            'volatilidad': None,
            'sharpe_ratio': None,
            'rating_estrellas': None,
            'max_caida': None,

            # Costes
            'ter': None,

            # Histórico
            'historico_nav': None,

            # Metadata
            'fecha_extraccion': datetime.now(),
            'extraccion_exitosa': False,
            'error': None
        }

        try:
            self.logger.info(f"   📡 Consultando Morningstar...")
            fondo = Funds(isin)

            # Verificar que existe
            if not hasattr(fondo, 'name') or not fondo.name:
                raise Exception("Fondo no encontrado")

            self.logger.info(f"   ✅ Encontrado")

            # 1. IDENTIFICACIÓN
            datos_minimos['nombre_completo'] = fondo.name if hasattr(fondo, 'name') else None
            datos_minimos['codigo'] = fondo.code if hasattr(fondo, 'code') else None
            datos_minimos['tipo_activo'] = fondo.asset_type if hasattr(fondo, 'asset_type') else None

            # 2. SNAPSHOT (datos básicos)
            try:
                snapshot = fondo.snapshot()
                if snapshot and isinstance(snapshot, dict):
                    datos_minimos['categoria'] = snapshot.get('category')
                    datos_minimos['divisa'] = snapshot.get('currency')
                    datos_minimos['patrimonio'] = snapshot.get('fundSize')
                    self.logger.info(f"   📊 Básicos: OK")
            except Exception as e:
                self.logger.debug(f"      Snapshot error: {e}")

            # 3. RENTABILIDADES (trailingReturn sin 's')
            try:
                returns = fondo.trailingReturn()
                if returns:
                    # Convertir a dict si es necesario
                    if hasattr(returns, 'to_dict'):
                        returns = returns.to_dict()

                    if isinstance(returns, dict):
                        # Mapear los periodos (buscar diferentes posibles nombres)
                        periodos = {
                            'rent_1m': ['1M', 'oneMonth', '1 Month'],
                            'rent_3m': ['3M', 'threeMonth', '3 Month'],
                            'rent_6m': ['6M', 'sixMonth', '6 Month'],
                            'rent_ytd': ['YTD', 'ytd'],
                            'rent_1y': ['1Y', 'oneYear', '1 Year'],
                            'rent_3y': ['3Y', 'threeYear', '3 Year'],
                            'rent_5y': ['5Y', 'fiveYear', '5 Year']
                        }

                        for campo, posibles_keys in periodos.items():
                            for key in posibles_keys:
                                if key in returns:
                                    valor = returns[key]
                                    # Si viene como dict, intentar extraer el valor
                                    if isinstance(valor, dict):
                                        datos_minimos[campo] = valor.get('value', valor.get('return'))
                                    else:
                                        datos_minimos[campo] = valor
                                    break

                        self.logger.info(f"   📈 Rentabilidades: OK")
                    else:
                        self.logger.debug(f"      Returns tipo inesperado: {type(returns)}")
            except Exception as e:
                self.logger.debug(f"      Returns error: {e}")

            # 4. RIESGO (riskReturnSummary)
            try:
                risk = fondo.riskReturnSummary()
                if risk:
                    if hasattr(risk, 'to_dict'):
                        risk = risk.to_dict()

                    if isinstance(risk, dict):
                        datos_minimos['volatilidad'] = risk.get('volatility')
                        datos_minimos['sharpe_ratio'] = risk.get('sharpeRatio')
                        datos_minimos['rating_estrellas'] = risk.get('starRating')
                        datos_minimos['max_caida'] = risk.get('maxDrawDown')
                        self.logger.info(f"   📉 Riesgo: OK")
            except Exception as e:
                self.logger.debug(f"      Risk error: {e}")

            # 5. COSTES (de snapshot o método específico)
            try:
                # Intentar obtener TER del snapshot primero
                if snapshot and isinstance(snapshot, dict):
                    datos_minimos['ter'] = snapshot.get('ongoingCharge') or snapshot.get('ter')

                # Si no está en snapshot, intentar método específico
                if not datos_minimos['ter']:
                    fee_data = fondo.feeLevel() if hasattr(fondo, 'feeLevel') and callable(
                        getattr(fondo, 'feeLevel')) else None
                    if fee_data and isinstance(fee_data, dict):
                        datos_minimos['ter'] = fee_data.get('ongoingCharge')

                if datos_minimos['ter']:
                    self.logger.info(f"   💰 TER: OK")
            except Exception as e:
                self.logger.debug(f"      TER error: {e}")

            # 6. HISTÓRICO NAV (solo último año para reducir datos)
            try:
                fecha_fin = datetime.now()
                fecha_inicio = fecha_fin - timedelta(days=self.config.get('historical_years', 3) * 365)

                historial = fondo.nav(fecha_inicio, fecha_fin, 'monthly')

                if historial and len(historial) > 0:
                    datos_minimos['historico_nav'] = historial
                    self.logger.info(f"   📊 Histórico: {len(historial)} meses")
            except Exception as e:
                self.logger.debug(f"      Histórico error: {e}")

            # Marcar como exitoso
            datos_minimos['extraccion_exitosa'] = True
            self.logger.info(f"   ✅ Extracción completa")

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"   ❌ Error: {error_msg}")
            datos_minimos['error'] = error_msg
            datos_minimos['extraccion_exitosa'] = False

        return datos_minimos


# =============================================================================
# PROGRAMA PRINCIPAL
# =============================================================================

def cargar_fondos_json(ruta: str) -> List[Dict]:
    """Carga fondos desde JSON"""
    logger = logging.getLogger(__name__)
    try:
        logger.info(f"📁 Cargando fondos...")
        with open(ruta, 'r', encoding='utf-8') as f:
            fondos = json.load(f)
        logger.info(f"✅ {len(fondos)} fondos cargados")
        return fondos
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return []


def procesar_fondos(fondos_json: List[Dict], extractor: MstarPyExtractorMinimal,
                    db_manager: MongoDBManager, delay: int = 2) -> Dict[str, int]:
    """Procesa todos los fondos"""
    logger = logging.getLogger(__name__)

    stats = {
        'total': len(fondos_json),
        'exitosos': 0,
        'con_errores': 0,
        'guardados': 0
    }

    logger.info(f"\n{'=' * 80}")
    logger.info(f"🚀 PROCESANDO {stats['total']} FONDOS")
    logger.info(f"{'=' * 80}\n")

    for idx, fondo_json in enumerate(fondos_json, 1):
        isin = fondo_json.get('isin', 'DESCONOCIDO')

        logger.info(f"\n📍 Fondo {idx}/{stats['total']}")

        datos_completos = extractor.extraer_datos_fondo(isin, fondo_json)

        if datos_completos.get('extraccion_exitosa'):
            stats['exitosos'] += 1
        else:
            stats['con_errores'] += 1

        if db_manager.insert_or_update_fondo(datos_completos):
            stats['guardados'] += 1

        if idx < stats['total']:
            time.sleep(delay)

    return stats


def mostrar_resumen(stats: Dict[str, int], db_manager: MongoDBManager):
    """Muestra resumen final"""
    logger = logging.getLogger(__name__)

    logger.info(f"\n{'=' * 80}")
    logger.info(f"📊 RESUMEN")
    logger.info(f"{'=' * 80}")
    logger.info(f"   Total: {stats['total']}")
    logger.info(f"   Exitosos: {stats['exitosos']} ({stats['exitosos'] / stats['total'] * 100:.1f}%)")
    logger.info(f"   Errores: {stats['con_errores']} ({stats['con_errores'] / stats['total'] * 100:.1f}%)")
    logger.info(f"   Guardados: {stats['guardados']}")

    total_db = db_manager.get_fondos_count()
    logger.info(f"\n📚 Total en DB: {total_db} fondos")
    logger.info(f"\n{'=' * 80}")


def main():
    """Función principal"""
    logger = setup_logging(PROGRAM_CONFIG['log_file_path'])

    logger.info(f"{'=' * 80}")
    logger.info(f"🎯 EXTRACTOR MINIMALISTA - Solo Datos Esenciales")
    logger.info(f"{'=' * 80}")
    logger.info(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'=' * 80}\n")

    try:
        db_manager = MongoDBManager(MONGO_CONFIG)
        if not db_manager.connect():
            logger.error("❌ No se pudo conectar a MongoDB")
            return 1

        fondos_json = cargar_fondos_json(PROGRAM_CONFIG['json_input_path'])
        if not fondos_json:
            logger.error("❌ No se pudieron cargar fondos")
            db_manager.close()
            return 1

        extractor = MstarPyExtractorMinimal(MSTARPY_CONFIG)

        stats = procesar_fondos(fondos_json, extractor, db_manager,
                                delay=MSTARPY_CONFIG['delay_seconds'])

        mostrar_resumen(stats, db_manager)
        db_manager.close()

        logger.info(f"\n✅ Proceso completado")
        logger.info(f"📝 Log: {PROGRAM_CONFIG['log_file_path']}")

        return 0

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrumpido")
        return 130
    except Exception as e:
        logger.error(f"\n❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
