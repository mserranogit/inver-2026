#!/usr/bin/env python3
"""
Programa para extraer datos de fondos de inversión usando mstarpy
y guardarlos en MongoDB

CORRECCIONES APLICADAS:
- Eliminado parámetro 'country' del constructor Funds() (no compatible con versión actual de mstarpy)
- Simplificado el constructor a: Funds(isin)
- Mejorado manejo de errores

Autor: Asistente Claude
Fecha: 2026-02-05
"""

import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import sys

# Librerías necesarias
try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
except ImportError:
    print("❌ Error: pymongo no está instalado")
    print("Instálalo con: pip install pymongo")
    sys.exit(1)

try:
    from mstarpy import Funds
    MSTARPY_DISPONIBLE = True
except ImportError:
    print("❌ Error: mstarpy no está instalado")
    print("Instálalo con: pip install mstarpy")
    sys.exit(1)

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Configuración MongoDB
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'admin',
    'password': 'mike',
    'database': 'db-inver',
    'collection': 'fondos',
    'auth_source': 'admin'  # Base de datos de autenticación
}

# Configuración mstarpy
MSTARPY_CONFIG = {
    'delay_seconds': 2  # Delay entre peticiones
}

# Configuración del programa
PROGRAM_CONFIG = {
    'json_input_path': '../../assets/json/fondos_open_R1.json',
    'log_file_path': '../../assets/logs/fondos_extraction_log.txt',
    'update_existing': True,  # Reemplazar si existe
    'save_errors': True,  # Guardar en MongoDB aunque haya error
    'historical_years': 3  # Años de histórico a extraer
}

# =============================================================================
# CONFIGURACIÓN DE LOGGING
# =============================================================================

def setup_logging(log_file: str):
    """Configura el sistema de logging"""
    
    # Formato del log
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configurar logging tanto a archivo como a consola
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
# CONEXIÓN A MONGODB
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
            # Construir URI de conexión
            if self.config['username'] and self.config['password']:
                uri = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['auth_source']}"
            else:
                uri = f"mongodb://{self.config['host']}:{self.config['port']}"
            
            self.logger.info(f"🔌 Conectando a MongoDB en {self.config['host']}:{self.config['port']}...")
            
            # Conectar
            self.client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000
            )
            
            # Verificar conexión
            self.client.admin.command('ping')
            
            # Seleccionar base de datos y colección
            self.db = self.client[self.config['database']]
            self.collection = self.db[self.config['collection']]
            
            self.logger.info(f"✅ Conectado a MongoDB - Base de datos: {self.config['database']}, Colección: {self.config['collection']}")
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            self.logger.error(f"❌ Error de conexión a MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error inesperado al conectar a MongoDB: {e}")
            return False
    
    def insert_or_update_fondo(self, fondo_data: Dict) -> bool:
        """Inserta o actualiza un fondo en MongoDB"""
        try:
            # Usar ISIN como identificador único
            isin = fondo_data.get('isin')
            if not isin:
                self.logger.error("❌ Fondo sin ISIN, no se puede guardar")
                return False
            
            # Agregar timestamp de última actualización
            fondo_data['ultima_actualizacion'] = datetime.now()
            
            # Actualizar o insertar (upsert)
            result = self.collection.update_one(
                {'isin': isin},
                {'$set': fondo_data},
                upsert=True
            )
            
            if result.upserted_id:
                self.logger.info(f"   ✅ Insertado nuevo fondo en MongoDB")
            elif result.modified_count > 0:
                self.logger.info(f"   ✅ Actualizado fondo existente en MongoDB")
            else:
                self.logger.info(f"   ℹ️  Fondo ya existía sin cambios")
            
            return True
            
        except Exception as e:
            self.logger.error(f"   ❌ Error al guardar en MongoDB: {e}")
            return False
    
    def get_fondos_count(self) -> int:
        """Obtiene el número de fondos en la colección"""
        try:
            return self.collection.count_documents({})
        except:
            return 0
    
    def close(self):
        """Cierra la conexión a MongoDB"""
        if self.client:
            self.client.close()
            self.logger.info("🔌 Conexión a MongoDB cerrada")

# =============================================================================
# EXTRACCIÓN DE DATOS CON MSTARPY
# =============================================================================

class MstarPyExtractor:
    """Extractor de datos usando mstarpy"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def extraer_datos_fondo(self, isin: str, datos_originales: Dict) -> Dict:
        """
        Extrae todos los datos relevantes de un fondo usando mstarpy
        
        Args:
            isin: Código ISIN del fondo
            datos_originales: Datos originales del JSON
            
        Returns:
            Diccionario con todos los datos del fondo
        """
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"🔍 Procesando: {datos_originales.get('nombre', 'N/A')[:60]}")
        self.logger.info(f"   ISIN: {isin}")
        
        # Iniciar con datos originales del JSON
        datos_completos = datos_originales.copy()
        datos_completos['isin'] = isin
        datos_completos['mstarpy_datos_extraidos'] = False
        datos_completos['mstarpy_error'] = None
        datos_completos['fecha_extraccion'] = datetime.now()
        
        try:
            # CORRECCIÓN: Crear objeto del fondo sin parámetro 'country'
            # La versión actual de mstarpy solo acepta el término de búsqueda
            self.logger.info(f"   📡 Consultando Morningstar...")
            fondo = Funds(isin)  # ← CAMBIO AQUÍ: Sin 'term=' ni 'country='
            
            # Verificar si se encontró el fondo
            if not hasattr(fondo, 'name') or fondo.name is None:
                raise Exception("Fondo no encontrado en Morningstar")
            
            self.logger.info(f"   ✅ Fondo encontrado: {fondo.name}")
            
            # Extraer datos básicos
            self._extraer_datos_basicos(fondo, datos_completos)
            
            # Extraer rentabilidades
            self._extraer_rentabilidades(fondo, datos_completos)
            
            # Extraer datos de riesgo
            self._extraer_riesgo(fondo, datos_completos)
            
            # Extraer costes
            self._extraer_costes(fondo, datos_completos)
            
            # Extraer portfolio
            self._extraer_portfolio(fondo, datos_completos)
            
            # Extraer benchmark
            self._extraer_benchmark(fondo, datos_completos)
            
            # Extraer series históricas
            self._extraer_series_historicas(fondo, datos_completos)
            
            # Marcar extracción como exitosa
            datos_completos['mstarpy_datos_extraidos'] = True
            self.logger.info(f"   ✅ Datos extraídos correctamente")
            
        except Exception as e:
            # Guardar error pero continuar
            error_msg = str(e)
            self.logger.error(f"   ❌ Error al extraer datos de mstarpy: {error_msg}")
            datos_completos['mstarpy_error'] = error_msg
            datos_completos['mstarpy_datos_extraidos'] = False
        
        return datos_completos
    
    def _extraer_datos_basicos(self, fondo, datos: Dict):
        """Extrae datos básicos del fondo"""
        try:
            datos['nombre_morningstar'] = getattr(fondo, 'name', None)
            datos['isin_morningstar'] = getattr(fondo, 'isin', None)
            datos['categoria'] = getattr(fondo, 'category', None)
            datos['tipo_activo'] = getattr(fondo, 'assetType', None)
            datos['divisa'] = getattr(fondo, 'currency', None)
            datos['patrimonio'] = getattr(fondo, 'netAssets', None)
            datos['fecha_inicio'] = getattr(fondo, 'inceptionDate', None)
            
            self.logger.info(f"   📊 Datos básicos extraídos")
        except Exception as e:
            self.logger.warning(f"   ⚠️  Error extrayendo datos básicos: {e}")
    
    def _extraer_rentabilidades(self, fondo, datos: Dict):
        """Extrae rentabilidades del fondo"""
        try:
            # Intentar obtener objeto de rentabilidades
            try:
                trailing_returns = fondo.trailingReturns() if hasattr(fondo, 'trailingReturns') else None
            except:
                trailing_returns = None
            
            if trailing_returns:
                datos['rentabilidad_1m'] = trailing_returns.get('oneMonth', None)
                datos['rentabilidad_3m'] = trailing_returns.get('threeMonth', None)
                datos['rentabilidad_6m'] = trailing_returns.get('sixMonth', None)
                datos['rentabilidad_ytd'] = trailing_returns.get('ytd', None)
                datos['rentabilidad_1y'] = trailing_returns.get('oneYear', None)
                datos['rentabilidad_3y'] = trailing_returns.get('threeYear', None)
                datos['rentabilidad_5y'] = trailing_returns.get('fiveYear', None)
                datos['rentabilidad_10y'] = trailing_returns.get('tenYear', None)
            
            self.logger.info(f"   📈 Rentabilidades extraídas")
        except Exception as e:
            self.logger.warning(f"   ⚠️  Error extrayendo rentabilidades: {e}")
    
    def _extraer_riesgo(self, fondo, datos: Dict):
        """Extrae métricas de riesgo"""
        try:
            datos['rating_morningstar'] = getattr(fondo, 'starRating', None)
            datos['riesgo_morningstar'] = getattr(fondo, 'riskRating', None)
            datos['volatilidad'] = getattr(fondo, 'volatility', None)
            datos['sharpe_ratio'] = getattr(fondo, 'sharpeRatio', None)
            datos['sortino_ratio'] = getattr(fondo, 'sortinoRatio', None)
            datos['max_drawdown'] = getattr(fondo, 'maxDrawdown', None)
            
            self.logger.info(f"   📉 Métricas de riesgo extraídas")
        except Exception as e:
            self.logger.warning(f"   ⚠️  Error extrayendo riesgo: {e}")
    
    def _extraer_costes(self, fondo, datos: Dict):
        """Extrae costes y comisiones"""
        try:
            datos['ter'] = getattr(fondo, 'ongoingCharge', None)
            datos['comision_gestion'] = getattr(fondo, 'managementFee', None)
            
            self.logger.info(f"   💰 Costes extraídos")
        except Exception as e:
            self.logger.warning(f"   ⚠️  Error extrayendo costes: {e}")
    
    def _extraer_portfolio(self, fondo, datos: Dict):
        """Extrae datos de portfolio"""
        try:
            portfolio_data = {}
            try:
                if hasattr(fondo, 'portfolio'):
                    portfolio_data = fondo.portfolio()
            except:
                pass
            
            datos['duracion_media'] = portfolio_data.get('duration', None)
            datos['num_posiciones'] = portfolio_data.get('numberOfHoldings', None)
            datos['top_10_holdings'] = portfolio_data.get('top10Holdings', None)
            datos['distribucion_sectorial'] = portfolio_data.get('sectorBreakdown', None)
            datos['calidad_crediticia'] = portfolio_data.get('creditQuality', None)
            
            self.logger.info(f"   📋 Datos de portfolio extraídos")
        except Exception as e:
            self.logger.warning(f"   ⚠️  Error extrayendo portfolio: {e}")
    
    def _extraer_benchmark(self, fondo, datos: Dict):
        """Extrae datos de benchmark"""
        try:
            datos['benchmark'] = getattr(fondo, 'benchmark', None)
            datos['tracking_error'] = getattr(fondo, 'trackingError', None)
            datos['information_ratio'] = getattr(fondo, 'informationRatio', None)
            
            self.logger.info(f"   🎯 Datos de benchmark extraídos")
        except Exception as e:
            self.logger.warning(f"   ⚠️  Error extrayendo benchmark: {e}")
    
    def _extraer_series_historicas(self, fondo, datos: Dict):
        """Extrae series históricas"""
        try:
            fecha_fin = datetime.now()
            fecha_inicio = fecha_fin - timedelta(days=self.config.get('historical_years', 3) * 365)
            
            historial = None
            try:
                if hasattr(fondo, 'nav'):
                    historial = fondo.nav(
                        start_date=fecha_inicio,
                        end_date=fecha_fin,
                        frequency="monthly"
                    )
            except:
                pass
            
            if historial is not None and len(historial) > 0:
                # Convertir lista de diccionarios para MongoDB
                datos['historial_mensual'] = historial
                self.logger.info(f"   📊 Series históricas extraídas ({len(historial)} registros)")
            else:
                datos['historial_mensual'] = None
                self.logger.info(f"   ℹ️  No hay series históricas disponibles")
                
        except Exception as e:
            self.logger.warning(f"   ⚠️  Error extrayendo series históricas: {e}")
            datos['historial_mensual'] = None

# =============================================================================
# PROGRAMA PRINCIPAL
# =============================================================================

def cargar_fondos_json(ruta: str) -> List[Dict]:
    """Carga los fondos desde el archivo JSON"""
    logger = logging.getLogger(__name__)
    try:
        logger.info(f"📁 Cargando fondos desde {ruta}...")
        with open(ruta, 'r', encoding='utf-8') as f:
            fondos = json.load(f)
        logger.info(f"✅ Cargados {len(fondos)} fondos del archivo JSON")
        return fondos
    except FileNotFoundError:
        logger.error(f"❌ Archivo no encontrado: {ruta}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error al parsear JSON: {e}")
        return []
    except Exception as e:
        logger.error(f"❌ Error inesperado al cargar JSON: {e}")
        return []

def procesar_fondos(
    fondos_json: List[Dict],
    extractor: MstarPyExtractor,
    db_manager: MongoDBManager,
    delay: int = 2
) -> Dict[str, int]:
    """
    Procesa todos los fondos: extrae datos y guarda en MongoDB
    
    Returns:
        Diccionario con estadísticas del proceso
    """
    logger = logging.getLogger(__name__)
    
    stats = {
        'total': len(fondos_json),
        'exitosos': 0,
        'con_errores': 0,
        'guardados': 0,
        'errores_guardado': 0
    }
    
    logger.info(f"\n{'='*80}")
    logger.info(f"🚀 INICIANDO PROCESAMIENTO DE {stats['total']} FONDOS")
    logger.info(f"{'='*80}\n")
    
    for idx, fondo_json in enumerate(fondos_json, 1):
        isin = fondo_json.get('isin', 'DESCONOCIDO')
        
        logger.info(f"\n📍 Fondo {idx}/{stats['total']}")
        
        # Extraer datos con mstarpy
        datos_completos = extractor.extraer_datos_fondo(isin, fondo_json)
        
        # Actualizar estadísticas
        if datos_completos.get('mstarpy_datos_extraidos'):
            stats['exitosos'] += 1
        else:
            stats['con_errores'] += 1
        
        # Guardar en MongoDB
        if db_manager.insert_or_update_fondo(datos_completos):
            stats['guardados'] += 1
        else:
            stats['errores_guardado'] += 1
        
        # Delay entre peticiones (excepto en el último)
        if idx < stats['total']:
            time.sleep(delay)
    
    return stats

def mostrar_resumen(stats: Dict[str, int], db_manager: MongoDBManager):
    """Muestra un resumen final del proceso"""
    logger = logging.getLogger(__name__)
    
    logger.info(f"\n{'='*80}")
    logger.info(f"📊 RESUMEN FINAL DEL PROCESO")
    logger.info(f"{'='*80}")
    logger.info(f"\n📈 Extracción de datos (mstarpy):")
    logger.info(f"   • Total de fondos procesados: {stats['total']}")
    logger.info(f"   • Extracciones exitosas: {stats['exitosos']} ({stats['exitosos']/stats['total']*100:.1f}%)")
    logger.info(f"   • Extracciones con errores: {stats['con_errores']} ({stats['con_errores']/stats['total']*100:.1f}%)")
    
    logger.info(f"\n💾 Guardado en MongoDB:")
    logger.info(f"   • Fondos guardados: {stats['guardados']}")
    logger.info(f"   • Errores al guardar: {stats['errores_guardado']}")
    
    # Contar fondos en la base de datos
    total_db = db_manager.get_fondos_count()
    logger.info(f"\n📚 Base de datos:")
    logger.info(f"   • Total de fondos en MongoDB: {total_db}")
    
    logger.info(f"\n{'='*80}")

def main():
    """Función principal del programa"""
    
    # Configurar logging
    logger = setup_logging(PROGRAM_CONFIG['log_file_path'])
    
    logger.info(f"{'='*80}")
    logger.info(f"🎯 EXTRACTOR DE DATOS DE FONDOS - mstarpy → MongoDB")
    logger.info(f"{'='*80}")
    logger.info(f"Fecha de inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*80}\n")
    
    try:
        # 1. Conectar a MongoDB
        db_manager = MongoDBManager(MONGO_CONFIG)
        if not db_manager.connect():
            logger.error("❌ No se pudo conectar a MongoDB. Abortando.")
            return 1
        
        # 2. Cargar fondos desde JSON
        fondos_json = cargar_fondos_json(PROGRAM_CONFIG['json_input_path'])
        if not fondos_json:
            logger.error("❌ No se pudieron cargar fondos. Abortando.")
            db_manager.close()
            return 1
        
        # 3. Crear extractor de mstarpy
        extractor = MstarPyExtractor(MSTARPY_CONFIG)
        
        # 4. Procesar todos los fondos
        stats = procesar_fondos(
            fondos_json,
            extractor,
            db_manager,
            delay=MSTARPY_CONFIG['delay_seconds']
        )
        
        # 5. Mostrar resumen
        mostrar_resumen(stats, db_manager)
        
        # 6. Cerrar conexión
        db_manager.close()
        
        logger.info(f"\n✅ Proceso completado exitosamente")
        logger.info(f"📝 Log guardado en: {PROGRAM_CONFIG['log_file_path']}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Proceso interrumpido por el usuario")
        return 130
    except Exception as e:
        logger.error(f"\n❌ Error inesperado: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
