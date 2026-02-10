#!/usr/bin/env python3
"""
Script de utilidades para verificar y consultar la base de datos MongoDB
de fondos de inversión
"""

import sys
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import json

# Configuración MongoDB (debe coincidir con el script principal)
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'admin',
    'password': 'mike',
    'database': 'db-inver',
    'collection': 'fondos',
    'auth_source': 'admin'
}

def conectar_mongodb():
    """Conecta a MongoDB y retorna la colección"""
    try:
        # Construir URI
        uri = f"mongodb://{MONGO_CONFIG['username']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/{MONGO_CONFIG['auth_source']}"
        
        # Conectar
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        
        db = client[MONGO_CONFIG['database']]
        collection = db[MONGO_CONFIG['collection']]
        
        print(f"✅ Conectado a MongoDB exitosamente")
        return client, collection
        
    except ConnectionFailure as e:
        print(f"❌ Error de conexión a MongoDB: {e}")
        return None, None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, None

def mostrar_estadisticas(collection):
    """Muestra estadísticas de la base de datos"""
    print("\n" + "="*80)
    print("📊 ESTADÍSTICAS DE LA BASE DE DATOS")
    print("="*80)
    
    # Total de fondos
    total = collection.count_documents({})
    print(f"\n📚 Total de fondos: {total}")
    
    # Fondos con datos de mstarpy exitosos
    exitosos = collection.count_documents({'mstarpy_datos_extraidos': True})
    print(f"✅ Fondos con datos mstarpy exitosos: {exitosos} ({exitosos/total*100:.1f}%)")
    
    # Fondos con errores
    con_errores = collection.count_documents({'mstarpy_datos_extraidos': False})
    print(f"❌ Fondos con errores en mstarpy: {con_errores} ({con_errores/total*100:.1f}%)")
    
    # Fondos por tipo
    print(f"\n📋 Distribución por tipo de fondo:")
    tipos = collection.aggregate([
        {'$group': {'_id': '$tipoFondo', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ])
    for tipo in tipos:
        print(f"   • {tipo['_id']}: {tipo['count']} fondos")
    
    # Fondos con rating Morningstar
    con_rating = collection.count_documents({'rating_estrellas': {'$ne': None}})
    print(f"\n⭐ Fondos con rating Morningstar: {con_rating}")
    
    # Estadísticas de rentabilidad
    print(f"\n📈 Estadísticas de rentabilidad 2025:")
    pipeline = [
        {'$match': {'ren-2025': {'$exists': True}}},
        {'$addFields': {
            'rent_num': {
                '$toDouble': {
                    '$replaceAll': {
                        'input': {'$replaceAll': {'input': '$ren-2025', 'find': '%', 'replacement': ''}},
                        'find': ',',
                        'replacement': '.'
                    }
                }
            }
        }},
        {'$group': {
            '_id': None,
            'promedio': {'$avg': '$rent_num'},
            'max': {'$max': '$rent_num'},
            'min': {'$min': '$rent_num'}
        }}
    ]
    
    try:
        stats = list(collection.aggregate(pipeline))
        if stats:
            print(f"   • Promedio: {stats[0]['promedio']:.2f}%")
            print(f"   • Máxima: {stats[0]['max']:.2f}%")
            print(f"   • Mínima: {stats[0]['min']:.2f}%")
    except:
        print("   (No se pudieron calcular estadísticas)")

def listar_fondos_exitosos(collection, limite=10):
    """Lista los fondos con extracción exitosa"""
    print("\n" + "="*80)
    print(f"✅ FONDOS CON EXTRACCIÓN EXITOSA (primeros {limite})")
    print("="*80)
    
    fondos = collection.find(
        {'mstarpy_datos_extraidos': True}
    ).limit(limite)
    
    for idx, fondo in enumerate(fondos, 1):
        print(f"\n{idx}. {fondo.get('nombre', 'N/A')}")
        print(f"   ISIN: {fondo.get('isin', 'N/A')}")
        print(f"   Rating: {fondo.get('rating_estrellas', 'N/A')} ⭐")
        print(f"   Rentabilidad 3y: {fondo.get('rentabilidad_3y', 'N/A')}")
        print(f"   Sharpe Ratio 3y: {fondo.get('sharpe_ratio_3y', 'N/A')}")
        print(f"   Volatilidad 3y: {fondo.get('volatilidad_3y', 'N/A')}")

def listar_fondos_con_errores(collection):
    """Lista los fondos que tuvieron errores"""
    print("\n" + "="*80)
    print("❌ FONDOS CON ERRORES EN EXTRACCIÓN")
    print("="*80)
    
    fondos = collection.find({'mstarpy_datos_extraidos': False})
    
    count = 0
    for fondo in fondos:
        count += 1
        print(f"\n{count}. {fondo.get('nombre', 'N/A')}")
        print(f"   ISIN: {fondo.get('isin', 'N/A')}")
        print(f"   Error: {fondo.get('mstarpy_error', 'Desconocido')}")

def buscar_fondo_por_isin(collection, isin):
    """Busca y muestra información detallada de un fondo por ISIN"""
    print("\n" + "="*80)
    print(f"🔍 BÚSQUEDA DE FONDO: {isin}")
    print("="*80)
    
    fondo = collection.find_one({'isin': isin})
    
    if not fondo:
        print(f"\n❌ No se encontró ningún fondo con ISIN: {isin}")
        return
    
    print(f"\n✅ Fondo encontrado:")
    print(f"\n📌 DATOS BÁSICOS:")
    print(f"   Nombre: {fondo.get('nombre', 'N/A')}")
    print(f"   ISIN: {fondo.get('isin', 'N/A')}")
    print(f"   Tipo: {fondo.get('tipoFondo', 'N/A')}")
    print(f"   Subtipo: {fondo.get('subtipoFondo', 'N/A')}")
    
    print(f"\n⭐ RATINGS:")
    print(f"   Rating Morningstar: {fondo.get('rating_estrellas', 'N/A')} estrellas")
    print(f"   Analyst Rating: {fondo.get('analyst_rating', 'N/A')}")
    print(f"   Sustainability: {fondo.get('sustainability_rating', 'N/A')}")
    
    print(f"\n📈 RENTABILIDADES:")
    print(f"   YTD: {fondo.get('rentabilidad_ytd', 'N/A')}")
    print(f"   1 año: {fondo.get('rentabilidad_1y', 'N/A')}")
    print(f"   3 años: {fondo.get('rentabilidad_3y', 'N/A')}")
    print(f"   5 años: {fondo.get('rentabilidad_5y', 'N/A')}")
    
    print(f"\n📉 MÉTRICAS DE RIESGO:")
    print(f"   Nivel riesgo: {fondo.get('riesgo', 'N/A')}")
    print(f"   Volatilidad 3y: {fondo.get('volatilidad_3y', 'N/A')}")
    print(f"   Sharpe Ratio 3y: {fondo.get('sharpe_ratio_3y', 'N/A')}")
    print(f"   Max Drawdown 3y: {fondo.get('max_drawdown_3y', 'N/A')}")
    
    print(f"\n💰 COSTES:")
    print(f"   Comisión (JSON): {fondo.get('comision', 'N/A')}")
    print(f"   TER: {fondo.get('ter', 'N/A')}")
    print(f"   Comisión entrada: {fondo.get('comision_entrada', 'N/A')}")
    print(f"   Comisión salida: {fondo.get('comision_salida', 'N/A')}")
    
    print(f"\n👥 GESTIÓN:")
    print(f"   Gestor: {fondo.get('gestor', 'N/A')}")
    print(f"   Familia: {fondo.get('familia_fondos', 'N/A')}")
    print(f"   Fecha inicio: {fondo.get('fecha_inicio', 'N/A')}")
    print(f"   AUM: {fondo.get('aum', 'N/A')}")
    
    print(f"\n🔧 ESTADO EXTRACCIÓN:")
    print(f"   Datos mstarpy: {'✅ Exitoso' if fondo.get('mstarpy_datos_extraidos') else '❌ Con errores'}")
    if not fondo.get('mstarpy_datos_extraidos'):
        print(f"   Error: {fondo.get('mstarpy_error', 'Desconocido')}")
    print(f"   Fecha extracción: {fondo.get('fecha_extraccion', 'N/A')}")
    print(f"   Última actualización: {fondo.get('ultima_actualizacion', 'N/A')}")

def exportar_fondos_criterios(collection, output_file='/mnt/user-data/outputs/fondos_criterios.json'):
    """
    Exporta fondos que cumplen criterios: rentabilidad 3-4% y riesgo 1-2/7
    """
    print("\n" + "="*80)
    print("📤 EXPORTANDO FONDOS QUE CUMPLEN CRITERIOS")
    print("="*80)
    
    # Buscar fondos con datos exitosos
    query = {
        'mstarpy_datos_extraidos': True,
        'rentabilidad_3y': {'$gte': 3.0, '$lte': 4.0}
    }
    
    fondos = list(collection.find(query))
    
    print(f"\n✅ Encontrados {len(fondos)} fondos que cumplen criterios")
    
    if fondos:
        # Convertir ObjectId a string para JSON
        for fondo in fondos:
            fondo['_id'] = str(fondo['_id'])
            # Convertir datetime a string
            if 'fecha_extraccion' in fondo:
                fondo['fecha_extraccion'] = str(fondo['fecha_extraccion'])
            if 'ultima_actualizacion' in fondo:
                fondo['ultima_actualizacion'] = str(fondo['ultima_actualizacion'])
        
        # Guardar
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(fondos, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Fondos exportados a: {output_file}")
    else:
        print("ℹ️  No hay fondos para exportar")

def menu_principal():
    """Muestra el menú principal"""
    print("\n" + "="*80)
    print("🔧 UTILIDADES MONGODB - FONDOS DE INVERSIÓN")
    print("="*80)
    print("\nOpciones:")
    print("1. Mostrar estadísticas generales")
    print("2. Listar fondos con extracción exitosa")
    print("3. Listar fondos con errores")
    print("4. Buscar fondo por ISIN")
    print("5. Exportar fondos que cumplen criterios (3-4% rentabilidad)")
    print("6. Salir")
    print("\n" + "="*80)

def main():
    """Función principal"""
    
    # Conectar a MongoDB
    client, collection = conectar_mongodb()
    if not collection:
        return 1
    
    try:
        while True:
            menu_principal()
            opcion = input("\nSelecciona una opción (1-6): ").strip()
            
            if opcion == '1':
                mostrar_estadisticas(collection)
            elif opcion == '2':
                limite = input("\n¿Cuántos fondos quieres ver? (default: 10): ").strip()
                limite = int(limite) if limite.isdigit() else 10
                listar_fondos_exitosos(collection, limite)
            elif opcion == '3':
                listar_fondos_con_errores(collection)
            elif opcion == '4':
                isin = input("\nIntroduce el ISIN del fondo: ").strip().upper()
                buscar_fondo_por_isin(collection, isin)
            elif opcion == '5':
                exportar_fondos_criterios(collection)
            elif opcion == '6':
                print("\n👋 ¡Hasta luego!")
                break
            else:
                print("\n❌ Opción inválida")
            
            input("\n[Presiona ENTER para continuar]")
    
    finally:
        if client:
            client.close()
            print("\n🔌 Conexión cerrada")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
