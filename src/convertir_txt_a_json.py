import json
import os


def convertir_txt_a_json(archivo_entrada):
    """
    Convierte un archivo de texto con registros de fondos a formato JSON.

    Args:
        archivo_entrada: Ruta del archivo de texto a convertir
    """
    fondos = []

    with open(archivo_entrada, 'r', encoding='utf-8') as f:
        lineas = f.readlines()

    i = 0
    while i < len(lineas):
        linea = lineas[i].strip()

        # Saltar líneas vacías
        if not linea:
            i += 1
            continue

        # Detectar inicio de registro
        if linea in ['Renta Fija', 'Mercado Monetario']:
            fondo = {}
            fondo['tipoFondo'] = linea
            i += 1

            # Si es Renta Fija, la siguiente línea es el subtipo
            if linea == 'Renta Fija':
                fondo['subtipoFondo'] = lineas[i].strip()
                i += 1

            # Nombre del fondo
            fondo['nombre'] = lineas[i].strip()
            i += 1

            # ISIN
            fondo['isin'] = lineas[i].strip()
            i += 1

            # Riesgo
            if lineas[i].strip() == 'Riesgo':
                i += 1
                fondo['riesgo'] = lineas[i].strip()
                i += 1

            # Rentabilidad YTD
            if lineas[i].strip() == 'Rentabilidad YTD':
                i += 1
                fondo['ren-ytd'] = lineas[i].strip()
                i += 1

            # Rentabilidad 2025
            if lineas[i].strip() == 'Rentabilidad 2025':
                i += 1
                fondo['ren-2025'] = lineas[i].strip()
                i += 1

            # Comisión de gestión
            if lineas[i].strip() == 'Comisión de gestión*':
                i += 1
                fondo['comision'] = lineas[i].strip()
                i += 1

            # Agregar el fondo a la lista
            fondos.append(fondo)

            # Saltar el separador ***
            if i < len(lineas) and lineas[i].strip() == '***':
                i += 1
        else:
            i += 1

    # Guardar en archivo JSON en el mismo directorio
    script_dir = os.path.dirname(os.path.abspath(__file__))
    limpio = script_dir[:-4]
    assets_path = os.path.join(limpio, "assets")

    archivo_salida = os.path.join(assets_path, 'json', 'fondos_open_R2.json')
    with open(archivo_salida, 'w', encoding='utf-8') as f:
        json.dump(fondos, f, ensure_ascii=False, indent=2)

    print(f"✓ Conversión completada: {len(fondos)} fondos procesados")
    print(f"✓ Archivo generado: {archivo_salida}")


# Ejecutar el programa
if __name__ == "__main__":
    # Ruta del archivo de entrada
    script_dir = os.path.dirname(os.path.abspath(__file__))
    limpio = script_dir[:-4]
    assets_path = os.path.join(limpio, "assets")
    archivo_entrada = os.path.join(assets_path, 'txt', 'fondos-riesgo-2.txt')


    # Verificar que existe el archivo
    if not os.path.exists(archivo_entrada):
        print(f"✗ Error: No se encuentra el archivo '{archivo_entrada}'")
        print(f"  Asegúrate de que existe el directorio 'assets/json' y el archivo dentro.")
    else:
        convertir_txt_a_json(archivo_entrada)