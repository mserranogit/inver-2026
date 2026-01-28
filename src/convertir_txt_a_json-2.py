import json
import os


def leer_fondos(ruta_entrada):
    """
    Lee el archivo de fondos y convierte cada registro en un diccionario.
    """
    fondos = []

    with open(ruta_entrada, 'r', encoding='utf-8') as f:
        lineas = f.readlines()

    i = 0
    while i < len(lineas):
        linea = lineas[i].strip()

        # Saltar líneas vacías al inicio
        if not linea:
            i += 1
            continue

        # Saltar delimitadores *** al inicio
        if linea == '***':
            i += 1
            continue

        # Iniciar nuevo registro - primera línea es tipoFondo
        fondo = {}
        fondo['tipoFondo'] = linea
        i += 1

        # Saltar líneas vacías
        while i < len(lineas) and not lineas[i].strip():
            i += 1

        # Si tipoFondo es "Inversión alternativa", no tiene subtipoFondo
        if fondo['tipoFondo'] == 'Inversión alternativa':
            # La siguiente línea es directamente el nombre
            if i < len(lineas):
                fondo['nombre'] = lineas[i].strip()
                i += 1
        else:
            # Línea 2: subtipoFondo
            if i < len(lineas):
                fondo['subtipoFondo'] = lineas[i].strip()
                i += 1

            # Saltar líneas vacías
            while i < len(lineas) and not lineas[i].strip():
                i += 1

            # Línea 3: nombre
            if i < len(lineas):
                fondo['nombre'] = lineas[i].strip()
                i += 1

        # Saltar líneas vacías
        while i < len(lineas) and not lineas[i].strip():
            i += 1

        # Línea 4: isin
        if i < len(lineas):
            fondo['isin'] = lineas[i].strip()
            i += 1

        # Procesar el resto de campos hasta encontrar ***
        while i < len(lineas):
            linea = lineas[i].strip()

            # Saltar líneas vacías
            if not linea:
                i += 1
                continue

            # Fin de registro
            if linea == '***':
                i += 1
                break

            # Procesar campos específicos
            if linea == 'Riesgo':
                i += 1
                while i < len(lineas) and not lineas[i].strip():
                    i += 1
                if i < len(lineas):
                    fondo['riesgo'] = lineas[i].strip()
                    i += 1

            elif linea == 'Rentabilidad YTD':
                i += 1
                while i < len(lineas) and not lineas[i].strip():
                    i += 1
                if i < len(lineas):
                    fondo['ren-ytd'] = lineas[i].strip()
                    i += 1

            elif linea == 'Rentabilidad 2025':
                i += 1
                while i < len(lineas) and not lineas[i].strip():
                    i += 1
                if i < len(lineas):
                    fondo['ren-2025'] = lineas[i].strip()
                    i += 1

            elif linea == 'Comisión de gestión*':
                i += 1
                while i < len(lineas) and not lineas[i].strip():
                    i += 1
                if i < len(lineas):
                    fondo['comision'] = lineas[i].strip()
                    i += 1

            else:
                # Línea desconocida, avanzar
                i += 1

        # Añadir el fondo a la lista si tiene datos mínimos
        if 'nombre' in fondo and 'isin' in fondo:
            fondos.append(fondo)

    return fondos


def main():
    """
    Función principal que ejecuta la conversión.
    """
    # Obtener rutas relativas al script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    limpio = script_dir[:-4]
    assets_path = os.path.join(limpio, "assets")

    ruta_entrada = os.path.join(assets_path, 'txt', 'fondos-riesgo-2.txt')
    ruta_salida = os.path.join(assets_path, 'json', 'fondos-riesgo-2.json')

    # Crear directorios si no existen
    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)

    # Leer y convertir
    print(f"Leyendo archivo: {ruta_entrada}")
    fondos = leer_fondos(ruta_entrada)

    # Guardar en JSON
    with open(ruta_salida, 'w', encoding='utf-8') as f:
        json.dump(fondos, f, ensure_ascii=False, indent=2)

    print(f"Conversión completada: {len(fondos)} fondos procesados")
    print(f"Archivo generado: {ruta_salida}")


if __name__ == "__main__":
    main()