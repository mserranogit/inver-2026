import json
import os

def conver_etf_json(filename_base='etf_open_R1'):
    input_path = os.path.join('assets', 'txt', f'{filename_base}.txt')
    output_path = os.path.join('assets', 'json', f'{filename_base}.json')
    
    if not os.path.exists(input_path):
        print(f"Error: El archivo de entrada {input_path} no existe.")
        return

    # Crear directorio de salida si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    records = []
    
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        # Leemos todas las líneas y limpiamos espacios básicos, pero mantenemos la estructura
        lines = [line.strip() for line in f]

    current_record_lines = []
    for line in lines:
        if line == '***':
            if current_record_lines:
                # Procesar el registro acumulado
                record = process_record(current_record_lines)
                if record:
                    records.append(record)
                current_record_lines = []
        else:
            # Solo añadir si no es una línea vacía al inicio del bloque
            if line or current_record_lines:
                current_record_lines.append(line)
    
    # Procesar el último registro si no termina en ***
    if current_record_lines:
        record = process_record(current_record_lines)
        if record:
            records.append(record)

    # Guardar en JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=4)
    
    print(f"Conversión completada para {filename_base}. Se han procesado {len(records)} registros.")
    print(f"Archivo guardado en: {output_path}")

def process_record(lines):
    # Limpiar posibles líneas vacías al principio
    while lines and not lines[0]:
        lines.pop(0)
    
    if not lines:
        return None

    data = {}
    
    tipo_etf = lines[0]
    data['tipoEtf'] = tipo_etf
    
    idx = 1
    if tipo_etf == 'Mercado Monetario':
        # Caso Mercado Monetario
        if idx < len(lines):
            data['nombreEtf'] = lines[idx]
            idx += 1
        
        if idx < len(lines):
            isin_line = lines[idx]
            if '|' in isin_line:
                data['isin'] = isin_line.split('|')[1].strip()
            else:
                data['isin'] = isin_line
            idx += 1
            
        data['subtipoEtf'] = None
    else:
        # Otros casos (Renta Fija, etc)
        if idx < len(lines):
            data['subtipoEtf'] = lines[idx]
            idx += 1
        
        if idx < len(lines):
            data['nombreEtf'] = lines[idx]
            idx += 1
            
        if idx < len(lines):
            isin_line = lines[idx]
            if '|' in isin_line:
                data['isin'] = isin_line.split('|')[1].strip()
            else:
                data['isin'] = isin_line
            idx += 1

    # Buscar campo Riesgo
    data['riesgo'] = None
    # Buscamos 'Riesgo' en las líneas restantes
    while idx < len(lines):
        if lines[idx] == 'Riesgo':
            idx += 1
            if idx < len(lines):
                data['riesgo'] = lines[idx]
            break
        idx += 1
            
    return data

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Si se pasan argumentos (ej: etf_open_R2)
        for arg in sys.argv[1:]:
            conver_etf_json(arg)
    else:
        # Por defecto R1 y R2
        conver_etf_json('etf_open_R1')
        conver_etf_json('etf_open_R2')
