#!/usr/bin/env python3
"""
Script para extraer titulos de ebooks y renombrar archivos - VERSION MEJORADA
Soporta: PDF, EPUB, MOBI, AZW3
"""

import os
from pathlib import Path
from typing import Optional

# Importar bibliotecas
try:
    import fitz  # PyMuPDF

    FITZ_AVAILABLE = True
except ImportError:
    FITZ_AVAILABLE = False
    print("fitz (PyMuPDF) no instalado. Para PDFs: pip install pymupdf")

try:
    import ebooklib
    from ebooklib import epub

    EPUB_AVAILABLE = True
except ImportError:
    EPUB_AVAILABLE = False


def sanitize_filename(filename: str) -> str:
    """Limpia el nombre de archivo de caracteres invalidos"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "")
    if len(filename) > 200:
        filename = filename[:200]
    return filename.strip()


def is_likely_title(line: str) -> bool:
    """Determina si una linea parece ser un titulo de libro"""
    line = line.strip()

    if len(line) < 5 or len(line) > 150:
        return False

    words = line.split()
    if len(words) < 2:
        return False

    # Palabras/frases que indican que NO es un titulo
    skip_patterns = [
        "copyright",
        "published by",
        "all rights reserved",
        "advance praise",
        "table of contents",
        "begin reading",
        "penguin press",
        "by the same author",
        "about the",
        "acknowledgments",
        "introduction",
        "foreword",
        "dedication",
        "first published",
        "by ",
        "thank you",
        "library of congress",
        "isbn",
        "www.",
        ".com",
        "http",
        "printed in",
        "cover design",
        "author photo",
        "for ",
        "to my",
        "in memory of",
        "praise for",
        "hay house",
        "random house",
        "harper",
        "simon",
        "schuster",
        "macmillan",
        "hachette",
        "penguin",
    ]

    line_lower = line.lower()
    for pattern in skip_patterns:
        if pattern in line_lower:
            return False

    # Debe tener al menos una palabra con mayuscula (titulos propios)
    has_capitalized = any(word[0].isupper() for word in words if word)
    if not has_capitalized:
        return False

    # No debe ser principalmente numeros
    alpha_chars = sum(1 for c in line if c.isalpha())
    if alpha_chars < len(line) * 0.4:  # Menos del 40% es texto
        return False

    return True


def extract_pdf_title(filepath: str) -> Optional[str]:
    """Extrae el titulo de un archivo PDF usando PyMuPDF"""
    if not FITZ_AVAILABLE:
        return None

    try:
        doc = fitz.open(filepath)
        text_content = ""

        # Extraer texto de las primeras 5 paginas
        for page_num in range(min(5, len(doc))):
            page = doc[page_num]
            text_content += page.get_text()

        doc.close()

        if text_content:
            lines = [line.strip() for line in text_content.split("\n")]

            # Primera pasada: buscar lineas que parezcan titulos
            candidates = []
            for i, line in enumerate(lines):
                clean_line = "".join(
                    c if c.isprintable() else " " for c in line
                ).strip()
                if is_likely_title(clean_line):
                    # Puntuar basado en posicion y caracteristicas
                    score = 0
                    if i < 20:  # Titulos suelen estar al principio
                        score += 10
                    if clean_line[0].isupper():  # Empieza con mayuscula
                        score += 5
                    if all(w[0].isupper() for w in clean_line.split()[:3] if w):
                        score += 3  # Varias palabras con mayuscula

                    candidates.append((score, clean_line))

            # Ordenar por puntuacion y tomar el mejor
            if candidates:
                candidates.sort(reverse=True)
                return candidates[0][1]

    except Exception as e:
        print(f"  Error leyendo PDF: {e}")

    return None


def extract_epub_title(filepath: str) -> Optional[str]:
    """Extrae el titulo de un archivo EPUB"""
    if not EPUB_AVAILABLE:
        return None

    try:
        book = epub.read_epub(filepath)
        titles = book.get_metadata("DC", "title")
        if titles:
            return titles[0][0]
    except Exception as e:
        print(f"  Error leyendo EPUB: {e}")

    return None


def extract_mobi_title(filepath: str) -> Optional[str]:
    """Extrae el titulo de archivos MOBI/AZW3 leyendo el header del archivo"""
    try:
        with open(filepath, "rb") as f:
            content = f.read()

        # Metodo 1: Buscar en el header PDB (Palm Database)
        # Los archivos MOBI/AZW3 son Palm databases
        if len(content) > 64:
            # El nombre del libro puede estar en el header
            db_name = (
                content[0:32].decode("latin-1", errors="ignore").strip("\x00").strip()
            )
            if len(db_name) > 5 and len(db_name) < 200 and not db_name.isdigit():
                # Limpiar caracteres no validos
                clean_name = "".join(
                    c if c.isprintable() else " " for c in db_name
                ).strip()
                if clean_name and len(clean_name.split()) >= 2:
                    return clean_name

        # Metodo 2: Buscar texto legible en los primeros 50KB
        first_bytes = content[: min(50000, len(content))]

        # Intentar decodificar como UTF-8 o Latin-1
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            try:
                text = first_bytes.decode(encoding, errors="ignore")
                # Buscar lineas que parezcan titulos (entre 20 y 150 caracteres)
                lines = [
                    line.strip()
                    for line in text.split("\n")
                    if 20 < len(line.strip()) < 150
                ]

                for line in lines[:20]:  # Revisar primeras 20 lineas largas
                    # Limpiar caracteres especiales
                    clean_line = "".join(
                        c if c.isprintable() and ord(c) < 127 else " " for c in line
                    )
                    clean_line = clean_line.strip()

                    # Verificar que sea un titulo probable
                    words = clean_line.split()
                    if len(words) >= 2 and len(words) <= 20:
                        # Debe tener palabras con mayusculas (titulos propios)
                        if any(w[0].isupper() for w in words if w):
                            # No debe ser texto de copyright o editorial
                            lower_line = clean_line.lower()
                            skip_words = [
                                "copyright",
                                "published",
                                "all rights",
                                "isbn",
                                "www.",
                                ".com",
                                "kindle",
                                "amazon",
                            ]
                            if not any(sw in lower_line for sw in skip_words):
                                # Debe tener al menos 60% de caracteres alfabeticos
                                alpha_count = sum(1 for c in clean_line if c.isalpha())
                                if alpha_count / len(clean_line) > 0.6:
                                    return clean_line
            except:
                continue

        # Metodo 3: Buscar el marcador EXTH si existe (formato MOBI)
        exth_marker = b"EXTH"
        pos = content.find(exth_marker)
        if pos != -1 and pos + 8 < len(content):
            # Leer el numero de registros
            record_count = int.from_bytes(content[pos + 8 : pos + 12], "big")
            idx = pos + 12

            for _ in range(min(record_count, 50)):  # Maximo 50 registros
                if idx + 8 > len(content):
                    break
                record_type = int.from_bytes(content[idx : idx + 4], "big")
                record_len = int.from_bytes(content[idx + 4 : idx + 8], "big")

                # Tipo 503 es el titulo
                if record_type == 503 and record_len > 8:
                    title_data = content[idx + 8 : idx + record_len]
                    # Limpiar caracteres no imprimibles
                    title_data = bytes(
                        b for b in title_data if 32 <= b <= 126 or b in [10, 13]
                    )
                    try:
                        title = title_data.decode("utf-8").strip()
                        if title and len(title) > 5:
                            return title
                    except:
                        pass

                idx += record_len
                if record_len == 0:
                    break

    except Exception as e:
        print(f"  Error leyendo MOBI/AZW3: {e}")

    return None


def extract_title(filepath: str) -> Optional[str]:
    """Extrae el titulo segun el tipo de archivo"""
    ext = Path(filepath).suffix.lower()

    if ext == ".pdf":
        return extract_pdf_title(filepath)
    elif ext == ".epub":
        return extract_epub_title(filepath)
    elif ext in [".mobi", ".azw3"]:
        return extract_mobi_title(filepath)

    return None


def rename_ebooks(directory: str):
    """Procesa todos los ebooks en un directorio y los renombra"""
    extensions = {".pdf", ".epub", ".mobi", ".azw3"}

    files = []
    for ext in extensions:
        files.extend(Path(directory).glob(f"*{ext}"))

    if not files:
        print(f"No se encontraron ebooks en: {directory}")
        return

    print(f"\nEncontrados {len(files)} ebooks\n")
    print("=" * 80)

    renamed_count = 0
    error_count = 0

    for filepath in files:
        try:
            print(f"\nProcesando: {filepath.name}")

            title = extract_title(str(filepath))

            if title:
                safe_title = sanitize_filename(title)
                new_filename = f"{safe_title}{filepath.suffix}"
                new_filepath = filepath.parent / new_filename

                counter = 1
                while new_filepath.exists() and new_filepath != filepath:
                    new_filename = f"{safe_title}_{counter}{filepath.suffix}"
                    new_filepath = filepath.parent / new_filename
                    counter += 1

                if new_filepath != filepath:
                    filepath.rename(new_filepath)
                    print(f"   Renombrado a: {new_filename}")
                    renamed_count += 1
                else:
                    print(f"   El archivo ya tiene el nombre correcto")
            else:
                print(f"   No se pudo extraer el titulo")
                error_count += 1
        except Exception as e:
            print(f"   Error: {e}")
            error_count += 1

    print("\n" + "=" * 80)
    print(f"\nResumen:")
    print(f"   Renombrados: {renamed_count}")
    print(f"   Sin titulo: {error_count}")
    print(f"   Total: {len(files)}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        directory = sys.argv[1]
    else:
        directory = input("Introduce la ruta del directorio con los ebooks: ").strip()
        if not directory:
            directory = "."

    if not os.path.isdir(directory):
        print(f"Error: El directorio '{directory}' no existe")
        sys.exit(1)

    print(
        f"\nATENCION: Este script renombrara los archivos en: {os.path.abspath(directory)}"
    )
    confirm = input("Deseas continuar? (s/N): ").strip().lower()

    if confirm in ["s", "si", "y", "yes"]:
        rename_ebooks(directory)
    else:
        print("Operacion cancelada")
