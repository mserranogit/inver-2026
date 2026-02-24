#!/usr/bin/env python3
"""
Script para extraer titulos de ebooks y renombrar archivos
Soporta: PDF, EPUB, MOBI, AZW3
"""

import os
import re
import sys
from pathlib import Path
from typing import Optional

# Intentar importar bibliotecas necesarias
try:
    import PyPDF2
except ImportError:
    PyPDF2 = None
    print("PyPDF2 no instalado. Para PDFs: pip install PyPDF2")

try:
    import ebooklib
    from ebooklib import epub
except ImportError:
    epub = None
    print("ebooklib no instalado. Para EPUBs: pip install EbookLib")

try:
    import mobi

    MOBI_AVAILABLE = True
except ImportError:
    mobi = None
    MOBI_AVAILABLE = False
    print("mobi no instalado. Para MOBI/AZW3: pip install mobi")


def sanitize_filename(filename: str) -> str:
    """Limpia el nombre de archivo de caracteres invalidos"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "")
    if len(filename) > 200:
        filename = filename[:200]
    return filename.strip()


def extract_pdf_title(filepath: str) -> Optional[str]:
    """Extrae el titulo de un archivo PDF usando PyMuPDF para mejor extraccion"""
    try:
        import fitz  # PyMuPDF

        doc = fitz.open(filepath)

        # Extraer texto de las primeras 3 paginas
        text_content = ""
        for page_num in range(min(3, len(doc))):
            page = doc[page_num]
            text_content += page.get_text()

        doc.close()

        if text_content:
            # Buscar lineas que parezcan titulos
            lines = [
                line.strip()
                for line in text_content.split("\n")
                if len(line.strip()) > 3 and len(line.strip()) < 200
            ]

            # Filtrar lineas que parecen titulos reales
            for line in lines:
                # Limpiar caracteres especiales
                clean_line = "".join(c if c.isprintable() else " " for c in line)
                clean_line = clean_line.strip()

                # Ignorar lineas comunes que no son titulos
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
                ]

                if any(pattern in clean_line.lower() for pattern in skip_patterns):
                    continue

                # Titulo debe tener al menos 2 palabras y ser razonable
                words = clean_line.split()
                if len(words) >= 2 and len(clean_line) > 5:
                    # Verificar que no sea solo el ISBN
                    if not clean_line.replace("-", "").replace(" ", "").isdigit():
                        return clean_line

    except Exception as e:
        # Fallback a PyPDF2 si fitz falla
        try:
            if PyPDF2:
                with open(filepath, "rb") as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    if pdf_reader.metadata:
                        title = pdf_reader.metadata.get("/Title", "")
                        if title and title.strip():
                            return title.strip()
        except:
            pass
        print(f"  Error leyendo PDF {filepath}: {e}")

    return None

    try:
        with open(filepath, "rb") as file:
            pdf_reader = PyPDF2.PdfReader(file)
            if pdf_reader.metadata:
                title = pdf_reader.metadata.get("/Title", "")
                if title and title.strip():
                    return title.strip()

            if len(pdf_reader.pages) > 0:
                first_page = pdf_reader.pages[0]
                text = first_page.extract_text()
                lines = [line.strip() for line in text.split("\n") if line.strip()]
                if lines:
                    potential_title = " ".join(lines[:2])
                    if len(potential_title) > 5:
                        return potential_title
    except Exception as e:
        print(f"  Error leyendo PDF {filepath}: {e}")

    return None


def extract_epub_title(filepath: str) -> Optional[str]:
    """Extrae el titulo de un archivo EPUB"""
    if not epub:
        return None

    try:
        book = epub.read_epub(filepath)
        titles = book.get_metadata("DC", "title")
        if titles:
            return titles[0][0]
    except Exception as e:
        print(f"  Error leyendo EPUB {filepath}: {e}")

    return None


def extract_mobi_title(filepath: str) -> Optional[str]:
    """Extrae el titulo de un archivo MOBI/AZW3 leyendo directamente el archivo"""
    try:
        # Leer el archivo MOBI como binario
        with open(filepath, "rb") as f:
            content = f.read()

        # Buscar el titulo en los metadatos EXTH (Extended Header)
        exth_marker = b"EXTH"
        pos = content.find(exth_marker)
        if pos != -1:
            idx = pos + 8
            end_of_exth = pos + 4 + int.from_bytes(content[pos + 4 : pos + 8], "big")

            while idx < end_of_exth:
                if idx + 8 > len(content):
                    break
                record_type = int.from_bytes(content[idx : idx + 4], "big")
                record_len = int.from_bytes(content[idx + 4 : idx + 8], "big")

                if record_type == 503 and record_len > 8:
                    title_data = content[idx + 8 : idx + record_len]
                    # Limpiar caracteres no imprimibles
                    title_data = bytes(
                        b for b in title_data if 32 <= b <= 126 or b in [10, 13]
                    )
                    try:
                        title = title_data.decode("utf-8").strip()
                        if title and len(title) > 2:
                            return title
                    except:
                        try:
                            title = title_data.decode("latin-1").strip()
                            if title and len(title) > 2:
                                return title
                        except:
                            pass

                idx += record_len
                if record_len == 0:
                    break

        # Metodo 2: Buscar texto legible al principio del archivo
        first_bytes = content[: min(50000, len(content))]

        for encoding in ["utf-8", "latin-1", "cp1252"]:
            try:
                text = first_bytes.decode(encoding, errors="ignore")
                # Limpiar caracteres no imprimibles
                text = "".join(
                    char for char in text if char.isprintable() or char in "\n\r\t"
                )
                lines = [
                    line.strip()
                    for line in text.split("\n")
                    if len(line.strip()) > 10 and len(line.strip()) < 200
                ]

                for line in lines[:10]:
                    # Limpiar la linea completamente
                    clean_line = "".join(char for char in line if char.isprintable())
                    if not any(char.isdigit() for char in clean_line[:5]):
                        if clean_line[0].isupper() or clean_line.isupper():
                            if len(clean_line.split()) >= 2:
                                return clean_line
            except:
                continue

        # Metodo 3: Usar mobi.extract
        if MOBI_AVAILABLE:
            try:
                tempdir, filepath_extracted = mobi.extract(filepath)
                import xml.etree.ElementTree as ET

                opf_path = os.path.join(tempdir, "mobi8", "OEBPS", "content.opf")
                if os.path.exists(opf_path):
                    tree = ET.parse(opf_path)
                    root = tree.getroot()
                    for meta in root.findall(
                        ".//{http://purl.org/dc/elements/1.1/}title"
                    ):
                        if meta.text:
                            return meta.text.strip()
            except:
                pass

    except Exception as e:
        print(f"  Error leyendo MOBI {filepath}: {e}")

    return None

    try:
        tempdir, filepath_extracted = mobi.extract(filepath)
        import xml.etree.ElementTree as ET

        opf_path = os.path.join(tempdir, "mobi8", "OEBPS", "content.opf")
        if os.path.exists(opf_path):
            tree = ET.parse(opf_path)
            root = tree.getroot()
            for meta in root.findall(".//{http://purl.org/dc/elements/1.1/}title"):
                if meta.text:
                    return meta.text.strip()
    except Exception as e:
        print(f"  Error leyendo MOBI {filepath}: {e}")

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
                try:
                    filepath.rename(new_filepath)
                    print(f"   Renombrado a: {new_filename}")
                    renamed_count += 1
                except Exception as e:
                    print(f"   Error al renombrar: {e}")
                    error_count += 1
            else:
                print(f"   El archivo ya tiene el nombre correcto")
        else:
            print(f"   No se pudo extraer el titulo")
            error_count += 1

    print("\n" + "=" * 80)
    print(f"\nResumen:")
    print(f"   Renombrados: {renamed_count}")
    print(f"   Sin titulo: {error_count}")
    print(f"   Total: {len(files)}")


if __name__ == "__main__":
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
