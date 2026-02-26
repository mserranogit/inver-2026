import streamlit as st
from styles import apply_styles

# ==========================================
# CONFIGURACIÓN GENERAL
# ==========================================
st.set_page_config(
    page_title="Inver 2026",
    page_icon="📊",
    layout="wide"
)

# Aplicar estilos globales
apply_styles()

# Función para el Dashboard Principal (evita recursión al no cargar el archivo Inicio.py)
def show_dashboard():
    st.title("📊 Inver 2026")
    st.subheader("Plataforma Profesional de Análisis de Fondos y ETFs")

    col1, col2, col3 = st.columns(3)
    col1.metric("Fondos en Base de Datos", "125")
    col2.metric("Duración Media", "3.2 años")
    col3.metric("Rentabilidad Media", "2.84 %")

    st.divider()
    st.markdown("### Bienvenido al sistema de análisis y construcción de carteras.")
    st.info("Utilice el menú de la izquierda para navegar entre las diferentes secciones de Fondos, ETFs y Datos Macro.")

# Definimos las páginas por secciones
pages = {
    "Dashboard": [
        st.Page(show_dashboard, title="Principal", icon="📊", default=True)
    ],
    "Mi Cartera": [
        st.Page("pages/13_💼_Gestion_Personal.py", title="Gestión", icon="💼"),
        st.Page("pages/14_📈_Rentabilidad_Personal.py", title="Rentabilidad", icon="📈"),
    ],
    "Fondos": [
        st.Page("pages/1_📈_Fondos_Renta_Fija.py", title="Fondos de Renta Fija", icon="📈"),
        st.Page("pages/2_⚖️_Comparador.py", title="Comparador", icon="⚖️"),
        st.Page("pages/3_🧮_Constructor_Cartera.py", title="Constructor de cartera", icon="🧮"),
        st.Page("pages/7_🤖_Constructor_Automatico.py", title="Constructor Automático", icon="🤖"),
    ],
    "ETFs": [
        st.Page("pages/8_📋_Lista_ETFs.py", title="Lista ETFs", icon="📋"),
        st.Page("pages/9_⚖️_Comparador_ETFs.py", title="Comparador", icon="⚖️"),
        st.Page("pages/10_🧮_Constructor_ETFs.py", title="Constructor de cartera", icon="🧮"),
        st.Page("pages/11_🤖_Constructor_Automatico_ETFs.py", title="Constructor Automático", icon="🤖"),
    ],
    "Macro": [
        st.Page("pages/15_📊_Datos_Macro.py", title="Datos Macro", icon="📊"),
        st.Page("pages/5_🏦_Tipos_Interes.py", title="Tipos de Interés", icon="🏦"),
        st.Page("pages/6_📈_Curvas_Tipos.py", title="Curvas de Tipos", icon="📈"),
    ],
    "Administración": [
        st.Page("pages/4_⚙️_Administracion.py", title="Administración", icon="⚙️"),
        st.Page("pages/12_📁_Gestion_Carteras.py", title="Gestión de Carteras", icon="📁"),
    ]
}

# Ejecutar Navegación
pg = st.navigation(pages)
pg.run()
