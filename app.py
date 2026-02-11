import streamlit as st

# ======================================
# CONFIGURACIÓN
# ======================================
st.set_page_config(
    page_title="Inver 2026",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ======================================
# ESTILOS
# ======================================
st.markdown("""
<style>

/* Quitar barra blanca superior */
header {visibility: hidden;}
footer {visibility: hidden;}
[data-testid="stToolbar"] {display: none;}
.block-container {padding-top: 0.5rem;}

/* Fondo principal */
.main {
    background-color: #F4F6F9;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #E9EEF4;
    padding-top: 1.5rem;
}

/* Título */
.sidebar-title {
    font-size: 20px;
    font-weight: 600;
    color: #1F2937;
    margin-bottom: 10px;
}

/* Secciones */
.sidebar-section {
    font-size: 11px;
    font-weight: 600;
    color: #6B7280;
    margin-top: 5px;
    margin-bottom: 3px;
    letter-spacing: 0.5px;
}

/* BOTONES MÁS COMPACTOS */
.stButton > button {
    width: 100%;
    background-color: transparent;
    color: #374151;
    border: none;
    text-align: left;
    border-radius: 6px;
    font-size: 12px;
}

/* Hover elegante */
.stButton > button:hover {
    background-color: #DCE4F2;
    color: #111827;
}

/* Elimina margen vertical extra que Streamlit añade */
div[data-testid="stVerticalBlock"] > div {
    gap: 0.2rem;
}

</style>

""", unsafe_allow_html=True)

# ======================================
# ESTADO DE NAVEGACIÓN
# ======================================
if "page" not in st.session_state:
    st.session_state.page = "Dashboard"

# ======================================
# SIDEBAR
# ======================================
with st.sidebar:

    st.markdown('<div class="sidebar-title">📊 Inver 2026</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">ANÁLISIS</div>', unsafe_allow_html=True)

    if st.button("Dashboard"):
        st.session_state.page = "Dashboard"

    if st.button("Fondos Renta Fija"):
        st.session_state.page = "Fondos Renta Fija"

    if st.button("Ranking"):
        st.session_state.page = "Ranking"

    st.markdown('<div class="sidebar-section">CARTERAS</div>', unsafe_allow_html=True)

    if st.button("Constructor"):
        st.session_state.page = "Constructor"

    if st.button("Comparador"):
        st.session_state.page = "Comparador"

    st.markdown('<div class="sidebar-section">SISTEMA</div>', unsafe_allow_html=True)

    if st.button("Administración"):
        st.session_state.page = "Administración"

    if st.button("Logs"):
        st.session_state.page = "Logs"

# ======================================
# CONTENIDO PRINCIPAL
# ======================================
st.title(st.session_state.page)

if st.session_state.page == "Dashboard":
    st.info("Vista global del universo de fondos.")

elif st.session_state.page == "Fondos Renta Fija":
    st.info("Listado y análisis individual.")

elif st.session_state.page == "Ranking":
    st.info("Clasificación por métricas clave.")

elif st.session_state.page == "Constructor":
    st.info("Optimización y simulación.")

elif st.session_state.page == "Comparador":
    st.info("Comparación entre carteras.")

elif st.session_state.page == "Administración":
    st.info("Gestión del sistema.")

elif st.session_state.page == "Logs":
    st.info("Registro de eventos.")
