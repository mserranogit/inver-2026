import streamlit as st

# ==========================================
# CONFIGURACIÓN GENERAL
# ==========================================
st.set_page_config(
    page_title="Inver 2026",
    page_icon="📊",
    layout="wide"
)

# ==========================================
# LIMPIEZA UI
# ==========================================
st.markdown("""
<style>
header {visibility: hidden;}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

.stApp {
    background-color: #f4f6f9;
}

section[data-testid="stSidebar"] {
    background-color: #2c3e50;
}

section[data-testid="stSidebar"] * {
    color: #ecf0f1 !important;
}

h1, h2, h3 {
    color: #2c3e50;
}
</style>
""", unsafe_allow_html=True)

# ==========================================
# DASHBOARD
# ==========================================

st.title("📊 Inver 2026")
st.subheader("Plataforma Profesional de Análisis de Fondos")

col1, col2, col3 = st.columns(3)

col1.metric("Fondos en Base de Datos", "125")
col2.metric("Duración Media", "3.2 años")
col3.metric("Rentabilidad Media", "2.84 %")

st.divider()

st.markdown("### Bienvenido al sistema de análisis y construcción de carteras.")
