import streamlit as st
from styles import apply_styles

st.set_page_config(layout="wide")
apply_styles()
st.title("📈 Fondos de Renta Fija")

submenu = st.sidebar.selectbox(
    "Opciones",
    ["Listado", "Análisis Individual", "Ranking"]
)

if submenu == "Listado":
    st.header("Listado de Fondos")
    st.dataframe({
        "ISIN": ["LU0293294277", "FR0011387299"],
        "Tramo": ["long", "short"],
        "Sensibilidad": ["media", "baja"]
    })

elif submenu == "Análisis Individual":
    st.header("Análisis Individual")
    st.info("Aquí irá el análisis completo.")

elif submenu == "Ranking":
    st.header("Ranking de Fondos")
    st.info("Ranking por Sharpe, duración, etc.")
