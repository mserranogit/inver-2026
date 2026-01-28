import streamlit as st

from src.menu.sidebar_menu import SidebarMenu


filelist=[]

st.set_page_config(
    page_title="Inicio",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.write("Leer fichero json")

menu = SidebarMenu()
menu.createMenu()
