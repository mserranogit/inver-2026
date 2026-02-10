import streamlit as st

from src.menu.sidebar_menu import SidebarMenu

st.set_page_config(
    page_title="My App",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# Render the accordion menu
menu = SidebarMenu()
menu.createMenu()
# Your main page content
st.title("Welcome to My App")
st.write("Select a page from the sidebar menu.")
no_sidebar_style = """
    <style>
        div[data-testid="stSidebarNav"] {display: none;}
    </style>
"""
st.markdown(no_sidebar_style, unsafe_allow_html=True)