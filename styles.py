import streamlit as st

def apply_styles():
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
