import streamlit as st

def apply_styles():
    st.markdown("""
    <style>

    /* ===========================
       OCULTAR ELEMENTOS STREAMLIT
       =========================== */
    header {visibility: hidden;}
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* ===========================
       FONDO GENERAL
       =========================== */
    .stApp {
        background-color: #f4f6f9;
    }

    /* ===========================
       SIDEBAR
       =========================== */
    section[data-testid="stSidebar"] {
        background-color: #1f2937;
        padding-top: 1rem;
    }

    /* ===========================
       MENÚ MULTIPAGE (CORRECTO)
       =========================== */

    /* Texto del menú */
    [data-testid="stSidebarNav"] span {
        color: #e5e7eb !important;
        font-weight: 500;
    }

    /* Hover */
    [data-testid="stSidebarNav"] button:hover {
        background-color: rgba(255,255,255,0.08) !important;
        border-radius: 6px;
    }

    /* Página activa */
    [data-testid="stSidebarNav"] button[aria-current="page"] {
        background-color: #4a6fa5 !important;
        border-radius: 6px;
    }

    [data-testid="stSidebarNav"] button[aria-current="page"] span {
        color: #ffffff !important;
        font-weight: 600;
    }

    /* ===========================
       FILTROS
       =========================== */

    section[data-testid="stSidebar"] label {
        color: #f1f5f9 !important;
        font-weight: 600;
    }

    section[data-testid="stSidebar"] div[data-baseweb="select"] {
        color: #111827 !important;
    }

    div[role="listbox"] {
        color: #111827 !important;
    }

    /* ===========================
       TÍTULOS
       =========================== */
    h1, h2, h3 {
        color: #2c3e50;
    }

    </style>
    """, unsafe_allow_html=True)
