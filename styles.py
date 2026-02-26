import streamlit as st

def apply_styles():
    st.markdown("""
    <style>

    /* ===========================
       OCULTAR ELEMENTOS STREAMLIT
       =========================== */
    header[data-testid="stHeader"] {visibility: hidden;}
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

    /* Todos los textos del sidebar en color claro */
    [data-testid="stSidebarNav"] span,
    [data-testid="stNavSectionHeader"] span {
        color: #e5e7eb !important;
        font-weight: 500;
    }

    /* Estilo diferenciado para títulos de sección */
    [data-testid="stNavSectionHeader"] span {
        font-size: 0.68rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.12em !important;
        text-transform: uppercase !important;
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

    /* ===========================
       PAGINACIÓN
       =========================== */
    .pagination-info {
        font-size: 14px;
        color: #6c757d;
        margin-top: 6px;
    }

    .page-active {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 38px;
        background-color: #4a6fa5;
        color: white;
        border-radius: 4px;
        font-weight: 600;
        text-align: center;
    }

    </style>
    """, unsafe_allow_html=True)
