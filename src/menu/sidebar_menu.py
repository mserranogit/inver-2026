import streamlit as st
from sidebar_accordion_menu import sidebar_accordion_menu



class SidebarMenu:

    def createMenu(self):
        menu = {
            "🏠 Home": "home",  # None or "home" for main page
            "🧐 Contabilidad": {
                "📱 Categorías": "categorias",  # .py extension added automatically
                "📆️ Selecciona mes": "sel_mes",  # .py extension added automatically
                "𝄜 Convierte a csv": "convierte_csv_mes",  # .py extension added automatically
                "🤔️️ Mostrar mes": "mostrar_mes",
                "☑️ Importar mes": "importar_mes",
                "☑️ Modificaciones": "crud_conta_grupos"
            },
            "📊 Análisis": {
                "📋 Informe mensual": "informes_mensual",
                "📋 Informe anual": "informes_anual",
                "📈 Gráficos mes": "graficos_mensual",
                "📈 Gráficos año": "graficos_anual"
            },
            "⚙️ Inversiones": {
                "Depósitos": "crud_depositos",
                "Fondos": "crud_fondos",
                "ETF": "crud_etf",
                "Resultado": "resul_inver",
            }
        }
        sidebar_accordion_menu(menu)

