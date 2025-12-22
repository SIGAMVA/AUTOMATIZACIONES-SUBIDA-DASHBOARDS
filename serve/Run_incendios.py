serve/run_incendios.py
import sys
import os
import logging

# Configuración de rutas para encontrar 'pipelines' y 'utils'
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from utils.arcgis_auth import autenticar_arcgis
from pipelines.fire_susceptibility.main_fire_susceptibility import procesar_incendios

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    try:
        gis = autenticar_arcgis()
        if gis:
            procesar_incendios(gis)
    except Exception as e:
        logging.error(f" Error crítico en run_incendios: {e}")