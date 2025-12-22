import sys
import os
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from utils.arcgis_auth import autenticar_arcgis
from pipelines.mass_movements.main_mass_movements import procesar_movimientos_masa

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    try:
        logging.info("🚀 Ejecutando orquestador de Movimientos en Masa")
        gis = autenticar_arcgis()
        if gis:
            procesar_movimientos_masa(gis)
    except Exception as e:
        logging.error(f"💀 Error crítico: {e}")