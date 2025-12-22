import sys
import os
import logging

# --- 1. CONFIGURACIÓN DE RUTAS ---
# Esto permite importar desde 'pipelines' y 'utils' estando en 'serve'
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# --- 2. IMPORTACIONES ---
from utils.arcgis_auth import autenticar_arcgis
# Asegúrate de importar la función correcta que definiste en pipelines
from pipelines.operational.main_operacional import procesar_datos_operacionales 

# Configuración básica de logs (Buena práctica en Ciencia de Datos)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    logging.info("🚀 Iniciando ejecución del Dashboard Operacional")
    
    try:
        # 1. Autenticar (Centralizado)
        gis = autenticar_arcgis()
        
        # 2. Ejecutar Lógica (Modularizada)
        if gis:
            procesar_datos_operacionales(gis)
            logging.info("✅ Proceso finalizado exitosamente")
        else:
            logging.error(" Falló la autenticación en ArcGIS")
            
    except Exception as e:
        logging.error(f"💀 Error crítico en el proceso: {e}")
        # Aquí podrías agregar un envío de correo automático si falla