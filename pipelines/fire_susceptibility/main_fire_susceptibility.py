import requests
import geopandas as gpd
from arcgis.features import GeoAccessor
import io
import datetime
import logging

# Configuración de Logs local para este módulo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURACIÓN ---
URL_DATOS_KML = "https://siata.gov.co/hidrologia/incendios_forestales/Mapa_diario_AMVA/susceptibilidad_IF.kml"
ITEM_ID = "49294579c5f341b8b78b066a705ca7c3" 

def procesar_incendios(gis):
    """
    Ejecuta la actualización de la capa de Incendios.
    Args:
        gis: Objeto GIS autenticado (desde utils).
    """
    logging.info("🔥 Iniciando pipeline de Susceptibilidad de Incendios")
    
    # --- PASO A: Descargar y Leer Datos KML (Con Cache Busting) ---
    try:
        logging.info("🔗 Descargando datos KML desde SIATA...")
        
        # Cache Busting
        timestamp = int(datetime.datetime.now().timestamp())
        cache_busting_url = f"{URL_DATOS_KML}?v={timestamp}"
        
        headers = {'Cache-Control': 'no-cache', 'Pragma': 'no-cache'}
        buffer = io.BytesIO()
        
        with requests.get(cache_busting_url, timeout=60, stream=True, headers=headers) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                buffer.write(chunk)
        
        buffer.seek(0)
        gdf = gpd.read_file(buffer)

        if gdf.empty:
            logging.warning("⚠️ El KML descargado está vacío.")
            return False
        
        # Limpieza geometría
        gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].to_crs(epsg=4326)
        
    except Exception as e:
        logging.error(f"❌ Error en descarga/lectura KML: {e}")
        return False

    # --- PASO B: Transformación ---
    try:
        if 'Name' not in gdf.columns:
            logging.error("❌ No se encontró la columna 'Name' en el KML.")
            return False
        
        mapeo_simbologia = {"Susc: 1": 2, "Susc: 2": 1, "Susc: 3": 0}
        gdf['SymbolID'] = gdf['Name'].str.strip().map(mapeo_simbologia).fillna(-1).astype(int)
        
    except Exception as e:
        logging.error(f"❌ Error en transformación: {e}")
        return False

    # --- PASO C: Carga a ArcGIS ---
    try:
        logging.info("🌐 Actualizando capa en ArcGIS Online...")
        target_layer = gis.content.get(ITEM_ID).layers[0]
        
        # Truncate (Borrar todo)
        target_layer.delete_features(where='1=1')
        
        # Append (Agregar nuevos)
        sdf = GeoAccessor.from_geodataframe(gdf)
        target_layer.edit_features(adds=sdf)
        
        logging.info("🎉 Capa de Incendios actualizada correctamente.")
        return True

    except Exception as e:
        logging.error(f"❌ Error actualizando ArcGIS: {e}")
        return False