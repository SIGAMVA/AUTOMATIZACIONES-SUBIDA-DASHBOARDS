# -*- coding: utf-8 -*-

import requests
import pandas as pd
import geopandas as gpd
from arcgis.gis import GIS
from arcgis.features import GeoAccessor
import io
import datetime # Importamos la librería de fecha y hora

# ===================================================================
# === 1. CONFIGURACIÓN GENERAL                                    ===
# ===================================================================

URL_DATOS_KML = "https://siata.gov.co/hidrologia/incendios_forestales/Mapa_diario_AMVA/susceptibilidad_IF.kml"

# --- Credenciales y ID del Feature Service ---
GIS_URL = "https://www.arcgis.com"
GIS_USER = "unidad.gestion.riesgo"
ITEM_ID = "49294579c5f341b8b78b066a705ca7c3" 

# ⚠️ ADVERTENCIA DE SEGURIDAD: Usar solo en entornos controlados.
GIS_PASSWORD = "XXXXXXXX" # Reemplaza con tu contraseña

# ===================================================================
# === 2. FUNCIÓN PRINCIPAL DE ACTUALIZACIÓN                       ===
# ===================================================================

def actualizar_capa_incendios_produccion_final():
    """
    Versión final con "cache busting" para garantizar la descarga de datos frescos.
    """
    print("\n🚀 Iniciando proceso de actualización de producción (con cache busting)...")
    
    # --- PASO A: Descargar y Leer Datos KML (Con Cache Busting) ---
    try:
        print("🔗 Paso 1/3: Descargando datos KML desde SIATA...")
        
        # ========== MODIFICACIÓN CLAVE: CACHE BUSTING ==========
        # Generamos un número único basado en la hora actual
        timestamp = int(datetime.datetime.now().timestamp())
        # Creamos una nueva URL con el parámetro único para "engañar" al caché
        cache_busting_url = f"{URL_DATOS_KML}?v={timestamp}"
        print(f"    -> URL generada: {cache_busting_url}")
        
        headers = {
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        # =====================================================

        buffer = io.BytesIO()
        
        # Usamos la nueva URL única en la solicitud
        with requests.get(cache_busting_url, timeout=60, stream=True, headers=headers) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                buffer.write(chunk)
        
        buffer.seek(0)

        print("📄 Paso 2/3: Leyendo polígonos con GeoPandas...")
        gdf = gpd.read_file(buffer)

        if gdf.empty:
            print("⚠️ ADVERTENCIA: El KML descargado está vacío. Proceso detenido."); return
        
        gdf = gdf[gdf.geometry.notna() & gdf.geometry.is_valid].to_crs(epsg=4326)
        
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR de red durante la descarga del KML: {e}"); return
    except Exception as e:
        print(f"❌ ERROR durante la lectura del KML: {e}"); return

    # --- PASO B: Transformación de Datos con el Mapeo Corregido ---
    try:
        if 'Name' not in gdf.columns:
            print("❌ CRÍTICO: No se encontró la columna 'Name' en el KML."); return
        
        mapeo_simbologia = {
            "Susc: 1": 2,
            "Susc: 2": 1,
            "Susc: 3": 0
        }

        gdf['SymbolID'] = gdf['Name'].str.strip().map(mapeo_simbologia)
        gdf['SymbolID'] = gdf['SymbolID'].fillna(-1).astype(int)
        print("✅ Campo 'SymbolID' creado y populado con la lógica correcta.")
            
    except Exception as e:
        print(f"❌ ERROR durante la transformación de datos: {e}"); return

    # --- PASO C: Conexión y Carga a ArcGIS Online ---
    try:
        print("\n🌐 Paso 3/3: Conectando y actualizando la capa en ArcGIS Online...")
        gis = GIS(GIS_URL, GIS_USER, GIS_PASSWORD)
        
        target_layer = gis.content.get(ITEM_ID).layers[0]
        print(f"🎯 Capa objetivo: '{target_layer.properties.name}'")
        
        print("    -> Borrando entidades antiguas..."); target_layer.delete_features(where='1=1')
        
        sdf = GeoAccessor.from_geodataframe(gdf)
        print("    -> Añadiendo nuevas entidades..."); target_layer.edit_features(adds=sdf)
        
        print("\n🎉 ¡ÉXITO! El Feature Service ha sido actualizado correctamente.")

    except Exception as e:
        print(f"❌ ERROR durante la conexión o actualización en ArcGIS Online: {e}")

# ===================================================================
# === 3. EJECUCIÓN DEL SCRIPT                                     ===
# ===================================================================

if __name__ == "__main__":
    actualizar_capa_incendios_produccion_final()

    
