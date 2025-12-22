import os
import requests
import datetime
import re
import json
import logging
import pandas as pd
import geopandas as gpd
import numpy as np
from bs4 import BeautifulSoup
from arcgis.features import GeoAccessor

# Configuración de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURACIÓN ---
BASE_URL_DIR_TEMPLATE = "https://siata.gov.co/geotecnia/COE_{year}/modelos/{yyyymmdd}/AM/"
FILE_NAME_TEMPLATE = "alertas_7d_{yyyy_mm_dd}.kml"
ITEM_ID = "c69debbaa88047c394f1c1eff4922143"

# Definir ruta de descarga dentro del proyecto (carpeta 'data' en la raíz)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR)) # Subir 2 niveles
LOCAL_DOWNLOAD_PATH = os.path.join(ROOT_DIR, "data", "mov_masa")

def descargar_siata_diario():
    """Descarga el KML del día actual."""
    if not os.path.exists(LOCAL_DOWNLOAD_PATH):
        os.makedirs(LOCAL_DOWNLOAD_PATH)

    hoy = datetime.date.today()
    url_completa = BASE_URL_DIR_TEMPLATE.format(
        year=hoy.strftime("%Y"), 
        yyyymmdd=hoy.strftime("%Y%m%d")
    ) + FILE_NAME_TEMPLATE.format(yyyy_mm_dd=hoy.strftime("%Y-%m-%d"))
    
    nombre_archivo = f"alertas_siata_{hoy.strftime('%Y-%m-%d')}.kml"
    ruta_local = os.path.join(LOCAL_DOWNLOAD_PATH, nombre_archivo)
    
    logging.info(f"⬇️ Descargando KML desde: {url_completa}")
    try:
        response = requests.get(url_completa, headers={'User-Agent': 'Mozilla/5.0'}, stream=True, timeout=60)
        if response.status_code == 200:
            with open(ruta_local, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return ruta_local
        else:
            logging.error(f"❌ Error HTTP: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"❌ Fallo descarga: {e}")
        return None

def procesar_movimientos_masa(gis):
    """
    Pipeline completo: Descarga -> ETL (BeautifulSoup + GeoPandas) -> Carga ArcGIS
    """
    logging.info("⛰️ Iniciando pipeline Movimientos en Masa")

    # 1. Descargar Datos
    ruta_kml = descargar_siata_diario()
    if not ruta_kml or not os.path.exists(ruta_kml):
        logging.error("❌ No hay archivo KML para procesar.")
        return False

    # 2. Habilitar Drivers KML y Leer
    gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
    gpd.io.file.fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
    
    try:
        gdf = gpd.read_file(ruta_kml, driver='LIBKML')
    except Exception:
        try:
            gdf = gpd.read_file(ruta_kml)
        except Exception as e:
            logging.error(f"❌ Error leyendo KML: {e}")
            return False

    if gdf.empty:
        logging.warning("⚠️ KML vacío.")
        return False

    # 3. Limpieza de Columnas
    cols_limpias = []
    for col in gdf.columns:
        clean = re.sub(r'[^a-zA-Z0-9_]', '', str(col).strip().replace(" ", "_"))
        cols_limpias.append(f"_{clean}" if clean and clean[0].isdigit() else clean)
    gdf.columns = cols_limpias

    # Identificar columnas clave
    col_name = next((c for c in gdf.columns if "Name" in c), None)
    col_desc = next((c for c in gdf.columns if "Description" in c), None)

    # 4. Parseo de HTML (BeautifulSoup)
    # Mapeo: Clave HTML -> Campo ArcGIS
    html_map = {
        "Municipio": "Municipio", "Área_texto": "Area_Texto", 
        "Vereda": "Vereda", "Comuna": "Comuna", "Barrio": "Barrio",
        "Área_num": "Area_Numerica", "Acu_7": "Acu_7", "Acu_90_7": "Acu_90_7"
    }
    
    # Inicializar columnas
    for col in html_map.values():
        gdf[col] = None

    if col_desc:
        for idx, row in gdf.iterrows():
            html = row[col_desc]
            if isinstance(html, str):
                soup = BeautifulSoup(html, 'html.parser')
                rows = soup.find_all('tr')
                for tr in rows:
                    cells = tr.find_all('td')
                    if len(cells) == 2:
                        key = re.sub(r'[^a-zA-Z0-9_]', '', cells[0].get_text(strip=True).replace(" ", "_"))
                        val = cells[1].get_text(strip=True)
                        
                        # Lógica específica para mapear keys del HTML a columnas del DF
                        if key == "rea": # A veces 'Área' pierde tilde
                            if gdf.at[idx, "Area_Texto"] is None: gdf.at[idx, "Area_Texto"] = val
                            else: gdf.at[idx, "Area_Numerica"] = val
                        elif key in html_map:
                             gdf.at[idx, html_map[key]] = val
                        elif key == "Acu_7": gdf.at[idx, "Acu_7"] = val
                        elif key == "Acu_90_7": gdf.at[idx, "Acu_90_7"] = val

    # 5. Geometría y Proyección
    gdf = gdf[gdf.geometry.notnull() & gdf.geometry.is_valid]
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    
    # 6. Preparar atributos para ArcGIS
    # Lógica para SymbolID y Name
    gdf['SymbolID'] = 0
    if col_name:
        # Extraer número del nombre para SymbolID (ej: "Alerta - 1" -> 1)
        gdf['SymbolID'] = gdf[col_name].astype(str).str.extract(r'(\d+)$').fillna(0).astype(int)
        gdf['Name'] = gdf[col_name] # Simplificado para el ejemplo
        gdf['categoria'] = gdf['SymbolID'] # Según tu script original

    # 7. Carga a ArcGIS (Usando GeoAccessor que es más rápido que el bucle for manual)
    try:
        logging.info("🌐 Actualizando ArcGIS Online...")
        layer = gis.content.get(ITEM_ID).layers[0]
        layer.delete_features(where="1=1")
        
        # Convertir a SDF y subir
        # Seleccionamos solo las columnas que nos interesan para evitar errores de esquema
        cols_finales = list(html_map.values()) + ['Name', 'SymbolID', 'categoria', 'geometry']
        cols_existentes = [c for c in cols_finales if c in gdf.columns]
        
        sdf = GeoAccessor.from_geodataframe(gdf[cols_existentes])
        layer.edit_features(adds=sdf)
        
        logging.info(f"🎉 Éxito. {len(gdf)} registros cargados.")
        return True
        
    except Exception as e:
        logging.error(f"❌ Error en carga a ArcGIS: {e}")
        return False