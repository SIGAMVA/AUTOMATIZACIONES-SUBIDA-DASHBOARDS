from arcgis.gis import GIS
import geopandas as gpd
import os
import pandas as pd
from datetime import date
import json
import re # Para expresiones regulares
from bs4 import BeautifulSoup # Para parsear HTML de la descripción
import numpy as np # Para verificar None o NaN de manera más robusta

import requests
import datetime

# --- Configuración descarga ---
BASE_URL_DIR_TEMPLATE = "https://siata.gov.co/geotecnia/COE_{year}/modelos/{yyyymmdd}/AM/"
FILE_NAME_TEMPLATE = "alertas_7d_{yyyy_mm_dd}.kml"
LOCAL_DOWNLOAD_PATH = "E:\PRACTICAS_2025_1\EMERGENCIA\datos"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

def descargar_archivo_siata_diario():
    hoy = datetime.date.today()
    year_str = hoy.strftime("%Y")
    yyyymmdd_str = hoy.strftime("%Y%m%d")
    yyyy_mm_dd_str = hoy.strftime("%Y-%m-%d")

    url_directorio = BASE_URL_DIR_TEMPLATE.format(year=year_str, yyyymmdd=yyyymmdd_str)
    nombre_archivo_remoto = FILE_NAME_TEMPLATE.format(yyyy_mm_dd=yyyy_mm_dd_str)
    url_completa = url_directorio + nombre_archivo_remoto
    nombre_archivo_local = os.path.join(LOCAL_DOWNLOAD_PATH, f"alertas_siata_{yyyy_mm_dd_str}.kml")

    if not os.path.exists(LOCAL_DOWNLOAD_PATH):
        os.makedirs(LOCAL_DOWNLOAD_PATH)

    print(f"[INFO] Descargando archivo desde: {url_completa}")
    try:
        response = requests.get(url_completa, headers=HEADERS, stream=True, timeout=60)
        if response.status_code == 200:
            with open(nombre_archivo_local, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"[OK] Archivo descargado correctamente en: {nombre_archivo_local}")
        else:
            print(f"[ERROR] Código de estado HTTP: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] Fallo al descargar archivo SIATA: {e}")



# === CONFIGURACIÓN ===
# Login
gis = GIS("https://www.arcgis.com", "unidad.gestion.riesgo", "XXXXXXX") # Reemplaza con tus credenciales

# ID del Feature Layer en ArcGIS Online
item_id = "c69debbaa88047c394f1c1eff4922143" # Reemplaza con el ID de tu item

# Ruta local al archivo KML alertas_siata_2025-05-26
# nombre_kml = "alertas_siata_2025-05-26.kml" # Asegúrate que este sea el archivo correcto
# # Cambia la siguiente ruta a donde tengas tu archivo KML

# === NUEVO BLOQUE AUTOMATIZADO PARA GENERAR NOMBRE DE ARCHIVO KML ===
from datetime import date

# Fecha actual
fecha_actual = date.today()
fecha_str = fecha_actual.strftime("%Y-%m-%d")

# Nombre dinámico del archivo basado en fecha
nombre_kml = f"alertas_siata_{fecha_str}.kml"
ruta_kml = os.path.join("E:\PRACTICAS_2025_1\EMERGENCIA\datos", nombre_kml)


ruta_kml = os.path.join("E:\PRACTICAS_2025_1\EMERGENCIA\datos", nombre_kml)

# --- Nombres de los campos de destino en ArcGIS Feature Layer ---
name_target_field_arcgis = "Name"       # Campo para el nombre descriptivo
symbol_id_target_field_arcgis = "SymbolID" # Campo para el ID de simbología (numérico)
categoria_target_field_arcgis = "categoria" # Campo para la categexioría

# Mapeo de CLAVES LIMPIADAS del HTML a los NOMBRES DE CAMPO en ArcGIS.
html_key_to_arcgis_field_map = {
    "Municipio": "Municipio",
    "Área_texto": "Area_Texto", 
    "Vereda": "Vereda",
    "Comuna": "Comuna",
    "Barrio": "Barrio",
    "Área_num": "Area_Numerica", 
    "Acu_7": "Acu_7",
    "Acu_90_7": "Acu_90_7",
}

DEFAULT_SYMBOL_ID_IF_NOT_FOUND = 0 


# Llama la función justo antes de leer el archivo
descargar_archivo_siata_diario()

# Luego se genera el nombre del archivo:
fecha_actual = date.today()
fecha_str = fecha_actual.strftime("%Y-%m-%d")
nombre_kml = f"alertas_siata_{fecha_str}.kml"
ruta_kml = os.path.join("E:\PRACTICAS_2025_1\EMERGENCIA\datos", nombre_kml)


# === 1. VERIFICAR Y LEER ARCHIVO KML ===

if not os.path.exists(ruta_kml):
    print(f"❌ No se encontró el archivo KML: {ruta_kml}"); exit()

print(f"📄 Leyendo archivo KML: {ruta_kml}")
try:
    gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
    gpd.io.file.fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
    gdf = gpd.read_file(ruta_kml, driver='LIBKML')
except Exception as e_libkml:
    print(f"⚠️ Error al leer KML con LIBKML: {e_libkml}. Intentando con driver KML por defecto...")
    try: gdf = gpd.read_file(ruta_kml) 
    except Exception as e_defaultkml: print(f"❌ Error fatal al leer KML: {e_defaultkml}"); exit()

if gdf.empty: print("❌ El archivo KML está vacío o no produjo un GeoDataFrame."); exit()
print(f"ジオ Filas en GDF después de leer KML: {len(gdf)}")
# print(f"ジオ Columnas originales del KML: {gdf.columns.tolist()}") 
# if 'Name' in gdf.columns and not gdf.empty: print(f"ジオ Muestra KML Name (1ra fila): {gdf['Name'].iloc[0] if len(gdf) > 0 else 'N/A'}")
# if 'Description' in gdf.columns and not gdf.empty: print(f"ジオ Muestra KML Description (1ra fila): {gdf['Description'].iloc[0] if len(gdf) > 0 else 'N/A'}")

# === 2. LIMPIEZA INICIAL DE NOMBRES DE COLUMNA DEL GDF ===
original_kml_column_names = gdf.columns.tolist()
cleaned_gdf_column_names = []
for col_name in original_kml_column_names:
    cleaned = str(col_name).strip().replace(" ", "_").replace("-", "_").replace(":", "_")
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '', cleaned) 
    if cleaned and cleaned[0].isdigit(): cleaned = "_" + cleaned
    if not cleaned: cleaned = f"col_limpia_vacia_{len(cleaned_gdf_column_names)}"
    cleaned_gdf_column_names.append(cleaned)
gdf.columns = cleaned_gdf_column_names
# print(f"ジオ Nombres de columna GDF después de limpieza inicial: {gdf.columns.tolist()}")

kml_name_col_cleaned = None 
kml_description_col_cleaned = None
if "Name" in original_kml_column_names: 
    kml_name_col_cleaned = cleaned_gdf_column_names[original_kml_column_names.index("Name")]
    print(f"ジオ Columna 'Name' del KML (original) ahora es '{kml_name_col_cleaned}' en GDF")
if "Description" in original_kml_column_names:
    kml_description_col_cleaned = cleaned_gdf_column_names[original_kml_column_names.index("Description")]
    # print(f"ジオ Columna 'Description' del KML (original) ahora es '{kml_description_col_cleaned}' en GDF")

# === 2.5 FILTRO (MENOS AGRESIVO AHORA) BASADO EN LA EXISTENCIA DE DATOS EN LA COLUMNA 'NAME' DEL KML ===
# Este filtro se mantiene, pero el filtro principal será el de contenido HTML.
if kml_name_col_cleaned and kml_name_col_cleaned in gdf.columns:
    print(f"ジオ Filas en GDF ANTES del filtro por KML Name (pre-HTML): {len(gdf)}")
    initial_filter_condition = gdf[kml_name_col_cleaned].notna() & \
                               (gdf[kml_name_col_cleaned].astype(str).str.strip() != '')
    gdf = gdf[initial_filter_condition]
    print(f"ℹ️ Filas en GDF DESPUÉS del filtro por KML Name (no nulo/vacío/espacios): {len(gdf)}")
    if gdf.empty:
        print(f"❌ ADVERTENCIA: No quedaron filas después de filtrar por la columna KML Name '{kml_name_col_cleaned}'. El KML podría no tener nombres válidos.")
        # No salir necesariamente, el filtro HTML podría ser más relevante.
else:
    print(f"⚠️ ADVERTENCIA: No se pudo aplicar el filtro por KML Name. Columna '{kml_name_col_cleaned}' no encontrada o no identificada.")

# === 3. EXTRAER ATRIBUTOS DEL HTML Y AÑADIRLOS AL GDF ===
expected_html_cols = list(html_key_to_arcgis_field_map.keys())
for col in expected_html_cols: gdf[col] = None # Inicializar columnas en GDF

if kml_description_col_cleaned and kml_description_col_cleaned in gdf.columns and not gdf.empty:
    # print(f"ℹ️ Procesando HTML de la columna GDF: '{kml_description_col_cleaned}'") 
    for index, kml_row in gdf.iterrows(): 
        html_content = kml_row[kml_description_col_cleaned]
        if html_content and isinstance(html_content, str):
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                table = soup.find('table') 
                if table:
                    html_table_rows = table.find_all('tr')
                    area_keys_counter = 0 
                    for tr in html_table_rows:
                        cells = tr.find_all('td')
                        if len(cells) == 2: 
                            key_from_html_raw = cells[0].get_text(strip=True)
                            value_from_html = cells[1].get_text(strip=True)
                            key_cleaned_from_html = str(key_from_html_raw).strip().replace(" ", "_").replace("-", "_").replace(":", "_")
                            key_cleaned_from_html = re.sub(r'[^a-zA-Z0-9_]', '', key_cleaned_from_html)
                            if key_cleaned_from_html and key_cleaned_from_html[0].isdigit(): key_cleaned_from_html = "_" + key_cleaned_from_html
                            
                            # Asignar valor si la clave limpia es una de las columnas esperadas del HTML
                            if key_cleaned_from_html == "Área": 
                                if area_keys_counter == 0 and "Área_texto" in expected_html_cols: gdf.loc[index, "Área_texto"] = value_from_html
                                elif area_keys_counter == 1 and "Área_num" in expected_html_cols: gdf.loc[index, "Área_num"] = value_from_html
                                area_keys_counter += 1
                            elif key_cleaned_from_html in expected_html_cols: # Usar expected_html_cols para la verificación
                                gdf.loc[index, key_cleaned_from_html] = value_from_html
            except Exception as e_parse_html: print(f"⚠️ Error parseando HTML para fila GDF {index}: {e_parse_html}")
else:
    print(f"⚠️ No se procesó columna '{kml_description_col_cleaned}' para extraer datos HTML (columna no encontrada o GDF vacío después del filtro KML Name).")

# === 3.5 FILTRO PRINCIPAL BASADO EN CONTENIDO HTML SIGNIFICATIVO ===
if not gdf.empty:
    print(f"ジオ Filas en GDF ANTES del filtro por contenido HTML significativo: {len(gdf)}")
    # Crear una condición booleana: True si al menos una de las columnas HTML esperadas tiene un valor no nulo
    # Usar .any(axis=1) para verificar si alguna columna en la lista tiene datos para esa fila
    # Primero, asegurarse que los valores no sean solo cadenas vacías o espacios
    for col in expected_html_cols:
        if col in gdf.columns: # Asegurarse que la columna existe
             # Convertir a string, quitar espacios, y luego reemplazar cadenas vacías con np.nan para que .notna() funcione
            gdf[col] = gdf[col].astype(str).str.strip().replace('', np.nan)

    html_content_filter = gdf[expected_html_cols].notna().any(axis=1)
    gdf = gdf[html_content_filter]
    print(f"✅ Filas en GDF DESPUÉS del filtro por contenido HTML significativo: {len(gdf)}")
    if gdf.empty:
        print(f"❌ CRÍTICO: No quedaron filas después de filtrar por contenido HTML. Verifica el parseo del HTML y el mapeo de campos.")
        exit()
else:
    print("ℹ️ GDF ya estaba vacío antes del filtro por contenido HTML.")


# === 4. VALIDACIÓN Y REPROYECCIÓN DE GEOMETRÍA ===
print(f" Filas en GDF ANTES del filtro de geometría (después del filtro HTML): {len(gdf)}")
gdf = gdf[gdf.geometry.notnull() & gdf.geometry.is_valid & ~gdf.geometry.is_empty]
if gdf.empty: print("❌ No hay datos geométricamente válidos después de todos los filtros. Saliendo."); exit()
print(f"✅ Filas en GDF DESPUÉS del filtro de geometría: {len(gdf)}") 

if gdf.crs is None:
    print("⚠️ CRS no definido en GDF, asumiendo WGS84 (EPSG:4326).")
    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
elif gdf.crs.to_epsg() != 4326:
    print(f"🌐 Reproyectando GDF de {gdf.crs} a EPSG:4326.")
    gdf = gdf.to_crs("EPSG:4326")

print(f"📊 Registros finales válidos para procesar: {len(gdf)}")
# print(f"📐 Tipos de geometría: {gdf.geometry.geom_type.unique()}")

# === 5. OBTENER FEATURE LAYER DE ARCGIS Y VERIFICAR CAMPOS ===
layer_item = gis.content.get(item_id)
if not layer_item: print(f"❌ No se encontró Feature Layer ID: {item_id}"); exit()
if not layer_item.layers: print(f"❌ Ítem {item_id} no tiene capas."); exit()
arcgis_layer = layer_item.layers[0]
print(f"🎯 Feature Layer obtenido: {arcgis_layer.properties.name}")
layer_properties = arcgis_layer.properties
arcgis_layer_field_names = [field['name'] for field in layer_properties.fields]
arcgis_layer_field_types = {field['name']: field['type'] for field in layer_properties.fields}
# print(f"📜 Campos existentes en ArcGIS: {arcgis_layer_field_names}")
if name_target_field_arcgis not in arcgis_layer_field_names: print(f"❌ CRÍTICO: Campo ArcGIS '{name_target_field_arcgis}' NO existe.")
if symbol_id_target_field_arcgis not in arcgis_layer_field_names: print(f"❌ CRÍTICO: Campo ArcGIS '{symbol_id_target_field_arcgis}' NO existe.")
if categoria_target_field_arcgis not in arcgis_layer_field_names: print(f"⚠️ ADVERTENCIA: Campo ArcGIS '{categoria_target_field_arcgis}' NO existe.")


# === 6. ELIMINAR DATOS EXISTENTES DE LA CAPA ARCGIS ===
print("🗑️ Intentando eliminar datos existentes de ArcGIS...")
try:
    if "Delete" in layer_properties.capabilities:
        delete_result = arcgis_layer.delete_features(where="1=1")
        if delete_result and delete_result.get('success', False): print(f"🧹 Datos anteriores eliminados: {delete_result.get('deleteResults', 'OK')}")
        else: print(f"⚠️ No se pudieron eliminar datos o respuesta no exitosa: {delete_result}")
    else: print("⚠️ Capa ArcGIS no soporta eliminación o sin permisos.")
except Exception as e_delete: print(f"❌ Error al eliminar datos: {e_delete}")


# === 7. PREPARAR Y CARGAR NUEVOS DATOS A ARCGIS ===
print("📤 Preparando y cargando nuevos datos a ArcGIS...")
features_to_add_to_arcgis = []
skipped_secondary_filter_count = 0 # Renombrado para claridad

for index, gdf_row in gdf.iterrows(): # gdf ya está pre-filtrado por KML Name y contenido HTML
    arcgis_attributes = {} 
    gdf_row_dict = gdf_row.to_dict() 
    
    # --- 7.1 Extraer y procesar 'Name' y 'SymbolID' del KML Name ---
    kml_name_original_value = "" 
    if kml_name_col_cleaned and kml_name_col_cleaned in gdf_row_dict: 
        # Los valores NaN/None en la columna Name ya deberían haber sido filtrados en 2.5
        # Pero por seguridad, convertir a string y strip.
        kml_name_original_value = str(gdf_row_dict[kml_name_col_cleaned]).strip() 
    
    arcgis_name_processed = kml_name_original_value 
    extracted_numerical_value_for_symbolid = None

    if kml_name_original_value: 
        match = re.search(r'([\d\.]+)$', kml_name_original_value)
        if match:
            number_str_from_kml_name = match.group(1)
            try:
                extracted_numerical_value_for_symbolid = float(number_str_from_kml_name)
                pattern_text_then_separator_then_number = r'^(.*?)\s*–\s*' + re.escape(number_str_from_kml_name) + r'$'
                match_text_part = re.match(pattern_text_then_separator_then_number, kml_name_original_value)
                if match_text_part:
                    arcgis_name_processed = match_text_part.group(1).strip()
                elif kml_name_original_value == number_str_from_kml_name: 
                    arcgis_name_processed = number_str_from_kml_name 
            except ValueError: pass 
    
    if name_target_field_arcgis in arcgis_layer_field_names:
        arcgis_attributes[name_target_field_arcgis] = arcgis_name_processed if arcgis_name_processed.strip() else None
    
    symbol_id_to_assign = DEFAULT_SYMBOL_ID_IF_NOT_FOUND 
    if extracted_numerical_value_for_symbolid is not None:
        target_type = arcgis_layer_field_types.get(symbol_id_target_field_arcgis)
        try:
            if target_type in ['esriFieldTypeInteger', 'esriFieldTypeSmallInteger', 'esriFieldTypeOID']: symbol_id_to_assign = int(extracted_numerical_value_for_symbolid)
            elif target_type in ['esriFieldTypeDouble', 'esriFieldTypeSingle']: symbol_id_to_assign = float(extracted_numerical_value_for_symbolid)
            elif target_type == 'esriFieldTypeString': symbol_id_to_assign = str(extracted_numerical_value_for_symbolid)
            else: symbol_id_to_assign = extracted_numerical_value_for_symbolid 
        except ValueError: print(f"⚠️ No se pudo convertir SymbolID '{extracted_numerical_value_for_symbolid}' a {target_type}.")
    
    if symbol_id_target_field_arcgis in arcgis_layer_field_names:
        arcgis_attributes[symbol_id_target_field_arcgis] = symbol_id_to_assign

    if categoria_target_field_arcgis in arcgis_layer_field_names: 
        arcgis_attributes[categoria_target_field_arcgis] = symbol_id_to_assign 

    # --- 7.2 Mapear campos desde datos del GDF (que incluye los del HTML) ---
    has_any_processed_html_data = False # Renombrado para claridad
    for gdf_col_name_from_html, arcgis_target_field in html_key_to_arcgis_field_map.items():
        if arcgis_target_field in arcgis_layer_field_names: 
            # Los valores en gdf_row_dict para columnas HTML ya fueron convertidos a np.nan si eran vacíos/espacios en 3.5
            if gdf_col_name_from_html in gdf_row_dict and pd.notna(gdf_row_dict[gdf_col_name_from_html]):
                raw_html_value = gdf_row_dict[gdf_col_name_from_html] # Ya no debería ser solo espacios
                target_arcgis_type = arcgis_layer_field_types.get(arcgis_target_field)
                arcgis_field_def = next((f for f in layer_properties.fields if f['name'] == arcgis_target_field), None)
                processed_html_value = None 
                try:
                    # No es necesario verificar 'None', 'None', '' aquí si el paso 3.5 funcionó bien con np.nan
                    if target_arcgis_type in ['esriFieldTypeInteger', 'esriFieldTypeSmallInteger']: 
                        processed_html_value = int(float(str(raw_html_value).replace(",","."))) 
                    elif target_arcgis_type in ['esriFieldTypeDouble', 'esriFieldTypeSingle']: 
                        processed_html_value = float(str(raw_html_value).replace(",","."))
                    elif target_arcgis_type == 'esriFieldTypeString':
                        processed_html_value = str(raw_html_value) # Ya no debería ser solo espacios si 3.5 funcionó
                        if arcgis_field_def and arcgis_field_def.get('length') and len(processed_html_value) > arcgis_field_def['length']: 
                            processed_html_value = processed_html_value[:arcgis_field_def['length']]
                        # No es necesario verificar .strip() aquí si en 3.5 se reemplazó con np.nan
                    else: processed_html_value = raw_html_value 
                except (ValueError, TypeError) as e_conv:
                    print(f"⚠️ No se pudo convertir valor HTML '{raw_html_value}' para ArcGIS '{arcgis_target_field}'. Error: {e_conv}"); processed_html_value = None
                
                arcgis_attributes[arcgis_target_field] = processed_html_value
                if processed_html_value is not None: has_any_processed_html_data = True 

    # --- 7.3 Preparar Geometría ---
    arcgis_geometry = None
    kml_geometry_object = gdf_row_dict.get(gdf.geometry.name) 
    if kml_geometry_object and not kml_geometry_object.is_empty:
        geom_type = kml_geometry_object.geom_type
        try:
            if geom_type == 'Point': arcgis_geometry = {'x': kml_geometry_object.x, 'y': kml_geometry_object.y, 'spatialReference': {'wkid': 4326}}
            elif geom_type == 'LineString': arcgis_geometry = {'paths': [list(kml_geometry_object.coords)], 'spatialReference': {'wkid': 4326}}
            elif geom_type == 'Polygon':
                exterior_coords = list(kml_geometry_object.exterior.coords)
                interior_coords_list = [list(interior.coords) for interior in kml_geometry_object.interiors]
                arcgis_geometry = {'rings': [exterior_coords] + interior_coords_list, 'spatialReference': {'wkid': 4326}}
            elif geom_type == 'MultiPolygon':
                all_rings_for_multipolygon = []
                for poly in kml_geometry_object.geoms: 
                    poly_exterior = list(poly.exterior.coords); poly_interiors = [list(interior.coords) for interior in poly.interiors]
                    all_rings_for_multipolygon.append(poly_exterior); all_rings_for_multipolygon.extend(poly_interiors)
                if all_rings_for_multipolygon: arcgis_geometry = {'rings': all_rings_for_multipolygon, 'spatialReference': {'wkid': 4326}}
            elif geom_type == 'MultiLineString':
                all_paths_for_multilinestring = [list(line.coords) for line in kml_geometry_object.geoms]
                if all_paths_for_multilinestring: arcgis_geometry = {'paths': all_paths_for_multilinestring, 'spatialReference': {'wkid': 4326}}
        except Exception as e_geom: print(f"❌ Error procesando geometría '{geom_type}' fila {index}: {e_geom}")
    
    # --- 7.3.1 Condición SECUNDARIA para OMITIR features predominantemente vacíos ---
    current_arcgis_name_for_check = arcgis_attributes.get(name_target_field_arcgis) 
    is_name_truly_empty = not current_arcgis_name_for_check 
    is_symbol_id_default = symbol_id_to_assign == DEFAULT_SYMBOL_ID_IF_NOT_FOUND

    if arcgis_geometry:
        # El filtro principal ahora es el de la Sección 3.5.
        # Este es un chequeo adicional por si acaso.
        if is_symbol_id_default and is_name_truly_empty and not has_any_processed_html_data:
            skipped_secondary_filter_count += 1
        else:
            if not features_to_add_to_arcgis: 
                 print(f"ジオ Atributos del primer feature A AGREGAR: {json.dumps(arcgis_attributes, indent=2, ensure_ascii=False)}")
            features_to_add_to_arcgis.append({'attributes': arcgis_attributes, 'geometry': arcgis_geometry})

# --- 7.4 Cargar los features preparados a ArcGIS por lotes ---
if skipped_secondary_filter_count > 0:
    print(f"ℹ️ Total de features omitidos por el filtro SECUNDARIO (predominantemente vacíos): {skipped_secondary_filter_count}")

if not features_to_add_to_arcgis: print("⚠️ No se generaron features válidos para agregar a ArcGIS después de todos los filtros.")
else:
    print(f"✅ Preparados {len(features_to_add_to_arcgis)} features para agregar a ArcGIS.")
    chunk_size = 100 
    total_added_successfully = 0; total_failed = 0
    for i in range(0, len(features_to_add_to_arcgis), chunk_size):
        feature_chunk = features_to_add_to_arcgis[i:i + chunk_size]
        print(f"   📨 Enviando lote {i//chunk_size + 1} ({len(feature_chunk)} features)...")
        try:
            add_results = arcgis_layer.edit_features(adds=feature_chunk)
            batch_succeeded = sum(1 for res in add_results.get('addResults', []) if res.get('success'))
            batch_failed = len(feature_chunk) - batch_succeeded
            if add_results and add_results.get('addResults'):
                 for res_idx, res_val in enumerate(add_results['addResults']):
                    if not res_val.get('success'):
                        print(f"      ❌ Error en feature (índice en lote {res_idx}): {res_val.get('error', 'Error desconocido')}")
            elif not add_results.get('addResults'): 
                 print(f"      ❌ Respuesta inesperada del API para el lote: {add_results}"); batch_failed = len(feature_chunk)
            total_added_successfully += batch_succeeded; total_failed += batch_failed
            print(f"   Lote {i//chunk_size + 1} procesado. Exitosos: {batch_succeeded}, Fallidos: {batch_failed}")
        except Exception as e_chunk_add:
            print(f"   💥 Error crítico al agregar lote {i//chunk_size + 1}: {e_chunk_add}"); total_failed += len(feature_chunk) 
    print(f"🏁 Proceso de carga finalizado. Total Agregados: {total_added_successfully}, Total Fallidos: {total_failed}")

print("✅ Script completado.")

