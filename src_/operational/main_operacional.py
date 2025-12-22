import requests
import os
import datetime
import pandas as pd
import pytz
from datetime import timedelta

# --- Configuración ---
# ID de tu Google Sheet
SHEET_ID = "1YzhR91pyj-gCmv9DWhot7Y9nkBPdhnag13HzlhADsSU"

# gid de la pestaña número 3 (reemplaza con el valor real)
GID_PESTAÑA_3 = "1508274810"

# URL para exportar como CSV
EXPORT_URL_TEMPLATE = (
    "https://docs.google.com/spreadsheets/d/{sheet_id}"
    "/export?format=csv&gid={gid}"
)

# Carpeta donde se guardarán los archivos
LOCAL_DOWNLOAD_PATH = (
    "E:\PRACTICAS_2025_1\EMERGENCIA\datos"
)

# Encabezado para simular un navegador
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}
# --- Fin Configuración ---
 
def descargar_y_procesar_pestana_csv():
    """
    Descarga la pestaña 3 de la Google Sheet como CSV,
    elimina las primeras 4 filas y las filas que tengan 7 o más columnas vacías,
    y guarda la versión procesada con codificación UTF-8.
    """
    # Fecha actual para nombrar los archivos
    hoy = datetime.date.today()
    fecha_str = hoy.strftime("%Y-%m-%d")  # e.g. "2025-05-19"

    # Construir la URL de exportación
    url_csv = EXPORT_URL_TEMPLATE.format(
        sheet_id=SHEET_ID,
        gid=GID_PESTAÑA_3
    )

    # Asegurar que exista la carpeta destino
    if not os.path.exists(LOCAL_DOWNLOAD_PATH):
        os.makedirs(LOCAL_DOWNLOAD_PATH)
        print(f"Directorio creado: {LOCAL_DOWNLOAD_PATH}")

    # Nombre del archivo bruto (sin procesar)
    archivo_bruto = os.path.join(
        LOCAL_DOWNLOAD_PATH,
        f"sheet3_bruto_{fecha_str}.csv"
    )

    print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Descargando CSV desde: {url_csv}")
    try:
        resp = requests.get(
            url_csv, headers=HEADERS, stream=True, timeout=60
        )
        resp.raise_for_status()
        with open(archivo_bruto, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Archivo bruto guardado: {archivo_bruto}")

        # Procesar el CSV con pandas: eliminar primeras 4 filas
        df = pd.read_csv(
            archivo_bruto,
            skiprows=4,
            encoding='utf-8',
            engine='python'
        )

        # ++++ NUEVO: Eliminar filas con 7 o más columnas vacías ++++
        umbral_vacios = 20
        filas_a_eliminar = df[df.isnull().sum(axis=1) >= umbral_vacios].index
        df.drop(filas_a_eliminar, inplace=True)
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Filas después de eliminar con {umbral_vacios} o más vacíos: {len(df)}")
        
        # Asegúrate de que las columnas 'Fecha' y 'Hora' existan en el DataFrame
        if 'Fecha' in df.columns and 'Hora (00:00)' in df.columns:
            df['Fecha'] = df['Fecha'].astype(str)
            df['Hora (00:00)'] = df['Hora (00:00)'].astype(str)
            df['Fecha'] = (pd.to_datetime(df['Fecha'] + ' ' + df['Hora (00:00)'], errors='coerce')) + pd.Timedelta(hours=5)	

	   
        else:
            print("Las columnas 'Fecha' y/o 'Hora' no se encuentran en el DataFrame.")

        # Guardar archivo procesado
        archivo_procesado = os.path.join(
            LOCAL_DOWNLOAD_PATH,
            f"sheet3_procesado_{fecha_str}.csv"
        )
        df.to_csv(archivo_procesado, index=False, encoding='utf-8') # Asegurar codificación UTF-8 al guardar
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Archivo procesado guardado: {archivo_procesado}")

    except requests.exceptions.Timeout:
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Error: Tiempo de espera agotado.")
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Error en la solicitud: {e}")
    except Exception as e:
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Error inesperado: {e}")

if __name__ == "__main__":
    descargar_y_procesar_pestana_csv()
    print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] Proceso de descarga y procesamiento finalizado.")
    from arcgis.gis import GIS
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point
import numpy as np
import datetime
hoy = datetime.date.today()
fecha_str = hoy.strftime("%Y-%m-%d")
# === CONFIGURACIÓN ===
# Login
gis = GIS("https://www.arcgis.com", "unidad.gestion.riesgo", "XXXXXX")  # Reemplaza con tus credenciales

# ID del Feature Layer en ArcGIS Online
item_id = "13e98e0ed80c4f45b0d5a8d4f742ef72"  # Reemplaza con el ID de tu item

# Ruta local al archivo CSV
nombre_csv = f"sheet3_procesado_{fecha_str}.csv" # Reemplaza con tu nombre de archivo
ruta_csv = os.path.join("E:\PRACTICAS_2025_1\EMERGENCIA\datos", nombre_csv)

# Campos de coordenadas en el CSV (reemplaza con los nombres correctos)
csv_longitude_field = "Longitud"   # Campo que contiene la longitud (X)
csv_latitude_field = "Latitud"     # Campo que contiene la latitud (Y)

# === 1. VERIFICAR Y LEER ARCHIVO CSV ===
if not os.path.exists(ruta_csv):
    print(f"❌ No se encontró el archivo CSV: {ruta_csv}")
    exit()

print(f"📄 Leyendo archivo CSV: {ruta_csv}")
try:
    # Leer CSV
    try:
        df = pd.read_csv(ruta_csv, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(ruta_csv, encoding='latin1')
        except UnicodeDecodeError:
            df = pd.read_csv(ruta_csv, encoding='ISO-8859-1')

    # ++++ NUEVO: Mapeo de nombres de columnas del CSV a los campos de ArcGIS ++++
    column_mapping = {
        "Hora (00:00)": "Hora__00_00_",
        "Tipo de Alerta": "Tipo_de_Alerta",
        "Municipio": "Municipio",
        "Estación de Nivel Asociada N°1": "Estación_de_Nivel_Asociada_N_1",
        "Estación de Nivel Asociada N°2": "Estación_de_Nivel_Asociada_N_2",
        "Latitud": "Latitud",
        "Longitud": "Longitud",
        "Código Estación de Nivel Asociada N°1": "Código_Estación_de_Nivel_Asocia",
        "Código Estación de Nivel Asociada N°2": "Código_Estación_de_Nivel_Asoc_1",
        "Códigos Estaciones\xa0": "Códigos_Estaciones_Asociadas",
        "SATC": "SATC",
        "Institución": "Institución",
        "Canal": "Canal",
        "Mensaje_Retroalimentación_Tiempo_Pasado": "Mensaje___Retroalimentación__Ti",
        "Persona_contactada_o_que_contactó": "Persona_contactada_o_que_contac",
        "¿Se activó sirena?": "F_Se_activó_sirena_",
        "Código_Sirena_Asociada": "Código_Sirena_Asociada",
        "La comunidad o el organismo de gestión de riesgo respondieron la llamada": "La_comunidad_o_el_organismo_de_",
        "La sirena sonó?": "La_sirena_sonó_",
        "Responsable de hacer recibir la interacción": "Responsable_de_hacer_recibir_la",
        "Verificado": "Verificado",
        "Evento": "Evento",
        "Año": "Año",
        "Mes": "Mes",
        "Día": "Día",
        "Fecha": "Fecha",
        "Unnamed_25": "Unnamed__25",
        "Unnamed_26": "Unnamed__26",
        "Unnamed_28": "Unnamed__28",
        # Añade aquí el resto de tus columnas y su correspondiente nombre en ArcGIS Online si faltan
    }

    # Renombrar las columnas del DataFrame para que coincidan con los campos de ArcGIS
    df = df.rename(columns=column_mapping)

    print(f"📋 Columnas después de renombrar: {df.columns.tolist()}")

except Exception as e_csv:
    print(f"❌ Error fatal al leer CSV: {e_csv}")
    exit()

if df.empty:
    print("❌ El archivo CSV está vacío o no se pudo leer correctamente.")
    exit()

print(f"📊 Filas en DataFrame después de leer CSV: {len(df)}")
print(f"📋 Columnas en el CSV: {df.columns.tolist()}")

# === 2. FILTRAR REGISTROS ===
# Convertir comas a puntos y manejar vacíos
df[csv_longitude_field] = (
    df[csv_longitude_field]
    .astype(str)
    .str.replace(',', '.')
    .str.strip()
    .replace(['', 'nan', 'None'], 0)
    .astype(float)
)

df[csv_latitude_field] = (
    df[csv_latitude_field]
    .astype(str)
    .str.replace(',', '.')
    .str.strip()
    .replace(['', 'nan', 'None'],0)
    .astype(float)
)

# Mantener filas aunque alguna coordenada falte (pero al menos una debe estar presente)
df = df[~df[[csv_longitude_field, csv_latitude_field]].isnull().all(axis=1)]  # Filtrar solo si AMBAS son nulas
print(f"📊 Filas válidas: {len(df)}")

# === 3. GEODATAFRAME ===
geometry = [
    Point(xy) if not np.isnan(xy[0]) and not np.isnan(xy[1]) else None
    for xy in zip(df[csv_longitude_field], df[csv_latitude_field])
]
gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

# === 4. OBTENER FEATURE LAYER DE ARCGIS ===
layer_item = gis.content.get(item_id)
if not layer_item:
    print(f"❌ No se encontró Feature Layer ID: {item_id}")
    exit()
if not layer_item.layers:
    print(f"❌ Ítem {item_id} no tiene capas.")
    exit()

arcgis_layer = layer_item.layers[0]
print(f"🎯 Feature Layer obtenido: {arcgis_layer.properties.name}")
layer_properties = arcgis_layer.properties
arcgis_field_names = [field['name'] for field in layer_properties.fields]
print(f"📜 Campos existentes en ArcGIS: {arcgis_field_names}")

# === 5. ELIMINAR DATOS EXISTENTES DE LA CAPA ARCGIS ===
print("🗑️ Intentando eliminar datos existentes de ArcGIS...")
try:
    if "Delete" in layer_properties.capabilities:
        delete_result = arcgis_layer.delete_features(where="1=1")
        if delete_result and delete_result.get('success', False):
            print(f"🧹 Datos anteriores eliminados: {delete_result.get('deleteResults', 'OK')}")
        else:
            print(f"⚠️ No se pudieron eliminar datos o respuesta no exitosa: {delete_result}")
    else:
        print("⚠️ Capa ArcGIS no soporta eliminación o sin permisos.")
except Exception as e_delete:
    print(f"❌ Error al eliminar datos: {e_delete}")

# === 6. PREPARAR Y CARGAR NUEVOS DATOS A ARCGIS ===
features_to_add_to_arcgis = []

# Obtener los tipos de datos de los campos de ArcGIS para la conversión
arcgis_field_types = {field['name']: field['type'] for field in layer_properties.fields}
arcgis_field_lengths = {field['name']: field.get('length') for field in layer_properties.fields if field.get('length') is not None}

# Incluir TODOS los campos del DataFrame (ahora con los nombres correctos de ArcGIS)
csv_fields = [col for col in df.columns if col not in [csv_longitude_field, csv_latitude_field]]

for index, row in gdf.iterrows():
    attributes = {}
    for field in csv_fields:  # Incluir todos los campos del DataFrame (renombrados)
        value = row[field]
        if pd.notna(value):
            # Convertir tipos de datos según corresponda
            if field in arcgis_field_types:
                field_type = arcgis_field_types[field]
                try:
                    if field_type in ['esriFieldTypeInteger', 'esriFieldTypeSmallInteger']:
                        attributes[field] = int(float(value)) if value not in ['', ' '] else None
                    elif field_type in ['esriFieldTypeDouble', 'esriFieldTypeSingle']:
                        attributes[field] = float(value) if value not in ['', ' '] else None
                    elif field_type == 'esriFieldTypeString':
                        attributes[field] = str(value)[:arcgis_field_lengths.get(field, 255)]
                    elif field_type == 'esriFieldTypeDate':
                        # Intentar convertir a timestamp (milisegundos desde epoch)
                        if isinstance(value, (int, float)):
                            attributes[field] = int(value)
                        else:
                            try:
                                # Probar diferentes formatos de fecha
                                date_obj = pd.to_datetime(value, errors='raise')
                                attributes[field] = int(date_obj.timestamp() * 1000)
                            except ValueError:
                                attributes[field] = None # Si no se puede parsear, asignar None
                    else:
                        attributes[field] = value
                except:
                    attributes[field] = None
            else:
                # Si el campo no existe en ArcGIS, se ignora (opcional: imprimir advertencia)
                pass

    # Manejar geometría (incluso si está vacía)
    geometry = None
    if not row.geometry.is_empty:
        geometry = {'x': row.geometry.x, 'y': row.geometry.y, 'spatialReference': {'wkid': 4326}}

    features_to_add_to_arcgis.append({'attributes': attributes, 'geometry': geometry})

# === 7. CARGAR FEATURES A ARCGIS POR LOTES ===
if not features_to_add_to_arcgis:
    print("⚠️ No hay features válidos para agregar.")
else:
    chunk_size = 100  # Reducir tamaño de lote para evitar timeouts
    total_added = 0
    total_failed = 0

    for i in range(0, len(features_to_add_to_arcgis), chunk_size):
        feature_chunk = features_to_add_to_arcgis[i:i + chunk_size]
        print(f"📦 Enviando lote {i//chunk_size + 1} ({len(feature_chunk)} features)...")

        try:
            add_results = arcgis_layer.edit_features(adds=feature_chunk)
            if not add_results.get('addResults'):
                print(f"❌ Error en el lote: Respuesta inválida de ArcGIS.")
                total_failed += len(feature_chunk)
                continue

            # Contar éxitos y errores
            succeeded = sum(1 for res in add_results['addResults'] if res.get('success', False))
            failed = len(feature_chunk) - succeeded
            total_added += succeeded
            total_failed += failed

            # Mostrar errores detallados
            for res in add_results['addResults']:
                if not res.get('success'):
                    print(f"  ⚠️ Error en feature ID {res.get('objectId', '?')}: {res.get('error', 'Sin detalles')}")

        except Exception as e:
            print(f"🔥 Error crítico en el lote: {str(e)}")
            total_failed += len(feature_chunk)

    print(f"✅ Total cargado: {total_added} | ❌ Fallidos: {total_failed}")

print("✅ Script completado.")

import requests
import os
import datetime
import pandas as pd

# --- Configuración ---
# ID de tu Google Sheet
tabla_id = "1YzhR91pyj-gCmv9DWhot7Y9nkBPdhnag13HzlhADsSU"
# gid de la nueva pestaña (reemplaza con el valor real)
gid_nueva_pestana = "834984775"
# Carpeta donde se guardará el archivo descargado
target_folder = r"E:\PRACTICAS_2025_1\EMERGENCIA\datos"
# Nombre base para el archivo
date_str = datetime.date.today().strftime("%Y-%m-%d")
# URL plantilla para exportar CSV
export_url = (
    f"https://docs.google.com/spreadsheets/d/{tabla_id}"
    f"/export?format=csv&gid={gid_nueva_pestana}"
)
# Encabezado para simular navegador
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# --- Función de descarga y procesamiento ---
def descargar_y_procesar_pestana_nueva():
    os.makedirs(target_folder, exist_ok=True)

    # Descarga
    raw_path = os.path.join(target_folder, f"Aumentos_{date_str}.csv")
    print(f"Descargando nueva pestaña desde: {export_url}")
    response = requests.get(export_url, headers=headers, timeout=60)
    response.raise_for_status()
    with open(raw_path, "wb") as f:
        f.write(response.content)
    print(f"Archivo bruto guardado en: {raw_path}")

    # Procesamiento: combinar Fecha y Hora (00:00)
    df = pd.read_csv(raw_path, engine='python', encoding='utf-8')
    if 'Fecha' in df.columns and 'Hora (00:00)' in df.columns:
        # Convertir a string y unir
        df['Fecha'] = (pd.to_datetime(
            df['Fecha'].astype(str) + ' ' + df['Hora (00:00)'].astype(str),
            errors='coerce'
        ))  + pd.Timedelta(hours=5)	

        # Opcional: eliminar columna de hora original
        df.drop(columns=['Hora (00:00)'], inplace=True)
        print(f"Columnas combinadas: 'Fecha' ahora incluye hora, 'Hora (00:00)' eliminada.")
    else:
        print("Columnas 'Fecha' y/o 'Hora (00:00)' no encontradas. No se realizó combinación.")

    # Guardar CSV procesado
    processed_path = os.path.join(target_folder, f"Aumentos_{date_str}.csv")
    df.to_csv(processed_path, index=False, encoding='utf-8')
    print(f"Archivo procesado guardado en: {processed_path}")

if __name__ == "__main__":
    descargar_y_procesar_pestana_nueva()
from arcgis.gis import GIS
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point
import numpy as np
import datetime
hoy = datetime.date.today()
fecha_str = hoy.strftime("%Y-%m-%d")
# === CONFIGURACIÓN ===
# Login
gis = GIS("https://www.arcgis.com", "unidad.gestion.riesgo", "SIGAMVA2024")  # Reemplaza con tus credenciales

# ID del Feature Layer en ArcGIS Online
item_id = "9b7bf773686848bc8bb41402098573e2"  # Reemplaza con el ID de tu item

# Ruta local al archivo CSV
nombre_csv = f"Aumentos_{fecha_str}.csv" # Reemplaza con tu nombre de archivo
ruta_csv = os.path.join("E:\PRACTICAS_2025_1\EMERGENCIA\datos", nombre_csv)

# Campos de coordenadas en el CSV (reemplaza con los nombres correctos)
csv_longitude_field = "Longitud"   # Campo que contiene la longitud (X)
csv_latitude_field = "Latitud"     # Campo que contiene la latitud (Y)

# === 1. VERIFICAR Y LEER ARCHIVO CSV ===
if not os.path.exists(ruta_csv):
    print(f"❌ No se encontró el archivo CSV: {ruta_csv}")
    exit()

print(f"📄 Leyendo archivo CSV: {ruta_csv}")
try:
    # Leer CSV
    try:
        df = pd.read_csv(ruta_csv, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(ruta_csv, encoding='latin1')
        except UnicodeDecodeError:
            df = pd.read_csv(ruta_csv, encoding='ISO-8859-1')

    # ++++ NUEVO: Mapeo de nombres de columnas del CSV a los campos de ArcGIS ++++
    column_mapping = {
        "Código Estación": "Código_Estación",
        "Nombre Estación": "Nombre_Estación",
        "Aumento": "Aumento",
        "Latitud": "Latitud",
        "Longitud": "Longitud",
        "Año": "Año",
        "Mes": "Mes",
        "Día": "Día",
        "Fecha": "Fecha",
        # Añade aquí el resto de tus columnas y su correspondiente nombre en ArcGIS Online si faltan
    }

    # Renombrar las columnas del DataFrame para que coincidan con los campos de ArcGIS
    df = df.rename(columns=column_mapping)

    print(f"📋 Columnas después de renombrar: {df.columns.tolist()}")

except Exception as e_csv:
    print(f"❌ Error fatal al leer CSV: {e_csv}")
    exit()

if df.empty:
    print("❌ El archivo CSV está vacío o no se pudo leer correctamente.")
    exit()

print(f"📊 Filas en DataFrame después de leer CSV: {len(df)}")
print(f"📋 Columnas en el CSV: {df.columns.tolist()}")

# === 2. FILTRAR REGISTROS ===
# Convertir comas a puntos y manejar vacíos
df[csv_longitude_field] = (
    df[csv_longitude_field]
    .astype(str)
    .str.replace(',', '.')
    .str.strip()
    .replace(['', 'nan', 'None'], 0)
    .astype(float)
)

df[csv_latitude_field] = (
    df[csv_latitude_field]
    .astype(str)
    .str.replace(',', '.')
    .str.strip()
    .replace(['', 'nan', 'None'],0)
    .astype(float)
)

# Mantener filas aunque alguna coordenada falte (pero al menos una debe estar presente)
df = df[~df[[csv_longitude_field, csv_latitude_field]].isnull().all(axis=1)]  # Filtrar solo si AMBAS son nulas
print(f"📊 Filas válidas: {len(df)}")

# === 3. GEODATAFRAME ===
geometry = [
    Point(xy) if not np.isnan(xy[0]) and not np.isnan(xy[1]) else None
    for xy in zip(df[csv_longitude_field], df[csv_latitude_field])
]
gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

# === 4. OBTENER FEATURE LAYER DE ARCGIS ===
layer_item = gis.content.get(item_id)
if not layer_item:
    print(f"❌ No se encontró Feature Layer ID: {item_id}")
    exit()
if not layer_item.layers:
    print(f"❌ Ítem {item_id} no tiene capas.")
    exit()

arcgis_layer = layer_item.layers[0]
print(f"🎯 Feature Layer obtenido: {arcgis_layer.properties.name}")
layer_properties = arcgis_layer.properties
arcgis_field_names = [field['name'] for field in layer_properties.fields]
print(f"📜 Campos existentes en ArcGIS: {arcgis_field_names}")

# === 5. ELIMINAR DATOS EXISTENTES DE LA CAPA ARCGIS ===
print("🗑️ Intentando eliminar datos existentes de ArcGIS...")
try:
    if "Delete" in layer_properties.capabilities:
        delete_result = arcgis_layer.delete_features(where="1=1")
        if delete_result and delete_result.get('success', False):
            print(f"🧹 Datos anteriores eliminados: {delete_result.get('deleteResults', 'OK')}")
        else:
            print(f"⚠️ No se pudieron eliminar datos o respuesta no exitosa: {delete_result}")
    else:
        print("⚠️ Capa ArcGIS no soporta eliminación o sin permisos.")
except Exception as e_delete:
    print(f"❌ Error al eliminar datos: {e_delete}")

# === 6. PREPARAR Y CARGAR NUEVOS DATOS A ARCGIS ===
features_to_add_to_arcgis = []

# Obtener los tipos de datos de los campos de ArcGIS para la conversión
arcgis_field_types = {field['name']: field['type'] for field in layer_properties.fields}
arcgis_field_lengths = {field['name']: field.get('length') for field in layer_properties.fields if field.get('length') is not None}

# Incluir TODOS los campos del DataFrame (ahora con los nombres correctos de ArcGIS)
csv_fields = [col for col in df.columns if col not in [csv_longitude_field, csv_latitude_field]]

for index, row in gdf.iterrows():
    attributes = {}
    for field in csv_fields:  # Incluir todos los campos del DataFrame (renombrados)
        value = row[field]
        if pd.notna(value):
            # Convertir tipos de datos según corresponda
            if field in arcgis_field_types:
                field_type = arcgis_field_types[field]
                try:
                    if field_type in ['esriFieldTypeInteger', 'esriFieldTypeSmallInteger']:
                        attributes[field] = int(float(value)) if value not in ['', ' '] else None
                    elif field_type in ['esriFieldTypeDouble', 'esriFieldTypeSingle']:
                        attributes[field] = float(value) if value not in ['', ' '] else None
                    elif field_type == 'esriFieldTypeString':
                        attributes[field] = str(value)[:arcgis_field_lengths.get(field, 255)]
                    elif field_type == 'esriFieldTypeDate':
                        # Intentar convertir a timestamp (milisegundos desde epoch)
                        if isinstance(value, (int, float)):
                            attributes[field] = int(value)
                        else:
                            try:
                                # Probar diferentes formatos de fecha
                                date_obj = pd.to_datetime(value, errors='raise')
                                attributes[field] = int(date_obj.timestamp() * 1000)
                            except ValueError:
                                attributes[field] = None # Si no se puede parsear, asignar None
                    else:
                        attributes[field] = value
                except:
                    attributes[field] = None
            else:
                # Si el campo no existe en ArcGIS, se ignora (opcional: imprimir advertencia)
                pass

    # Manejar geometría (incluso si está vacía)
    geometry = None
    if not row.geometry.is_empty:
        geometry = {'x': row.geometry.x, 'y': row.geometry.y, 'spatialReference': {'wkid': 4326}}

    features_to_add_to_arcgis.append({'attributes': attributes, 'geometry': geometry})

# === 7. CARGAR FEATURES A ARCGIS POR LOTES ===
if not features_to_add_to_arcgis:
    print("⚠️ No hay features válidos para agregar.")
else:
    chunk_size = 100  # Reducir tamaño de lote para evitar timeouts
    total_added = 0
    total_failed = 0

    for i in range(0, len(features_to_add_to_arcgis), chunk_size):
        feature_chunk = features_to_add_to_arcgis[i:i + chunk_size]
        print(f"📦 Enviando lote {i//chunk_size + 1} ({len(feature_chunk)} features)...")

        try:
            add_results = arcgis_layer.edit_features(adds=feature_chunk)
            if not add_results.get('addResults'):
                print(f"❌ Error en el lote: Respuesta inválida de ArcGIS.")
                total_failed += len(feature_chunk)
                continue

            # Contar éxitos y errores
            succeeded = sum(1 for res in add_results['addResults'] if res.get('success', False))
            failed = len(feature_chunk) - succeeded
            total_added += succeeded
            total_failed += failed

            # Mostrar errores detallados
            for res in add_results['addResults']:
                if not res.get('success'):
                    print(f"  ⚠️ Error en feature ID {res.get('objectId', '?')}: {res.get('error', 'Sin detalles')}")

        except Exception as e:
            print(f"🔥 Error crítico en el lote: {str(e)}")
            total_failed += len(feature_chunk)

    print(f"✅ Total cargado: {total_added} | ❌ Fallidos: {total_failed}")


print("✅ Script completado.")
