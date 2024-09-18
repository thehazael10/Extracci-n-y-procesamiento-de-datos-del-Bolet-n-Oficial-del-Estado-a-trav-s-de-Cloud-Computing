import os
import json
import boto3
from lxml import etree
from datetime import datetime

# Inicializar el cliente de S3
s3 = boto3.client('s3')

# Configuración de S3
S3_BUCKET = 'buckettestdescargas'
DESCARGAS_PREFIX = 'Descargas_contenidos/'  # Prefijo de los archivos XML descargados
PROCESADOS_PREFIX = 'Contenidos_procesados/'  # Prefijo para los archivos JSON procesados

# Función para listar todos los archivos XML en la carpeta Descargas_contenidos
def listar_archivos_s3():
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=DESCARGAS_PREFIX)
        archivos = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.xml')]
        return archivos
    except Exception as e:
        print(f"Error al listar archivos en S3: {e}")
        return []

# Función para descargar un archivo XML de S3
def descargar_archivo_s3(s3_key):
    try:
        archivo_obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        contenido_xml = archivo_obj['Body'].read()
        return contenido_xml
    except Exception as e:
        print(f"Error al descargar el archivo {s3_key} de S3: {e}")
        return None

# Función para transformar un único archivo XML en un diccionario JSON
def transformar_xml_a_json(contenido_xml):
    try:
        root = etree.fromstring(contenido_xml)
        json_data = {}

        # Ejemplo básico de transformación de elementos XML a JSON
        for elem in root.iter():
            if elem.tag not in json_data:
                json_data[elem.tag] = []
            json_data[elem.tag].append(elem.text)

        return json_data
    except Exception as e:
        print(f"Error al transformar el archivo XML a JSON: {e}")
        return None

# Función para verificar si un archivo JSON ya existe en S3
def archivo_json_existe(s3_key_json):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key_json)
        return True
    except Exception as e:
        return False

# Función para unificar todos los archivos XML en un único archivo JSON
def unificar_y_transformar_archivos():
    archivos = listar_archivos_s3()

    if not archivos:
        return "No se encontraron archivos XML para procesar."

    # Crear el nombre del archivo JSON
    nombre_archivo_json = f"contenidos_unificados_{datetime.now().strftime('%Y-%m-%d')}.json"
    s3_key_json = f"{PROCESADOS_PREFIX}{nombre_archivo_json}"

    # Verificar si el archivo JSON ya existe en S3
    if archivo_json_existe(s3_key_json):
        return f"El archivo {s3_key_json} ya existe en S3. Abortando operación."

    contenido_unificado = {}

    # Procesar cada archivo XML y unificar los contenidos
    for archivo in archivos:
        contenido_xml = descargar_archivo_s3(archivo)
        if contenido_xml:
            json_data = transformar_xml_a_json(contenido_xml)
            if json_data:
                contenido_unificado[archivo] = json_data

    # Convertir el contenido unificado a JSON
    archivo_json = json.dumps(contenido_unificado, indent=4)

    try:
        # Guardar el archivo JSON en S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key_json,
            Body=archivo_json,
            ContentType='application/json'
        )
        return f"Archivo JSON unificado guardado correctamente en {s3_key_json}."
    except Exception as e:
        return f"Error al guardar el archivo JSON en S3: {e}"

# Función Lambda principal
def lambda_handler(event, context):
    # Llamar a la función de unificación y transformación
    resultado = unificar_y_transformar_archivos()
    
    return {
        'statusCode': 200,
        'body': resultado
    }
