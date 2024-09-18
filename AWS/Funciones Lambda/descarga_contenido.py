import os
import boto3
import requests
from lxml import etree
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Inicializar cliente S3
s3 = boto3.client('s3')

# Configuración de S3
S3_BUCKET = 'buckettestdescargas'
SUMARIO_PREFIX = 'Descargas_sumarios/'  # Prefijo de sumarios en S3
CONTENIDOS_PREFIX = 'Descargas_contenidos/'  # Prefijo para los contenidos descargados

# Función para verificar si un archivo ya existe en S3
def archivo_existe_s3(s3_key):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except s3.exceptions.ClientError as e:
        # Si el archivo no existe, retornará un error 404
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

# Función para descargar un contenido y almacenarlo en S3
def descargar_contenido(enlace_xml, fecha_sumario):
    if not enlace_xml.startswith("https://www.boe.es"):
        enlace_xml = "https://www.boe.es" + enlace_xml

    # Crear el nombre del archivo con la fecha del sumario
    nombre_archivo_xml = f"{fecha_sumario}_{enlace_xml.split('=')[-1]}.xml"
    s3_key = f"{CONTENIDOS_PREFIX}{nombre_archivo_xml}"

    # Verificar si el archivo ya existe en S3
    if archivo_existe_s3(s3_key):
        print(f"El archivo {nombre_archivo_xml} ya existe en S3, se omite la descarga.")
        return

    try:
        # Descargar la publicación en XML
        contenido_publicacion_xml = requests.get(enlace_xml).content

        # Subir el archivo XML al bucket de S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=contenido_publicacion_xml,
            ContentType='application/xml'
        )
        print(f"Descargado y subido a S3: {nombre_archivo_xml}")
    except Exception as e:
        print(f"Error al descargar {enlace_xml}: {e}")

# Función para procesar el sumario y descargar los contenidos
def procesar_sumario(fecha_sumario):
    try:
        # Convertir la fecha al formato sin guiones: YYYYMMDD
        fecha_sumario_formateada = fecha_sumario.replace("-", "")
        s3_key = f"{SUMARIO_PREFIX}sumario_{fecha_sumario_formateada}.xml"
        
        # Obtener el archivo de sumario desde S3
        sumario_obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        sumario_xml = sumario_obj['Body'].read().decode('utf-8')

        # Parsear el XML usando lxml.etree
        root = etree.fromstring(sumario_xml.encode('utf-8'))

        # Buscar todos los elementos <url_xml>
        enlaces_xml = root.xpath('//url_xml/text()')

        if not enlaces_xml:
            return f"No se encontraron enlaces XML en el sumario para {fecha_sumario}."

        # Descargar los contenidos en paralelo usando ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=10) as executor:
            for enlace in enlaces_xml:
                executor.submit(descargar_contenido, enlace, fecha_sumario)

        return f"Descargas de contenidos completadas para el sumario {fecha_sumario}."
    
    except Exception as e:
        return f"Error al procesar el sumario {fecha_sumario}: {str(e)}"

# Función Lambda principal
def lambda_handler(event, context):
    # Obtener la fecha del sumario desde el evento (o usar la fecha actual)
    fecha_sumario = event.get('fecha_sumario', datetime.now().strftime('%Y-%m-%d'))
    
    # Llamar a la función para procesar el sumario
    resultado = procesar_sumario(fecha_sumario)
    
    return {
        'statusCode': 200,
        'body': resultado
    }
