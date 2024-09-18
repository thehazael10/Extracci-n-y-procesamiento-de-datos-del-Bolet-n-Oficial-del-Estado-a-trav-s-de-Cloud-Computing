import json
import boto3
import requests
from datetime import datetime

# Inicializar cliente S3
s3 = boto3.client('s3')

# Configuración de S3
S3_BUCKET = 'buckettestdescargas'  # Nombre del bucket de S3
S3_KEY_PREFIX = 'Descargas_sumarios/'  # Prefijo para las claves en S3

def descargar_sumario_boe(fecha):
    """Función para descargar el sumario del BOE usando la API y almacenarlo en S3."""
    # Construir la URL de la API del BOE con la fecha proporcionada
    url = f"https://www.boe.es/datosabiertos/api/boe/sumario/{fecha}"
    
    try:
        # Realizar la solicitud GET
        response = requests.get(url, headers={"Accept": "application/xml"})
        response.raise_for_status()  # Lanza una excepción si la solicitud falla

        # Verificar si el contenido es XML
        if 'xml' not in response.headers.get('Content-Type', ''):
            raise Exception(f"El contenido descargado no es XML. Contenido: {response.content[:200]}")

        # Nombre del archivo en S3 basado en la fecha
        nombre_archivo = f"sumario_{fecha}.xml"
        s3_key = f"{S3_KEY_PREFIX}{nombre_archivo}"

        # Subir el archivo XML al bucket de S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=response.content,
            ContentType='application/xml'
        )

        return f"Sumario {nombre_archivo} subido correctamente a {S3_BUCKET}/{s3_key}"
    
    except requests.exceptions.RequestException as e:
        return f"Error al descargar el sumario: {str(e)}"

def lambda_handler(event, context):
    """Función Lambda principal."""
    # Obtener la fecha actual en formato YYYYMMDD o usar una fecha del evento
    fecha = event.get('fecha', datetime.now().strftime('%Y%m%d'))

    # Llamar a la función para descargar el sumario
    resultado = descargar_sumario_boe(fecha)
    
    return {
        'statusCode': 200,
        'body': json.dumps(resultado)
    }
 
 