import boto3
import os
from datetime import datetime

# Inicializar cliente S3
s3 = boto3.client('s3')

# Configuración de S3
S3_BUCKET = 'buckettestdescargas'
SUMARIO_PREFIX = 'Descargas_sumarios/'
CONTENIDOS_PREFIX = 'Descargas_contenidos/'
PROCESADOS_PREFIX = 'Contenidos_procesados/'
NOTIFICATION_FLAG_PREFIX = 'notificaciones/'

# Función para verificar si ya se envió la notificación para la fecha
def verificar_notificacion_enviada(fecha):
    try:
        s3_key = f"{NOTIFICATION_FLAG_PREFIX}notificacion_{fecha}.txt"
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True  # Notificación ya enviada
    except:
        return False  # No se ha enviado notificación

# Función para marcar que la notificación ya fue enviada
def marcar_notificacion_como_enviada(fecha):
    s3_key = f"{NOTIFICATION_FLAG_PREFIX}notificacion_{fecha}.txt"
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body='Notificación enviada')

# Función para enviar el correo de notificación
def enviar_correo_notificacion(fecha, mensaje):
    sns = boto3.client('sns')
    ARN_TOPIC = 'arn:aws:sns:us-east-1:175583258912:Notificaciones_descarga'
    
    # Enviar notificación por SNS
    sns.publish(
        TopicArn=ARN_TOPIC,
        Subject=f'Estado de las descargas del BOE - {fecha}',
        Message=mensaje
    )

# Función para verificar la existencia de archivos en S3
def archivo_existe_en_s3(s3_key):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except:
        return False

# Función principal de procesamiento y notificación
def lambda_handler(event, context):
    # Obtener la fecha actual o la que viene en el evento
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    
    # Verificar si ya se ha enviado la notificación para hoy
    if verificar_notificacion_enviada(fecha_hoy):
        return {
            'statusCode': 200,
            'body': f'Notificación ya enviada para el día {fecha_hoy}.'
        }
    
    # Verificar la descarga del sumario
    sumario_key = f"{SUMARIO_PREFIX}sumario_{fecha_hoy.replace('-', '')}.xml"
    sumario_descargado = archivo_existe_en_s3(sumario_key)
    
    # Verificar la existencia de los contenidos
    contenidos_descargados = False
    contenido_prefix = f"{CONTENIDOS_PREFIX}{fecha_hoy}"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=contenido_prefix)
    if 'Contents' in response and len(response['Contents']) > 0:
        contenidos_descargados = True

    # Verificar la transformación
    transformado_key = f"{PROCESADOS_PREFIX}contenidos_unificados_{fecha_hoy}.json"
    transformacion_completada = archivo_existe_en_s3(transformado_key)

    # Crear el mensaje de notificación
    mensaje = f"Estado de las descargas del BOE para el día {fecha_hoy}:\n"
    if sumario_descargado:
        mensaje += "- Descarga del sumario: Completada\n"
    else:
        mensaje += "- Descarga del sumario: No completada\n"

    if contenidos_descargados:
        mensaje += "- Descarga de los contenidos: Completada\n"
    else:
        mensaje += "- Descarga de los contenidos: No completada\n"

    if transformacion_completada:
        mensaje += "- Transformación de los contenidos: Completada\n"
    else:
        mensaje += "- Transformación de los contenidos: No completada\n"

    # Determinar si enviar la notificación o no
    if sumario_descargado and contenidos_descargados and transformacion_completada:
        # Todo completado, enviar notificación y marcar como enviada
        enviar_correo_notificacion(fecha_hoy, mensaje)
        marcar_notificacion_como_enviada(fecha_hoy)
        return {
            'statusCode': 200,
            'body': f'Todo el proceso para la fecha {fecha_hoy} se completó correctamente.'
        }
    else:
        # Faltan procesos, esperar a las 22:00
        hora_actual = datetime.now().strftime('%H:%M')
        if hora_actual >= '22:00':
            # Si ya es después de las 22:00, enviar notificación con errores
            enviar_correo_notificacion(fecha_hoy, mensaje)
            marcar_notificacion_como_enviada(fecha_hoy)
            return {
                'statusCode': 200,
                'body': f'Notificación de error enviada para la fecha {fecha_hoy}.'
            }
        else:
            # Aún no es hora de notificar
            return {
                'statusCode': 200,
                'body': 'Proceso en curso, aún no se envió notificación.'
            }
