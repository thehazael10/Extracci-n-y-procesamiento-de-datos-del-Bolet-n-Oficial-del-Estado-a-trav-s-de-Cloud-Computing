import json
import boto3
from datetime import datetime, timedelta

# Inicializar cliente S3 y SNS
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Configuración de S3 y SNS
S3_BUCKET = 'buckettestdescargas'
SUMARIOS_PREFIX = 'Descargas_sumarios/'
CONTENIDOS_PREFIX = 'Descargas_contenidos/'
PROCESADOS_PREFIX = 'Contenidos_procesados/'
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:175583258912:Notificaciones_descarga' #Sustituir por el ARN específico si se desea adaptar

# Función para calcular el espacio utilizado en S3 en GB
def calcular_espacio_utilizado_s3():
    total_size = 0
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']
    
    return total_size / (1024 ** 3)  # Convertir a GB

# Función para eliminar archivos de S3
def eliminar_archivos_s3(dias_semana):
    archivos_eliminados = []
    errores = []

    for dia in dias_semana:
        try:
            # Eliminar archivos de sumarios
            sumario_key = f"{SUMARIOS_PREFIX}sumario_{dia}.xml"
            s3.delete_object(Bucket=S3_BUCKET, Key=sumario_key)
            archivos_eliminados.append(sumario_key)

            # Eliminar archivos de contenidos
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{CONTENIDOS_PREFIX}{dia}")

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3.delete_object(Bucket=S3_BUCKET, Key=obj['Key'])
                        archivos_eliminados.append(obj['Key'])
        except Exception as e:
            errores.append(f"Error eliminando archivos de {dia}: {str(e)}")

    return archivos_eliminados, errores

# Función para verificar si los archivos procesados existen
def verificar_archivos_procesados(dias_semana):
    archivos_procesados = []

    for dia in dias_semana:
        try:
            # Formato del nombre del archivo procesado
            procesado_key = f"{PROCESADOS_PREFIX}contenidos_unificados_{dia[:4]}-{dia[4:6]}-{dia[6:]}.json"
            s3.head_object(Bucket=S3_BUCKET, Key=procesado_key)
            archivos_procesados.append(procesado_key)
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                continue
            else:
                raise

    return archivos_procesados

# Función principal Lambda
def lambda_handler(event, context):
    # Obtener la fecha de hoy y las fechas de los días de la semana (Lunes a Domingo)
    hoy = datetime.now()
    lunes = hoy - timedelta(days=hoy.weekday())  # Obtener el lunes de la semana actual
    dias_semana = [(lunes + timedelta(days=i)).strftime('%Y%m%d') for i in range(7)]

    # Calcular el espacio inicial
    espacio_inicial = calcular_espacio_utilizado_s3()

    # Verificar si existen los archivos procesados
    archivos_procesados = verificar_archivos_procesados(dias_semana)

    if not archivos_procesados:
        return {
            'statusCode': 200,
            'body': 'No se encontraron archivos procesados de esta semana.'
        }

    # Eliminar los archivos de sumarios y contenidos correspondientes a los días de la semana
    archivos_eliminados, errores = eliminar_archivos_s3(dias_semana)

    # Calcular el espacio final después de la eliminación
    espacio_final = calcular_espacio_utilizado_s3()
    espacio_liberado = espacio_inicial - espacio_final

    # Preparar el mensaje del correo electrónico
    mensaje = f"Resultados de la limpieza de la semana:\n\n"

    if archivos_eliminados:
        mensaje += "Archivos eliminados:\n" + "\n".join(archivos_eliminados) + "\n\n"
    else:
        mensaje += "No se eliminaron archivos.\n\n"

    if errores:
        mensaje += "Errores encontrados:\n" + "\n".join(errores) + "\n\n"

    mensaje += f"Espacio inicial: {espacio_inicial:.2f} GB\n"
    mensaje += f"Espacio final: {espacio_final:.2f} GB\n"
    mensaje += f"Espacio liberado: {espacio_liberado:.2f} GB\n"

    # Enviar notificación por correo electrónico
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject='Resultados de limpieza de datos de la semana',
        Message=mensaje
    )

    return {
        'statusCode': 200,
        'body': 'Limpieza de datos completada y correo enviado.'
    }
