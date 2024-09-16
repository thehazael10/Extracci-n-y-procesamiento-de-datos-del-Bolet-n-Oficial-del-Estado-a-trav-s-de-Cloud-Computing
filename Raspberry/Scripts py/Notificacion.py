import os
import smtplib
import shutil
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Configuración de rutas
ruta_sumarios = "/media/hazael/E250574350571E1B/BOE/Descargas sumarios"
ruta_contenidos = "/media/hazael/E250574350571E1B/BOE/Descargas de Contenidos"
ruta_procesados = "/media/hazael/E250574350571E1B/BOE/Contenidos procesados"
archivo_estado = "/tmp/estado_correo_boe.txt"  # Archivo de estado para rastrear si ya se envió el correo
ruta_ssd = "/media/hazael/E250574350571E1B"  # Ruta del SSD donde queremos verificar la memoria

# Obtener la fecha de hoy para construir los nombres de los archivos
hoy = datetime.now().strftime("%Y-%m-%d")
archivo_sumario = f"sumario_{hoy}.xml"  # Cambiado para asegurarnos de que coincide con el formato correcto
archivo_json = os.path.join(ruta_procesados, f"contenido_unificado_{hoy}.json")

# Configuración del correo
remitente = "" # Insertar correo remitente
destinatario = "" # Insertar correo destinatario
contraseña = ""  # Insertar la contraseña creada para ese correo

# Función para enviar correo
def enviar_correo(asunto, mensaje_cuerpo):
    mensaje = MIMEMultipart()
    mensaje["From"] = remitente
    mensaje["To"] = destinatario
    mensaje["Subject"] = asunto
    
    mensaje.attach(MIMEText(mensaje_cuerpo, "plain"))

    try:
        servidor = smtplib.SMTP("smtp.gmail.com", 587)
        servidor.starttls()
        servidor.login(remitente, contraseña)
        servidor.sendmail(remitente, destinatario, mensaje.as_string())
        print("Correo enviado exitosamente")
    except Exception as e:
        print(f"Error enviando el correo: {e}")
    finally:
        servidor.quit()

# Función para obtener el uso de disco del SSD
def obtener_memoria_disponible():
    uso_disco = shutil.disk_usage(ruta_ssd)
    total = uso_disco.total // (1024 ** 3)  # Convertir a GB
    usado = uso_disco.used // (1024 ** 3)  # Convertir a GB
    libre = uso_disco.free // (1024 ** 3)  # Convertir a GB

    return f"Memoria total: {total} GB\nMemoria usada: {usado} GB\nMemoria libre: {libre} GB"

# Función para verificar si el archivo de sumario existe
def verificar_descarga_sumario():
    return os.path.exists(os.path.join(ruta_sumarios, archivo_sumario))

# Función para verificar si los archivos de contenidos existen
def verificar_descarga_contenidos():
    archivos_contenidos = [f for f in os.listdir(ruta_contenidos) if hoy in f and f.endswith(".xml")]
    return len(archivos_contenidos) > 0

# Función para verificar si el archivo JSON existe
def verificar_transformacion():
    return os.path.exists(archivo_json)

# Función para verificar si ya se envió un correo hoy
def correo_ya_enviado():
    if not os.path.exists(archivo_estado):
        return False
    with open(archivo_estado, "r") as f:
        fecha_ultimo_envio = f.read().strip()
    return fecha_ultimo_envio == datetime.now().strftime("%Y-%m-%d")

# Función para marcar que el correo ya se ha enviado hoy
def marcar_correo_enviado():
    with open(archivo_estado, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))

# Función principal para gestionar la verificación y los correos de alerta
def gestionar_descargas():
    ahora = datetime.now()
    
    # Verificar estado de cada paso
    sumario_descargado = verificar_descarga_sumario()
    contenidos_descargados = verificar_descarga_contenidos()
    transformacion_completada = verificar_transformacion()
    
    # Construir el resumen
    estado_sumario = "Completada" if sumario_descargado else "Rechazada"
    estado_contenidos = "Completada" if contenidos_descargados else "Rechazada"
    estado_transformacion = "Completada" if transformacion_completada else "Rechazada"
    
    memoria_disponible = obtener_memoria_disponible()

    if transformacion_completada and not correo_ya_enviado():
        # Enviar correo con la información de que todo se completó
        mensaje = (
            f"Descarga del sumario: {estado_sumario}\n"
            f"Descarga de los contenidos: {estado_contenidos}\n"
            f"Transformación de los contenidos: {estado_transformacion}\n\n"
            f"Estado de la memoria en el disco (SSD):\n{memoria_disponible}"
        )
        enviar_correo("Proceso completado", mensaje)
        marcar_correo_enviado()

    elif ahora.strftime("%H:%M") == "23:59" and not correo_ya_enviado():
        # Enviar correo de alerta si algo no se completó
        mensaje = (
            f"Descarga del sumario: {estado_sumario}\n"
            f"Descarga de los contenidos: {estado_contenidos}\n"
            f"Transformación de los contenidos: {estado_transformacion}\n\n"
            "Algunos de los procesos no se completaron hoy."
        )
        enviar_correo("Alerta: Proceso incompleto", mensaje)
        marcar_correo_enviado()

# Ejecutar la verificación
if __name__ == "__main__":
    gestionar_descargas()
