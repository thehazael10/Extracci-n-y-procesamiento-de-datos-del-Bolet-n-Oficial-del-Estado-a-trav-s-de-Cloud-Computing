import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# Configuración de rutas
ruta_sumarios = "/media/hazael/E250574350571E1B/BOE/Descargas sumarios"
ruta_contenidos = "/media/hazael/E250574350571E1B/BOE/Descargas de Contenidos"
ruta_procesados = "/media/hazael/E250574350571E1B/BOE/Contenidos procesados"

# Configuración del correo
remitente = "" # Insertar correo remitente.
destinatario = "" # Insertar correo destinatario.
contraseña = ""  # Insertar contraseña creada.

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

# Función para obtener las fechas de la semana (lunes a domingo)
def obtener_fechas_semana():
    hoy = datetime.now()
    lunes = hoy - timedelta(days=hoy.weekday())  # Obtener el lunes de esta semana
    fechas_semana = [(lunes + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]  # De lunes a domingo
    return fechas_semana

# Función para verificar archivos transformados
def verificar_archivos_transformados(fechas):
    resultados = []
    dias_semana = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

    for i, fecha in enumerate(fechas):
        # Verificar si existe el archivo transformado
        archivo_transformado = os.path.join(ruta_procesados, f"contenido_unificado_{fecha}.json")
        archivo_sumario = os.path.join(ruta_sumarios, f"sumario_{fecha}.xml")
        archivos_contenidos = [f for f in os.listdir(ruta_contenidos) if fecha in f and f.endswith(".xml")]

        if os.path.exists(archivo_transformado):
            # Si existen el sumario o archivos de contenido
            if os.path.exists(archivo_sumario) or archivos_contenidos:
                estado = "Archivos encontrados y no eliminados"
            else:
                estado = "Archivos encontrados y eliminados"
        else:
            estado = "Archivo no encontrado"
        
        resultados.append(f"{dias_semana[i]} ({fecha}): {estado}")
    
    return resultados

# Función para enviar el resumen de limpieza por correo
def enviar_resumen_limpieza():
    fechas_semana = obtener_fechas_semana()
    resumen = verificar_archivos_transformados(fechas_semana)

    # Preparar el correo con el resumen
    asunto = f"Resumen de la limpieza - {datetime.now().strftime('%Y-%m-%d')}"
    mensaje = f"Hola,\n\nEste es el resumen de la limpieza realizada:\n\n" + "\n".join(resumen) + "\n\nSaludos."
    enviar_correo(asunto, mensaje)

# Ejecutar el proceso de notificación
if __name__ == "__main__":
    enviar_resumen_limpieza()