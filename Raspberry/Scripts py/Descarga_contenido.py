import os
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Directorio donde tienes los archivos XML de los sumarios
directorio_sumarios = '/media/hazael/E250574350571E1B/BOE/Descargas sumarios'

# Directorio donde se guardarán las descargas
directorio_descargas = '/media/hazael/E250574350571E1B/BOE/Descargas de Contenidos'

# Crear una sesión de requests
session = requests.Session()

def descargar_publicacion(enlace_xml, fecha_sumario):
    # Asegurarse de que la URL tiene el dominio completo
    if not enlace_xml.startswith("https://www.boe.es"):
        enlace_xml = "https://www.boe.es" + enlace_xml

    # Crear el nombre del archivo con la fecha del sumario
    nombre_archivo_xml = f"{fecha_sumario}_{enlace_xml.split('=')[-1]}.xml"
    ruta_guardado = os.path.join(directorio_descargas, nombre_archivo_xml)

    # Comprobar si ya existe el archivo
    if os.path.exists(ruta_guardado):
        print(f"{nombre_archivo_xml} ya existe, omitiendo descarga.")
        return

    # Descargar la publicación en XML
    try:
        print(f"Descargando publicación desde {enlace_xml} hacia {ruta_guardado}")
        contenido_publicacion_xml = session.get(enlace_xml).content

        with open(ruta_guardado, 'wb') as pub_file:
            pub_file.write(contenido_publicacion_xml)
            print(f"Descargado: {nombre_archivo_xml}")
    except Exception as e:
        print(f"Error descargando {enlace_xml}: {e}")

def procesar_archivo_sumario(ruta_archivo):
    # Extraer la fecha del nombre del archivo de sumario
    nombre_archivo = os.path.basename(ruta_archivo)
    fecha_sumario = nombre_archivo.split('_')[1].split('.')[0]  # Asumiendo formato sumario_YYYY-MM-DD.xml
    print(f"Leyendo archivo de sumario: {ruta_archivo} correspondiente a la fecha {fecha_sumario}")

    with open(ruta_archivo, 'r', encoding='utf-8') as file:
        soup_sumario = BeautifulSoup(file, "xml")
        
        # Extraer los enlaces de las publicaciones en XML
        items = soup_sumario.find_all('item')
        print(f"Encontradas {len(items)} publicaciones en {ruta_archivo}")

        # Obtener todos los enlaces XML
        enlaces_xml = [item.find('urlXml').text for item in items]

        # Usar ThreadPoolExecutor para paralelizar las descargas
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(lambda enlace: descargar_publicacion(enlace, fecha_sumario), enlaces_xml)

def descargar_contenidos_hoy():
    # Obtener la fecha de hoy en formato YYYY-MM-DD
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    archivo_sumario_hoy = f"sumario_{fecha_hoy}.xml"
    ruta_archivo = os.path.join(directorio_sumarios, archivo_sumario_hoy)

    # Comprobar si el archivo de sumario de hoy existe
    if os.path.exists(ruta_archivo):
        procesar_archivo_sumario(ruta_archivo)
    else:
        print(f"No se encontró el sumario para la fecha de hoy: {fecha_hoy}")

if __name__ == "__main__":
    descargar_contenidos_hoy()
    print("Descarga de publicaciones XML completada para el día de hoy.")

