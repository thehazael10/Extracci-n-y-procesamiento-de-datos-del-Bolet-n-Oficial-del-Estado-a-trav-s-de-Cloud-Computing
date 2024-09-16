import os
import requests
import json
import xmltodict
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Directorios
directorio_sumarios = '/media/hazael/E250574350571E1B/BOE/Descargas sumarios'
directorio_contenidos = '/media/hazael/E250574350571E1B/BOE/Descargas de Contenidos'
directorio_transformados = '/media/hazael/E250574350571E1B/BOE/Contenidos procesados'

# Asegurarse de que los directorios existen
for directorio in [directorio_sumarios, directorio_contenidos, directorio_transformados]:
    if not os.path.exists(directorio):
        os.makedirs(directorio)

# Función para obtener el sumario de un día específico
def descargar_sumario(fecha):
    url_api = f"https://www.boe.es/diario_boe/xml.php?id=BOE-S-{fecha.strftime('%Y%m%d')}"
    try:
        response = requests.get(url_api)
        response.raise_for_status()
        sumario_xml = response.content
        archivo_sumario = os.path.join(directorio_sumarios, f"sumario_{fecha.strftime('%Y-%m-%d')}.xml")
        with open(archivo_sumario, 'wb') as file:
            file.write(sumario_xml)
        print(f"Sumario descargado para la fecha {fecha.strftime('%Y-%m-%d')}.")
        return archivo_sumario
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar el sumario para la fecha {fecha.strftime('%Y-%m-%d')}: {e}")
        return None

# Función para descargar el contenido XML desde el sumario
def descargar_contenido_xml(enlace_xml):
    # Asegurarse de que el enlace tiene el dominio completo
    if not enlace_xml.startswith("https://www.boe.es"):
        enlace_xml = "https://www.boe.es" + enlace_xml
    try:
        response = requests.get(enlace_xml)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar contenido desde {enlace_xml}: {e}")
        return None

# Función para procesar el sumario y descargar los contenidos en paralelo
def procesar_sumario(fecha, archivo_sumario):
    with open(archivo_sumario, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, "xml")
        items = soup.find_all('item')
        enlaces_xml = [item.find('urlXml').text for item in items if item.find('urlXml')]

        contenidos_xml = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            resultados = executor.map(descargar_contenido_xml, enlaces_xml)

        for resultado in resultados:
            if resultado:
                contenidos_xml.append(resultado)

        return contenidos_xml

# Función para unificar los contenidos XML y convertirlos a JSON
def transformar_y_unificar(fecha, contenidos_xml):
    xml_unificado = "<root>"
    for contenido in contenidos_xml:
        # Eliminar la declaración XML si existe
        contenido_sin_declaracion = contenido.decode('utf-8', errors='ignore').replace('<?xml version="1.0" encoding="UTF-8"?>', '')
        xml_unificado += contenido_sin_declaracion
    xml_unificado += "</root>"

    # Convertir el XML unificado a JSON
    json_data = xmltodict.parse(xml_unificado)

    # Guardar el archivo JSON unificado
    archivo_json = os.path.join(directorio_transformados, f"contenido_unificado_{fecha.strftime('%Y-%m-%d')}.json")
    with open(archivo_json, 'w', encoding='utf-8') as file:
        json.dump(json_data, file, ensure_ascii=False, indent=4)
    
    print(f"Archivo JSON unificado guardado en {archivo_json}")
    return archivo_json

# Función para eliminar los archivos de sumario y contenido XML
def limpiar_archivos(fecha):
    archivo_sumario = os.path.join(directorio_sumarios, f"sumario_{fecha.strftime('%Y-%m-%d')}.xml")
    archivos_contenidos = [f for f in os.listdir(directorio_contenidos) if fecha.strftime('%Y-%m-%d') in f]

    # Eliminar sumario
    if os.path.exists(archivo_sumario):
        os.remove(archivo_sumario)
        print(f"Archivo sumario {archivo_sumario} eliminado.")

    # Eliminar contenidos
    for archivo in archivos_contenidos:
        ruta_archivo = os.path.join(directorio_contenidos, archivo)
        os.remove(ruta_archivo)
        print(f"Archivo de contenido {ruta_archivo} eliminado.")

# Función principal para procesar el rango de fechas
def procesar_rango_fechas(fecha_inicio, fecha_fin):
    fecha_actual = fecha_inicio

    while fecha_actual <= fecha_fin:
        print(f"\nProcesando fecha {fecha_actual.strftime('%Y-%m-%d')}")

        # Paso 1: Descargar sumario
        archivo_sumario = descargar_sumario(fecha_actual)
        if archivo_sumario is None:
            fecha_actual += timedelta(days=1)
            continue

        # Paso 2: Descargar contenidos
        contenidos_xml = procesar_sumario(fecha_actual, archivo_sumario)

        # Paso 3: Unificar y transformar los contenidos a JSON
        if contenidos_xml:
            archivo_json = transformar_y_unificar(fecha_actual, contenidos_xml)

            # Paso 4: Limpiar archivos si el JSON se ha generado correctamente
            if os.path.exists(archivo_json):
                limpiar_archivos(fecha_actual)

        fecha_actual += timedelta(days=1)

# Función para ejecutar el script con fechas de inicio y fin
def ejecutar_proceso():
    # Ingresar las fechas de inicio y fin
    fecha_inicio_str = input("Introduce la fecha de inicio (YYYY-MM-DD): ")
    fecha_fin_str = input("Introduce la fecha de fin (YYYY-MM-DD): ")
    
    # Convertir las fechas de entrada a objetos datetime
    fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-%m-%d')
    fecha_fin = datetime.strptime(fecha_fin_str, '%Y-%m-%d')

    # Verificar que la fecha de inicio no sea mayor que la fecha de fin
    if fecha_inicio > fecha_fin:
        print("La fecha de inicio no puede ser mayor que la fecha de fin.")
        return
    
    # Procesar el rango de fechas
    procesar_rango_fechas(fecha_inicio, fecha_fin)

# Ejecutar el script
if __name__ == "__main__":
    ejecutar_proceso()