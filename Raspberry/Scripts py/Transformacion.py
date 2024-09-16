import os
import glob
import json
import xmltodict
from datetime import datetime

# Configuración de rutas
ruta_contenidos = "/media/hazael/E250574350571E1B/BOE/Descargas de Contenidos"
ruta_procesados = "/media/hazael/E250574350571E1B/BOE/Contenidos procesados"

# Obtener la fecha de hoy
hoy = datetime.now().strftime("%Y-%m-%d")

# Buscar todos los archivos con el formato YYYY-MM-DD_BOE-[A/B/etc.]-2024-XXXXXX.xml
archivos_xml = glob.glob(f"{ruta_contenidos}/{hoy}_BOE-*.xml")

# Archivo de salida final (JSON)
archivo_salida_json = os.path.join(ruta_procesados, f"contenido_unificado_{hoy}.json")

# Función para unificar los archivos XML y convertirlos directamente a JSON
def unificar_y_convertir_a_json():
    if len(archivos_xml) == 0:
        print("No se encontraron archivos XML para unificar hoy.")
        return

    lista_contenidos = []  # Lista para almacenar los contenidos unificados

    for archivo in archivos_xml:
        try:
            # Leer y convertir el contenido del archivo XML a un diccionario
            with open(archivo, "r", encoding="utf-8") as contenido:
                dict_data = xmltodict.parse(contenido.read())
                lista_contenidos.append(dict_data)
        except Exception as e:
            print(f"Error al procesar {archivo}: {e}")

    # Guardar la lista unificada como archivo JSON
    with open(archivo_salida_json, "w", encoding="utf-8") as archivo_json:
        json.dump(lista_contenidos, archivo_json, indent=4)

    print(f"Archivo JSON unificado generado en: {archivo_salida_json}")

# Ejecutar el proceso de unificación y conversión a JSON
if __name__ == "__main__":
    unificar_y_convertir_a_json()