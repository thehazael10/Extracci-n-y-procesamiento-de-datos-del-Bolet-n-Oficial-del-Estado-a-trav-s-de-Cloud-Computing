import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

# Ruta al SSD montado
ssd_path = Path("/media/hazael/E250574350571E1B")

# Directorio donde se guardarán los archivos descargados y el log
output_dir = ssd_path / "BOE/Descargas sumarios"
log_dir = ssd_path / "BOE/boe_logs"

# Crear los directorios si no existen
output_dir.mkdir(parents=True, exist_ok=True)
log_dir.mkdir(parents=True, exist_ok=True)

# Generar el nombre del archivo de log con fecha y hora de ejecución
fecha_ejecucion = datetime.now()
log_filename = fecha_ejecucion.strftime(f"log_%Y-%m-%d_%H-%M-%S.txt")
log_filepath = log_dir / log_filename

# Función para escribir en el log
def escribir_log(mensaje):
    with open(log_filepath, "a") as log_file:
        log_file.write(mensaje + "\n")

# Obtener la URL del BOE para una fecha específica
def obtener_url_boe(fecha):
    año = fecha.strftime("%Y")
    mes = fecha.strftime("%m")
    dia = fecha.strftime("%d")
    url = f"https://www.boe.es/boe/dias/{año}/{mes}/{dia}/"
    return url

# Función para descargar y guardar el XML del sumario
def descargar_sumario_xml(url, fecha):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Lanza una excepción si el estado HTTP es 4xx o 5xx
        
        soup = BeautifulSoup(response.text, 'html.parser')
        # Encuentra el enlace al sumario XML de manera general
        enlace_xml = soup.find('a', href=True, title=lambda x: x and 'Sumario' in x)
        
        if (enlace_xml):
            boe_xml_url = "https://www.boe.es" + enlace_xml['href']
            xml_response = requests.get(boe_xml_url, headers=headers)
            xml_response.raise_for_status()
            
            archivo_xml = output_dir / f"sumario_{fecha.strftime('%Y-%m-%d')}.xml"
            with open(archivo_xml, "wb") as f:
                f.write(xml_response.content)
            mensaje = f"Sumario XML de {fecha.strftime('%Y-%m-%d')} ({fecha.strftime('%A')}) guardado en {archivo_xml}"
            print(mensaje)
            escribir_log(mensaje)
        else:
            mensaje = f"No se encontró el enlace al XML del sumario para la fecha {fecha.strftime('%Y-%m-%d')} ({fecha.strftime('%A')})."
            print(mensaje)
            escribir_log(mensaje)
    except requests.exceptions.RequestException as e:
        mensaje = f"Error durante la solicitud HTTP para la fecha {fecha.strftime('%Y-%m-%d')} ({fecha.strftime('%A')}): {e}"
        print(mensaje)
        escribir_log(mensaje)

# Función principal para manejar la descarga del sumario del día actual
def manejar_descarga_sumario_hoy():
    fecha_hoy = datetime.now()
    archivo_xml = output_dir / f"sumario_{fecha_hoy.strftime('%Y-%m-%d')}.xml"

    # Verificar si ya existe el sumario del día actual
    if archivo_xml.exists():
        mensaje = f"El sumario de hoy ({fecha_hoy.strftime('%Y-%m-%d')}) ya ha sido descargado previamente."
        print(mensaje)
        escribir_log(mensaje)
    else:
        url_boe = obtener_url_boe(fecha_hoy)
        descargar_sumario_xml(url_boe, fecha_hoy)

# Ejecutar la función principal
if __name__ == "__main__":
    escribir_log(f"Ejecución iniciada el {fecha_ejecucion.strftime('%Y-%m-%d %H:%M:%S')} ({fecha_ejecucion.strftime('%A')})")
    manejar_descarga_sumario_hoy()
    escribir_log(f"Ejecución finalizada el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ({datetime.now().strftime('%A')})")