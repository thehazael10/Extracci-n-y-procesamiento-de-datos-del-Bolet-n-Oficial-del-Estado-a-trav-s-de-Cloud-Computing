import os
from datetime import datetime, timedelta

# Configuración de rutas
ruta_sumarios = "/media/hazael/E250574350571E1B/BOE/Descargas sumarios"
ruta_contenidos = "/media/hazael/E250574350571E1B/BOE/Descargas de Contenidos"
ruta_procesados = "/media/hazael/E250574350571E1B/BOE/Contenidos procesados"

# Función para obtener las fechas de la semana (lunes a domingo)
def obtener_fechas_semana():
    hoy = datetime.now()
    lunes = hoy - timedelta(days=hoy.weekday())  # Obtener el lunes de esta semana
    fechas_semana = [(lunes + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]  # De lunes a domingo
    return fechas_semana

# Función para listar archivos en la carpeta de sumarios (para diagnóstico)
def listar_archivos_sumarios():
    archivos = os.listdir(ruta_sumarios)
    print("Archivos en la carpeta de sumarios:")
    for archivo in archivos:
        print(archivo)

# Función para buscar archivos JSON en la carpeta de procesados
def buscar_archivos_json(fechas):
    archivos_encontrados = []
    for fecha in fechas:
        archivo_json = f"contenido_unificado_{fecha}.json"
        ruta_json = os.path.join(ruta_procesados, archivo_json)
        if os.path.exists(ruta_json):
            archivos_encontrados.append(ruta_json)
    return archivos_encontrados

# Función para buscar archivos XML de sumarios y contenidos correspondientes a las fechas
def listar_archivos_para_eliminar(fechas):
    archivos_a_eliminar = []
    
    for fecha in fechas:
        # Buscar archivos de sumarios (formato corregido a sumario_YYYY-MM-DD.xml)
        archivo_sumario = os.path.join(ruta_sumarios, f"sumario_{fecha}.xml")
        print(f"Buscando archivo de sumario: {archivo_sumario}")
        if os.path.exists(archivo_sumario):
            archivos_a_eliminar.append(archivo_sumario)
        else:
            print(f"Archivo de sumario no encontrado: {archivo_sumario}")
        
        # Buscar archivos de contenidos
        archivos_contenidos = [f for f in os.listdir(ruta_contenidos) if f.startswith(fecha) and f.endswith(".xml")]
        for archivo in archivos_contenidos:
            ruta_archivo = os.path.join(ruta_contenidos, archivo)
            archivos_a_eliminar.append(ruta_archivo)

    return archivos_a_eliminar

# Función para eliminar archivos
def eliminar_archivos(archivos):
    for archivo in archivos:
        if os.path.exists(archivo):
            try:
                os.remove(archivo)
                print(f"El archivo {archivo} ha sido eliminado con éxito.")
            except Exception as e:
                print(f"Error al intentar eliminar el archivo {archivo}: {e}")
        else:
            print(f"El archivo {archivo} no existe.")

# Función principal para gestionar la limpieza
def gestionar_limpieza():
    # Listar archivos en la carpeta de sumarios para diagnóstico
    listar_archivos_sumarios()

    # Obtener las fechas de la semana
    fechas_semana = obtener_fechas_semana()
    print(f"Fechas de la semana: {fechas_semana}")

    # Buscar archivos JSON en la carpeta de procesados
    archivos_json = buscar_archivos_json(fechas_semana)
    if archivos_json:
        print(f"Archivos JSON encontrados:\n{archivos_json}")
        
        # Listar archivos para eliminar
        archivos_para_eliminar = listar_archivos_para_eliminar(fechas_semana)
        print(f"Archivos a eliminar:\n{archivos_para_eliminar}")
        
        # Eliminar los archivos
        eliminar_archivos(archivos_para_eliminar)
    else:
        print("No se encontraron archivos transformados para realizar la limpieza.")

# Ejecutar la limpieza
if __name__ == "__main__":
    gestionar_limpieza()