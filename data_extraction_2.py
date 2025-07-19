import os
import pandas as pd
import time
import sqlite3
import requests

#Este script se encarga de leer los datos de los archivos CSV, persistir en una Base de datos local, compartir datos vía HTTP a APEX
#incluir Picos PF y alarma

# URL del endpoint
url = "https://apex-server-fmp.azurewebsites.net/api/energy-measurement/push" 

# Configuración de la base de datos SQLite
def crear_base_datos(nombre_bd):
    conexion = sqlite3.connect(nombre_bd)
    cursor = conexion.cursor()
    # Crear la tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lecturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Fecha TEXT,
            hora TEXT,
            PFaverage REAL,
            Totalintegralactivepower REAL,
            Totalintegralreactivepower REAL,
            Current_1 REAL,
            Current_2 REAL,
            Current_3 REAL,
            Frequency_1 REAL,
            Frequency_2 REAL,
            Frequency_3 REAL,
            Voltage_1 REAL,
            Voltage_2 REAL,
            Voltage_3 REAL,
            TotalInstantaneousActivePower REAL                         
        )
    ''')
    conexion.commit()
    conexion.close()

# Función para insertar un registro en la base de datos
def insertar_registro(nombre_bd, Fecha, hora, PF_average, Total_int_act_power, Total_int_react_power,
                      Current_1, Current_2, Current_3, Frequency_1, Frequency_2, Frequency_3,
                      Voltage_1, Voltage_2, Voltage_3, TotalInstantaneousActivePower):
    conexion = sqlite3.connect(nombre_bd)
    cursor = conexion.cursor()
    cursor.execute('''
        INSERT INTO lecturas (
            Fecha, hora, PFaverage, Totalintegralactivepower, Totalintegralreactivepower,
            Current_1, Current_2, Current_3,
            Frequency_1, Frequency_2, Frequency_3,
            Voltage_1, Voltage_2, Voltage_3,
            TotalInstantaneousActivePower
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (Fecha, hora, PF_average, Total_int_act_power, Total_int_react_power,
          Current_1, Current_2, Current_3, Frequency_1, Frequency_2, Frequency_3,
          Voltage_1, Voltage_2, Voltage_3, TotalInstantaneousActivePower))
    conexion.commit()
    conexion.close()

# Función para enviar los datos vía HTTP en formato JSON
def guardar_en_json(Fecha, hora, PF_average, Total_int_act_power, Total_int_react_power,
                    Current_1, Current_2, Current_3, Frequency_1, Frequency_2, Frequency_3,
                    Voltage_1, Voltage_2, Voltage_3, TotalInstantaneousActivePower):
    
    datos = {
        "MonitorId": 1,
        "PFaverage": PF_average,
        "TotalIntegralActivePower": Total_int_act_power,
        "TotalIntegralReactivePower": Total_int_react_power,
        "TotalInstantaneousActivePower": TotalInstantaneousActivePower,
        "Current1": Current_1,
        "Current2": Current_2,
        "Current3": Current_3,
        "Frequency1": Frequency_1,
        "Frequency2": Frequency_2,
        "Frequency3": Frequency_3,
        "Voltage1": Voltage_1,
        "Voltage2": Voltage_2,
        "Voltage3": Voltage_3,
    }

    inserts = {
        "list": [datos]
    }

    try:
        response = requests.post(url, json=inserts)
        if response.status_code == 200:
            print("✅ Respuesta exitosa:", response.json())
        else:
            print(f"❌ Error: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        print(f"⚠️ Error al realizar la solicitud: {e}")

# Función para leer el último registro del penúltimo archivo CSV
def leer_ultimo_registro_penultimo_archivo(carpeta):
    archivos_csv = sorted(
        [archivo for archivo in os.listdir(carpeta) if archivo.endswith('.csv')],
        key=lambda archivo: os.path.getctime(os.path.join(carpeta, archivo)),
        reverse=True
    )   
    
    if len(archivos_csv) < 2:
        print("No hay suficientes archivos .csv en la carpeta para seleccionar el penúltimo.")
        return None

    penultimo_archivo = archivos_csv[1]  # Segundo más reciente

    try:
        ruta_archivo = os.path.join(carpeta, penultimo_archivo)
        df = pd.read_csv(ruta_archivo)

        ultimo_registro = df.tail(1).squeeze()

        Fecha = ultimo_registro[0]
        hora = ultimo_registro[1]
        PF_average = ultimo_registro[2]
        Total_int_act_power = ultimo_registro[3]
        Total_int_react_power = ultimo_registro[4]
        Current_1 = ultimo_registro[5]
        Current_2 = ultimo_registro[6]
        Current_3 = ultimo_registro[7]
        Frequency_1 = ultimo_registro[8]
        Frequency_2 = ultimo_registro[9]
        Frequency_3 = ultimo_registro[10]
        Voltage_1 = ultimo_registro[11]
        Voltage_2 = ultimo_registro[12]
        Voltage_3 = ultimo_registro[13]
        TotalInstantaneousActivePower = ultimo_registro[14]

        print(f"📄 Penúltimo archivo leído exitosamente: {penultimo_archivo}")
        print(f"➡️ Último registro: Fecha: {Fecha}, Hora: {hora}, Total Active Power: {Total_int_act_power}, Frecuencia 1: {Frequency_1}, Voltage 2: {Voltage_2}")

        return (Fecha, hora, PF_average, Total_int_act_power, Total_int_react_power,
                Current_1, Current_2, Current_3, Frequency_1, Frequency_2, Frequency_3,
                Voltage_1, Voltage_2, Voltage_3, TotalInstantaneousActivePower)
    
    except Exception as e:
        print(f"⚠️ Error al leer el archivo {penultimo_archivo}: {e}. Reintentando en 5 segundos...")
        return None

# =========================== LOOP PRINCIPAL ===========================

nombre_bd = 'lecturas.db'
carpeta = 'C:\logs_energia'

crear_base_datos(nombre_bd)
ultimo_archivo_procesado = None

while True:
    archivos_csv = sorted(
        [archivo for archivo in os.listdir(carpeta) if archivo.endswith('.csv')],
        key=lambda archivo: os.path.getctime(os.path.join(carpeta, archivo)),
        reverse=True
    )

    if len(archivos_csv) < 2:
        print("⏳ Esperando nuevos archivos...")
        time.sleep(5)
        continue

    penultimo_archivo = archivos_csv[1]

    if penultimo_archivo != ultimo_archivo_procesado:
        datos = leer_ultimo_registro_penultimo_archivo(carpeta)
        if datos:
            insertar_registro(nombre_bd, *datos)
            guardar_en_json(*datos)
            ultimo_archivo_procesado = penultimo_archivo
        else:
            print("❌ No se pudieron leer los datos del penúltimo archivo.")
    else:
        print("⏳ Sin archivos nuevos para procesar.")

    time.sleep(2)
