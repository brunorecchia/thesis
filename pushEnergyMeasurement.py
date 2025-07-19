import requests

# URL del endpoint
url = "https://localhost:44302/api/energy-measurement/push"

#https://apex-server-dev.azurewebsites.net

# EnergyMeasurement
energyMeasurement = {
    "MonitorId": 2,
    "PFaverage": 0.80,
    "TotalIntegralActivePower": 1000,
    "TotalIntegralReactivePower": 1000,
    "Current_1": 10.123,
    "Current_2": 11.456,
    "Current_3": 12.789,
    "Frequency_1": 50.0,
    "Frequency_2": 50.1,
    "Frequency_3": 50.2,
    "Voltage_1": 220.0,
    "Voltage_2": 221.0,
    "Voltage_3": 219.0
}

energyMeasurement2 = {
    "MonitorId": 2,
    "PFaverage": 0.80,
    "TotalIntegralActivePower": 1000,
    "TotalIntegralReactivePower": 1000,
    "Current_1": 10.123,
    "Current_2": 11.456,
    "Current_3": 12.789,
    "Frequency_1": 50.0,
    "Frequency_2": 50.1,
    "Frequency_3": 50.2,
    "Voltage_1": 220.0,
    "Voltage_2": 221.0,
    "Voltage_3": 219.0
}

# Datos del cuerpo de la solicitud
inserts = {
    "List": [
        energyMeasurement,
        energyMeasurement2
    ]
}

# Realizar la solicitud POST
try:
    response = requests.post(url, json=inserts, verify=False)  # 'json' para incluir los parámetros en el cuerpo

    if response.status_code == 200:
        print("Respuesta exitosa:", response.json())
    else:
        print(f"Error: {response.status_code} - {response.text}")

except requests.RequestException as e:
    print(f"Ocurrió un error al realizar la solicitud: {e}")
