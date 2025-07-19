import requests

# URL del endpoint
url = "https://localhost:44302/api/energy-measurement/pull"

# Parámetros de la solicitud
pullParams = {
    "Id": None,
    "ViewName": "",
    "SearchString": None,
    "PageSize": 25,
    "PageNumber": 1,
    "SortAscending": True,
    "SortBy": ""
}

# Realizar la solicitud GET
try:
    response = requests.get(url, params=pullParams, verify=False)  # 'params' para incluir los parámetros en la URL

    if response.status_code == 200:
        print("Respuesta exitosa:", response.json()["returnCode"], response.json()["list"])
    else:
        print(f"Error: {response.status_code} - {response.text}")

except requests.RequestException as e:
    print(f"Ocurrió un error al realizar la solicitud: {e}")
