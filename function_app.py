import os
import io
import azure.functions as func
import logging
import json
import random
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

@app.queue_trigger(
        arg_name="azqueue",
        queue_name="requests",
        connection="QueueAzureWebJobsStorage"
        )
def serverlesspokeapi(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]['id']

    update_request(id, "inprogress")
    logger.info(f"La generación del reporte con id: {id} está en progreso.")

    request_info = get_request(id)

    pokemons = get_pokemons(request_info[0]["type"])

    logger.info(f"Se han encontrado los pokemons del tipo: {request_info[0]['type']}")
    logger.info(f"Obteniendo detalles de los pokemons...")

    ## Recorremos cada pokemon y obtenemos sus detalles
    pokemons_complete = [get_pokemon_details(pokemon) for pokemon in pokemons ]

    logger.info(f"Se han obtenido los detalles de los pokemons correctamente.")

    pokemon_bytes = generate_csv_to_blob(pokemons_complete)

    blob_name = f"poke_report_{id}.csv" # Generamos el nombre del archivo
    upload_csv_to_blob( blob_name = blob_name, csv_data=pokemon_bytes)
    logger.info(f"Finalizado!. El reporte {blob_name} se ha generado correctamente.")

    complete_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
    update_request(id, "completed", complete_url)


def update_request(id: int, status: str, url: str = None) -> dict:
    payload = {
        "status": status,
        "id": id
    }

    if url:
        payload["url"] = url

    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    return response.json()

def get_request(id: int) -> dict:
    response = requests.get(f"{DOMAIN}/api/request/{id}")
    return response.json()

def get_pokemons(type: str, sample_size: int = None) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=3000)
    data = response.json()
    pokemon_entries = data.get("pokemon", [])

    if sample_size:
        pokemon_entries = random.sample(pokemon_entries, sample_size)
        logger.info(f"Se obtuvieron {sample_size} pokemons aleatorios del tipo {type}.")

    return [ p["pokemon"] for p in pokemon_entries]

def get_pokemon_details(pokemon: dict) -> dict:
    name = pokemon["name"]
    url = pokemon["url"]
    try:
        response = requests.get(url, timeout=3000)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error obteniendo los detalles del pokemon '{name}': {e}")
        raise

    # Obtenemos las habilidades y las separamos por comas
    abilities = [ability["ability"]["name"] for ability in data.get("abilities", [])]
    abilities_csv = ", ".join(abilities)

    # Obtenemos las estadísticas base como propiedades de un diccionario
    base_stats = {stat["stat"]["name"]: stat["base_stat"] for stat in data.get("stats", [])}

    return {
        **pokemon,
        **base_stats,  # Desempaquetamos las estadísticas base en el diccionario
        "abilities": abilities_csv,
    }

def generate_csv_to_blob(pokemon_list: list) -> bytes:
    df = pd.DataFrame(pokemon_list)
    output = io.StringIO()
    df.to_csv( output, index=False, encoding='utf-8' )
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes

def upload_csv_to_blob( blob_name: str, csv_data: bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
    except Exception as e:
        logger.error(f"Error al subir el archivo {e}")
        raise


@app.queue_trigger(arg_name="azqueue", 
                   queue_name="delete-requests",
                   connection="QueueAzureWebJobsStorage") 
def deletepokereport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]['id']

    update_request(id, "deleting")
    logger.info(f"Request {id} is being deleted.")

    blob_name = f"poke_report_{id}.csv"

    delete_blob_from_storage(blob_name)
    logger.info(f"Blob {blob_name} deleted from storage.")

    delete_request(id)
    logger.info(f"Request {id} deleted from the database.")

def delete_request(id: int) -> dict:
    try:
        response = requests.delete(f"{DOMAIN}/api/request/{id}")
        return response.json()
    except Exception as e:
        logger.error(f"Error al intentar eliminar la solicitud {id}: {e}")
        raise
    
def delete_blob_from_storage(blob_name: str):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
        
        if blob_client.exists():
            blob_client.delete_blob()
            logger.info(f"Archivo '{blob_name}' eliminado correctamente.")
        else:
            logger.warning(f"El archivo '{blob_name}' no existe en el contenedor.")

    except Exception as e:
        logger.error(f"Error al intentar eliminar el archivo '{blob_name}': {e}")
        raise
