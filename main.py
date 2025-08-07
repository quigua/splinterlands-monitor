import os
import json
import time
import requests
from binascii import hexlify
from beem.message import sign_message
from datetime import datetime, timezone
import sqlite3
import subprocess
import logging

# Importamos nuestro nuevo módulo de base de datos
import database

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/mnt/ssd/Splinterlands_Services/desarrollo.log"),
        logging.StreamHandler()
    ]
)


# --- Configuración ---
API_BASE_URL = "https://api.splinterlands.com"

PENDING_REQUESTS_FILE = "/mnt/ssd/Splinterlands_Services/pending_requests.json"

def load_pending_requests():
    try:
        with open(PENDING_REQUESTS_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_pending_requests(requests_list):
    with open(PENDING_REQUESTS_FILE, 'w') as f:
        json.dump(requests_list, f, indent=2)

# --- Funciones de API ---

def compute_signature(string_to_sign, private_key):
    bytestring_signature = sign_message(string_to_sign, private_key)
    hex_signature = hexlify(bytestring_signature).decode("ascii")
    return hex_signature

def login_to_splinterlands(username, posting_key):
    logging.info("Iniciando login en Splinterlands...")
    ts = int(time.time() * 1000)
    message = f"{username}{ts}"
    signature = compute_signature(message, posting_key)
    login_endpoint = f"{API_BASE_URL}/players/login?name={username}&ts={ts}&sig={signature}"
    
    try:
        response = requests.get(login_endpoint, timeout=30)
        response.raise_for_status()
        login_data = response.json()
        if login_data.get('name') == username and 'token' in login_data:
            logging.info("Login exitoso.")
            return login_data['name'], login_data['token']
        else:
            logging.error(f"Login fallido. Respuesta inesperada: {login_data}")
            return None, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error durante el proceso de login: {e}")
        return None, None

def get_player_battle_history(player, auth_user, auth_token):
    """
    Obtiene las últimas 50 batallas de un jugador, con manejo de límites de tasa.
    """
    endpoint = f"{API_BASE_URL}/battle/history?player={player}"
    auth_params = {'username': auth_user, 'token': auth_token}
    
    retries = 0
    max_retries = 5
    initial_sleep = 1 # seconds
    
    while retries < max_retries:
        logging.info(f"Consultando historial para: {player} (Intento {retries + 1}/{max_retries})")
        try:
            response = requests.get(endpoint, params=auth_params, timeout=30)
            response.raise_for_status() # Esto lanzará una excepción para códigos de error HTTP (4xx, 5xx)

            # La API puede devolver 'no battles' que no es JSON
            if not response.text or 'no battles' in response.text:
                logging.info(f"No se encontraron batallas para {player} en la API.")
                return []
            
            battles_data = response.json().get('battles', [])
            logging.info(f"API devolvió {len(battles_data)} batallas para {player}.")
            return battles_data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429: # Too Many Requests
                retry_after = e.response.headers.get('Retry-After')
                sleep_time = initial_sleep * (2 ** retries) # Exponential backoff
                
                if retry_after:
                    try:
                        sleep_time = int(retry_after)
                    except ValueError:
                        pass # Fallback to exponential if Retry-After is not a valid int
                
                logging.warning(f"Límite de tasa de API alcanzado para {player} (HTTP 429). Esperando {sleep_time:.2f} segundos antes de reintentar.")
                time.sleep(min(sleep_time, 60)) # Cap sleep time at 60 seconds
                retries += 1
            else:
                logging.error(f"Error HTTP al obtener historial de {player}: {e}")
                return [] # Other HTTP errors are not retried
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Error de conexión al obtener historial de {player}: {e}")
            # For connection errors, also apply a small backoff before retrying
            time.sleep(initial_sleep * (2 ** retries))
            retries += 1
        
        except json.JSONDecodeError as e:
            logging.error(f"Error de decodificación JSON para {player}: {e}. Respuesta: {response.text[:200]}...")
            return [] # JSON errors are not retried

    logging.error(f"Falló la obtención del historial de {player} después de {max_retries} reintentos debido a límites de tasa o errores de conexión.")
    return []


# --- Lógica Principal del Monitor ---

if __name__ == "__main__":

    logging.info("Iniciando el monitor de batallas de Splinterlands...")
    
    hive_username = os.getenv("HIVE_USERNAME")
    hive_posting_key = os.getenv("HIVE_POSTING_KEY")

    if not hive_username or not hive_posting_key:
        logging.error("¡Error! Las variables de entorno HIVE_USERNAME y HIVE_POSTING_KEY no están definidas.")
        exit()

    user, token = login_to_splinterlands(hive_username, hive_posting_key)
    if not user or not token:
        logging.error("No se pudo iniciar sesión. Abortando.")
        exit()

    # --- NUEVO: Inicialización de Conexiones ---
    players_db_conn = None
    raw_battles_conn = None
    try:
        logging.info("Conectando a la base de datos de jugadores...")
        players_db_conn = database.get_players_db_connection()
        if not players_db_conn:
            logging.error("No se pudo conectar a la base de datos de jugadores. Abortando.")
            exit()
        logging.info("Base de datos de jugadores conectada.")

        logging.info("Conectando a la base de datos de batallas crudas...")
        raw_battles_conn = database.get_raw_battles_db_connection()
        if not raw_battles_conn:
            logging.error("No se pudo conectar a la base de datos de batallas crudas. Abortando.")
            exit()
        logging.info("Base de datos de batallas crudas conectada.")

        logging.info("Inicializando tablas...")
        database.initialize_players_table(players_db_conn)
        database.initialize_raw_battles_table(raw_battles_conn)
        logging.info("Tablas inicializadas.")

        if database.get_total_players(players_db_conn) == 0:
            logging.info(f"Base de datos de jugadores vacía. Añadiendo usuario inicial: {hive_username}")
            database.add_or_update_players_batch(players_db_conn, [hive_username])
            logging.info(f"Usuario inicial '{hive_username}' añadido a la base de datos de jugadores.")

        logging.info(f"Iniciando escaneo con {database.get_total_players(players_db_conn)} jugadores registrados...")

        # --- Bucle Principal ---
        while True:
            pending_requests = load_pending_requests()
            priority_players_names = [req['target_username'] for req in pending_requests if req.get('status') == "DETECTED" and req.get('target_username')]
            
            current_player = None
            if priority_players_names:
                current_player = database.get_priority_player_to_scan(players_db_conn, priority_players_names)
                if current_player:
                    logging.info(f"Priorizando escaneo para el jugador: {current_player} (solicitud pendiente).")

            if not current_player:
                current_player = database.get_player_to_scan(players_db_conn)
                if not current_player:
                    logging.info("No hay jugadores para escanear que cumplan el criterio de tiempo. Esperando...")
                    time.sleep(0.5)
                    continue

            logging.info(f"Procesando jugador: {current_player}")
            battles = get_player_battle_history(current_player, user, token)

            if not battles:
                logging.info(f"No se encontraron batallas para {current_player} en la API.")
            else:
                logging.info(f"Procesando {len(battles)} batallas de {current_player}...")
                players_to_add_update = set()
                battles_to_insert = []

                for battle in battles:
                    battle_id = battle.get('battle_queue_id_1')
                    if not battle_id:
                        logging.warning(f"Batalla sin battle_queue_id_1, saltando: {json.dumps(battle)}")
                        continue
                    
                    battles_to_insert.append(battle)
                    
                    player_1 = battle.get('player_1')
                    player_2 = battle.get('player_2')
                    if player_1: players_to_add_update.add(player_1)
                    if player_2: players_to_add_update.add(player_2)
                
                if battles_to_insert:
                    # --- MODIFICADO: Usar la conexión existente ---
                    database.insert_raw_battles_batch(raw_battles_conn, battles_to_insert)
                    logging.info(f"Batch inserted {len(battles_to_insert)} raw battles for {current_player}.")
                
                if players_to_add_update:
                    database.add_or_update_players_batch(players_db_conn, list(players_to_add_update))
                    logging.info(f"Batch updated {len(players_to_add_update)} players for {current_player}.")

            logging.info(f"Ciclo para {current_player} completado. Total de jugadores registrados: {database.get_total_players(players_db_conn)}")
            database.add_or_update_players_batch(players_db_conn, [current_player])
            logging.info(f"Timestamp para {current_player} actualizado.")

            if current_player in priority_players_names:
                for req in pending_requests:
                    if req.get('target_username') == current_player and req.get('status') == "DETECTED":
                        req['status'] = "READY_FOR_PROCESSING"
                        save_pending_requests(pending_requests)
                        logging.info(f"Solicitud para {current_player} marcada como READY_FOR_PROCESSING.")
                        break
            
            time.sleep(0.5)

    except KeyboardInterrupt:
        logging.info("Proceso interrumpido por el usuario.")
    except Exception as e:
        logging.error(f"Error inesperado en el bucle principal: {e}", exc_info=True)
    finally:
        # --- NUEVO: Cierre Seguro de Conexiones ---
        logging.info("Cerrando conexiones a la base de datos...")
        if players_db_conn:
            players_db_conn.close()
            logging.info("Conexión a la base de datos de jugadores cerrada.")
        if raw_battles_conn:
            raw_battles_conn.close()
            logging.info("Conexión a la base de datos de batallas crudas cerrada.")
        logging.info("Proceso de escaneo completado.")
