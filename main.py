
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
    print("Iniciando login en Splinterlands...")
    ts = int(time.time() * 1000)
    message = f"{username}{ts}"
    signature = compute_signature(message, posting_key)
    login_endpoint = f"{API_BASE_URL}/players/login?name={username}&ts={ts}&sig={signature}"
    
    try:
        response = requests.get(login_endpoint, timeout=30)
        response.raise_for_status()
        login_data = response.json()
        if login_data.get('name') == username and 'token' in login_data:
            print("Login exitoso.")
            return login_data['name'], login_data['token']
        else:
            print("Login fallido. Respuesta inesperada:", login_data)
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Error durante el proceso de login: {e}")
        return None, None

def get_player_battle_history(player, auth_user, auth_token):
    """Obtiene las últimas 50 batallas de un jugador."""
    endpoint = f"{API_BASE_URL}/battle/history?player={player}"
    auth_params = {'username': auth_user, 'token': auth_token}
    
    logging.info(f"Consultando historial para: {player}")
    try:
        response = requests.get(endpoint, params=auth_params, timeout=30)
        response.raise_for_status()
        # La API puede devolver 'no battles' que no es JSON
        if not response.text or 'no battles' in response.text:
            logging.info(f"No se encontraron batallas para {player} en la API.")
            return []
        battles_data = response.json().get('battles', [])
        logging.info(f"API devolvió {len(battles_data)} batallas para {player}.")
        return battles_data
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        logging.error(f"Error al obtener historial de {player}: {e}")
        return []



# --- Lógica Principal del Monitor ---


if __name__ == "__main__":

    logging.info("Iniciando el monitor de batallas de Splinterlands...")
    
    hive_username = os.getenv("HIVE_USERNAME")
    hive_posting_key = os.getenv("HIVE_POSTING_KEY")

    if not hive_username or not hive_posting_key:
        logging.error("¡Error! Las variables de entorno HIVE_USERNAME y HIVE_POSTING_KEY no están definidas.")
        exit()

    # Autenticación inicial
    user, token = login_to_splinterlands(hive_username, hive_posting_key)
    if not user or not token:
        logging.error("No se pudo iniciar sesión. Abortando.")
        exit()

    # Conectar a la base de datos de jugadores...
    logging.info("Conectando a la base de datos de jugadores...")
    players_db_conn = database.get_players_db_connection()
    if not players_db_conn:
        logging.error("No se pudo conectar a la base de datos de jugadores. Abortando.")
        exit()
    logging.info("Base de datos de jugadores conectada.")

    logging.info("Inicializando tabla de jugadores...")
    database.initialize_players_table(players_db_conn)
    logging.info("Tabla de jugadores inicializada.")

    # Conectar a la base de datos de batallas crudas...
    logging.info("Conectando a la base de datos de batallas crudas...")
    raw_battles_db_conn = database.get_raw_battles_db_connection()
    if not raw_battles_db_conn:
        logging.error("No se pudo conectar a la base de datos de batallas crudas. Abortando.")
        exit()
    logging.info("Base de datos de batallas crudas conectada.")

    logging.info("Inicializando tabla de batallas crudas...")
    database.initialize_raw_battles_table(raw_battles_db_conn)
    logging.info("Tabla de batallas crudas inicializada.")

    # Conectar a la base de datos de índice de batallas
    logging.info("Conectando a la base de datos de índice de batallas...")
    index_conn = database.get_battle_index_connection()
    if not index_conn:
        logging.error("No se pudo conectar a la base de datos de índice. Abortando.")
        exit()

    # Añadir el usuario inicial si la base de datos de jugadores está vacía
    if database.get_total_players(players_db_conn) == 0:
        logging.info(f"Base de datos de jugadores vacía. Añadiendo usuario inicial: {hive_username}")
        database.add_or_update_player(players_db_conn, hive_username)
        logging.info(f"Usuario inicial '{hive_username}' añadido a la base de datos de jugadores.")

    logging.info(f"Iniciando escaneo con {database.get_total_players(players_db_conn)} jugadores registrados...")

    # Bucle principal del monitor
    while True:

        # 1. Obtener jugadores prioritarios de pending_requests.json
        pending_requests = load_pending_requests()
        priority_players_names = []
        for req in pending_requests:
            if req.get('status') == "DETECTED" and req.get('target_username'):
                priority_players_names.append(req['target_username'])
        
        current_player = None
        if priority_players_names:
            current_player = database.get_priority_player_to_scan(players_db_conn, priority_players_names)
            if current_player:
                logging.info(f"Priorizando escaneo para el jugador: {current_player} (solicitud pendiente).")

        # 2. Si no hay jugadores prioritarios, obtener el siguiente jugador de la cola normal
        if not current_player:
            current_player = database.get_player_to_scan(players_db_conn)
            if not current_player:
                logging.info("No hay jugadores para escanear que cumplan el criterio de tiempo. Esperando...")
                time.sleep(60) # Esperar un minuto antes de reintentar
                continue

        logging.info(f"Procesando jugador: {current_player}")
        battles = get_player_battle_history(current_player, user, token)

        if not battles:
            logging.info(f"No se encontraron batallas para {current_player} en la API.")
        else:
            logging.info(f"Procesando {len(battles)} batallas de {current_player}...")
            try:
                for battle in battles:
                    battle_id = battle.get('battle_queue_id_1')
                    if not battle_id:
                        logging.warning(f"Batalla sin battle_queue_id_1, saltando: {json.dumps(battle)}")
                        continue

                    if database.battle_exists_in_index(index_conn, battle_id):
                        logging.info(f"Batalla {battle_id} ya existe en el índice, saltando inserción en raw_battles.")
                        continue

                    if database.insert_raw_battle(raw_battles_db_conn, battle):
                        logging.info(f"Batalla {battle_id} insertada/actualizada exitosamente en raw_battles.")
                    else:
                        logging.error(f"Fallo al insertar/actualizar batalla {battle_id} en raw_battles.")
                
                raw_battles_db_conn.commit()
                logging.info(f"Cambios para {current_player} guardados en raw_battles.db.")

            except Exception as e:
                logging.error(f"Error inesperado al procesar batallas de {current_player}: {e}")

            # 5. Añadir jugadores nuevos a la base de datos de jugadores
            # Esta parte debe estar fuera del try-except del procesamiento de batallas
            # para que se ejecute incluso si hay errores en la inserción de batallas.
            for battle in battles: # Re-iterar sobre las batallas para añadir jugadores
                player_1 = battle.get('player_1')
                player_2 = battle.get('player_2')
                
                if player_1:
                    database.add_or_update_player(players_db_conn, player_1)
                if player_2:
                    database.add_or_update_player(players_db_conn, player_2)
            
            logging.info(f"Ciclo para {current_player} completado. Total de jugadores registrados: {database.get_total_players(players_db_conn)}")
            # Pausa entre jugadores para ser amigables con la API
            time.sleep(5) # Reintroduce a small delay between processing players
        
        # Actualizar el timestamp del jugador actual después de escanearlo
        logging.info(f"Actualizando timestamp para {current_player}...")
        database.add_or_update_player(players_db_conn, current_player)
        logging.info(f"Timestamp para {current_player} actualizado.")

        # Si el jugador actual fue un jugador prioritario, actualizar el estado de la solicitud
        if current_player in priority_players_names:
            for req in pending_requests:
                if req.get('target_username') == current_player and req.get('status') == "DETECTED":
                    req['status'] = "READY_FOR_PROCESSING"
                    save_pending_requests(pending_requests)
                    logging.info(f"Solicitud para {current_player} marcada como READY_FOR_PROCESSING.")
                    break # Asumimos que solo hay una solicitud DETECTED por jugador prioritario

    # Cerrar la conexión a la base de datos de jugadores al finalizar
    logging.info("Cerrando conexión a la base de datos de jugadores...")
    players_db_conn.close()
    logging.info("Conexión a la base de datos de jugadores cerrada.")
    raw_battles_db_conn.close()
    logging.info("Conexión a la base de datos de batallas crudas cerrada.")
    logging.info("Proceso de escaneo completado.")
