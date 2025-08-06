import os
import json
import logging
from datetime import datetime, timezone
import sqlite3 # Import sqlite3 directly for batch operations

# Importamos nuestro módulo de base de datos
import database

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/mnt/ssd/Splinterlands_Services/process_raw_battles.log"),
        logging.StreamHandler()
    ]
)

# --- Rutas de Archivos ---
DB_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
RAW_BATTLES_DB = os.path.join(DB_FOLDER, "raw_battles.db")
SEASONS_DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seasons_data.json")

# --- Funciones de Temporada ---
def determine_battle_format(battle, match_type, game_format):
    """
    Determina el formato final de la batalla, manejando el caso de Ranked/null como Wild.
    """
    if match_type == 'Ranked' and (game_format is None or game_format == 'Ranked'):
        return 'wild'
    elif match_type == 'Tournament':
        # Check if 'brawl' is indicated in the settings JSON
        try:
            settings = json.loads(battle.get('settings', '{}'))
            tournament_id = settings.get('tournament_id', '')
            if 'BRAWL' in tournament_id.upper():
                return 'brawl'
        except json.JSONDecodeError:
            logging.warning(f"No se pudo decodificar settings para batalla de torneo {battle.get('battle_queue_id_1')}. Asumiendo formato 'tournament'.")
        return 'tournament' # Default for other tournaments
    elif game_format is None and match_type is not None:
        return match_type.lower()
    return game_format

def load_season_data():
    """
    Carga los datos de las temporadas desde seasons_data.json.
    """
    try:
        with open(SEASONS_DATA_FILE, 'r') as f:
            seasons = json.load(f)
            # Ordenar las temporadas por fecha de fin para facilitar la búsqueda
            seasons.sort(key=lambda x: datetime.fromisoformat(x['ends'].replace('Z', '+00:00')))
            return seasons
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error al cargar los datos de las temporadas desde {SEASONS_DATA_FILE}: {e}")
        return []

def get_season_id_from_date(battle_date_str, seasons_data):
    """
    Determina el ID de la temporada a la que pertenece una batalla
    basándose en su fecha de creación y los datos de las temporadas.
    """
    try:
        battle_date = datetime.fromisoformat(battle_date_str.replace('Z', '+00:00'))

        # Asegurarse de que las temporadas estén ordenadas por fecha de finalización
        # Esto ya se hace en load_season_data, pero es una buena práctica asegurarlo aquí también
        seasons_data.sort(key=lambda x: datetime.fromisoformat(x['ends'].replace('Z', '+00:00')))

        for i, season in enumerate(seasons_data):
            season_end_date = datetime.fromisoformat(season['ends'].replace('Z', '+00:00'))
            
            # La fecha de inicio de la temporada actual es la fecha de fin de la temporada anterior
            # Para la primera temporada, su inicio es el principio de los tiempos (o un valor muy bajo)
            if i == 0:
                season_start_date = datetime.min.replace(tzinfo=battle_date.tzinfo) # Usar el mismo tzinfo
            else:
                prev_season_end_date = datetime.fromisoformat(seasons_data[i-1]['ends'].replace('Z', '+00:00'))
                season_start_date = prev_season_end_date
            
            # Una batalla pertenece a una temporada si su fecha está entre el inicio (exclusivo) y el fin (inclusivo) de la temporada
            # O si es la primera temporada, desde el inicio de los tiempos hasta su fin
            if season_start_date < battle_date <= season_end_date:
                return season['id']
        
        # Si la batalla es posterior a la última temporada conocida en los datos
        if battle_date > datetime.fromisoformat(seasons_data[-1]['ends'].replace('Z', '+00:00')):
            # Podría ser la siguiente temporada o una futura no registrada aún
            # Para evitar asignar a una temporada muy lejana, podemos devolver None o la última conocida + 1
            # Por ahora, devolveremos None y un warning, como en la lógica original
            logging.warning(f"La fecha de batalla {battle_date_str} es posterior a la última temporada registrada. No se pudo determinar la temporada exacta.")
            return None # O seasons_data[-1]['id'] + 1 si se quiere una aproximación

        logging.warning(f"No se pudo determinar la temporada para la batalla con fecha: {battle_date_str}. Podría ser anterior a la primera temporada registrada.")
        return None
    except ValueError as e:
        logging.error(f"Error al parsear la fecha de la batalla '{battle_date_str}': {e}")
        return None

# --- Lógica Principal del Procesador ---
def process_raw_battles():
    logging.info("Iniciando el procesador de batallas crudas...")

    raw_battles_conn = database.get_raw_battles_db_connection()
    if not raw_battles_conn:
        raise Exception("No se pudo conectar a la base de datos de batallas crudas. Abortando.")

    index_conn = database.get_battle_index_connection()
    if not index_conn:
        raw_battles_conn.close()
        raise Exception("No se pudo conectar a la base de datos del índice. Abortando.")

    seasons_data = load_season_data()
    if not seasons_data:
        raw_battles_conn.close()
        raise Exception("No se pudieron cargar los datos de las temporadas. Abortando.")

    cursor = raw_battles_conn.cursor()
    cursor.execute("SELECT battle_id, battle_data FROM raw_battles")
    battles_to_process = cursor.fetchall()
    raw_battles_conn.close() # Close raw_battles_conn early as we have fetched all data

    processed_ids = []
    skipped_count = 0
    
    battles_by_db_destination = {}

    for battle_id, battle_data_json in battles_to_process:
        battle = json.loads(battle_data_json) # This will raise JSONDecodeError if invalid

        created_date = battle.get('created_date')
        match_type = battle.get('match_type')
        game_format = battle.get('format')

        if not created_date:
            logging.warning(f"Batalla {battle_id} no tiene 'created_date'. Saltando.")
            skipped_count += 1
            continue

        season_id = get_season_id_from_date(created_date, seasons_data)
        if season_id is None:
            logging.warning(f"No se pudo determinar la temporada para la batalla {battle_id}. Saltando.")
            skipped_count += 1
            continue
        
        final_format = determine_battle_format(battle, match_type, game_format)

        battle_data_tuple = (
            battle_id,
            battle.get('player_1'),
            battle.get('player_2'),
            battle.get('winner'),
            battle.get('loser'),
            match_type,
            final_format,
            battle.get('mana_cap'),
            battle.get('ruleset'),
            created_date,
            battle.get('player_1_rating_initial'),
            battle.get('player_2_rating_initial'),
            battle.get('player_1_rating_final'),
            battle.get('player_2_rating_final'),
            json.dumps(battle) # Store the original full JSON
        )

        db_key = (season_id, final_format)
        if db_key not in battles_by_db_destination:
            battles_by_db_destination[db_key] = []
        battles_by_db_destination[db_key].append(battle_data_tuple)
        
        processed_ids.append(battle_id)

    # --- Batch insert into structured databases ---
    total_inserted_structured = 0
    for (season_id, final_format), battles_to_insert_batch in battles_by_db_destination.items():
        structured_db_conn = database.get_structured_db_connection(season_id, final_format)
        if not structured_db_conn:
            raise Exception(f"No se pudo conectar a la DB estructurada para Temporada {season_id}, Formato {final_format}. Abortando.")
        
        database.initialize_structured_battle_table(structured_db_conn) # Ensure table exists
        
        cursor = structured_db_conn.cursor()
        cursor.executemany('''
            INSERT OR IGNORE INTO battles (
                battle_id, player_1, player_2, winner, loser, match_type, format,
                mana_cap, ruleset, created_date, player_1_rating_initial,
                player_2_rating_initial, player_1_rating_final, player_2_rating_final,
                full_battle_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', battles_to_insert_batch)
        structured_db_conn.commit() # Commit the batch
        total_inserted_structured += len(battles_to_insert_batch)
        logging.info(f"Lote de {len(battles_to_insert_batch)} batallas insertado en T{season_id}, F:{final_format}.")
        structured_db_conn.close()

    # --- Batch insert into battle index ---
    if processed_ids:
        cursor = index_conn.cursor()
        cursor.executemany("INSERT OR IGNORE INTO processed_battles (battle_id) VALUES (?)", [(pid,) for pid in processed_ids])
        index_conn.commit() # Commit the index batch
        logging.info(f"Lote de {len(processed_ids)} IDs de batalla insertado en el índice.")
    
    index_conn.close() # Close index connection after all operations

    # --- Delete processed battles from raw_battles.db (only if structured and index commits were successful) ---
    if processed_ids and total_inserted_structured == len(processed_ids): # Ensure all were inserted
        raw_battles_conn_for_delete = database.get_raw_battles_db_connection()
        if raw_battles_conn_for_delete:
            cursor = raw_battles_conn_for_delete.cursor()
            cursor.executemany("DELETE FROM raw_battles WHERE battle_id = ?", [(pid,) for pid in processed_ids])
            raw_battles_conn_for_delete.commit()
            logging.info(f"{len(processed_ids)} batallas procesadas eliminadas de raw_battles.db.")
            raw_battles_conn_for_delete.close()
    else:
        logging.warning("No se eliminaron batallas de raw_battles.db porque no todas se insertaron correctamente en las DBs estructuradas o el índice.")

    logging.info(f"Procesador de batallas crudas finalizado. Procesadas (intentadas): {len(battles_to_process)}, Insertadas en estructuradas: {total_inserted_structured}, Saltadas: {skipped_count}.")

if __name__ == "__main__":
    process_raw_battles()