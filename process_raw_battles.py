import os
import json
import logging
from datetime import datetime, timezone

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
        # Convertir la fecha de la batalla a un objeto datetime con información de zona horaria
        # Reemplazar 'Z' con '+00:00' para compatibilidad con fromisoformat
        battle_date = datetime.fromisoformat(battle_date_str.replace('Z', '+00:00'))

        # Iterar sobre las temporadas ordenadas para encontrar la correcta
        for season in seasons_data:
            season_end_date = datetime.fromisoformat(season['ends'].replace('Z', '+00:00'))
            # Si la fecha de la batalla es anterior o igual a la fecha de fin de la temporada,
            # entonces pertenece a esta temporada.
            if battle_date <= season_end_date:
                return season['id']
        
        # Si la batalla es posterior a la última temporada conocida
        logging.warning(f"No se pudo determinar la temporada para la batalla con fecha: {battle_date_str}. Podría ser una temporada futura o datos incompletos.")
        return None
    except ValueError as e:
        logging.error(f"Error al parsear la fecha de la batalla '{battle_date_str}': {e}")
        return None

# --- Lógica Principal del Procesador ---
def process_raw_battles():
    logging.info("Iniciando el procesador de batallas crudas...")

    raw_battles_conn = database.get_raw_battles_db_connection()
    if not raw_battles_conn:
        logging.error("No se pudo conectar a la base de datos de batallas crudas. Abortando.")
        return

    index_conn = database.get_battle_index_connection()
    if not index_conn:
        logging.error("No se pudo conectar a la base de datos del índice. Abortando.")
        raw_battles_conn.close()
        return

    seasons_data = load_season_data()
    if not seasons_data:
        logging.error("No se pudieron cargar los datos de las temporadas. Abortando.")
        raw_battles_conn.close()
        return

    cursor = raw_battles_conn.cursor()
    # Seleccionar batallas que aún no han sido procesadas
    cursor.execute("SELECT battle_id, battle_data FROM raw_battles")
    battles_to_process = cursor.fetchall()
    raw_battles_conn.close()

    processed_ids = []
    skipped_count = 0

    for battle_id, battle_data_json in battles_to_process:
        try:
            battle = json.loads(battle_data_json)
        except json.JSONDecodeError as e:
            logging.error(f"Error al decodificar JSON para la batalla {battle_id}: {e}. Datos crudos: {battle_data_json}")
            skipped_count += 1
            continue
        except Exception as e:
            logging.error(f"Error inesperado al cargar JSON para la batalla {battle_id}: {e}. Datos crudos: {battle_data_json}")
            skipped_count += 1
            continue

        try:
            created_date = battle.get('created_date')
            match_type = battle.get('match_type')
            game_format = battle.get('format') # 'format' puede ser 'modern' o 'wild'

            if not created_date:
                logging.warning(f"Batalla {battle_id} no tiene 'created_date'. Saltando.")
                skipped_count += 1
                continue

            season_id = get_season_id_from_date(created_date, seasons_data)
            if season_id is None:
                logging.warning(f"No se pudo determinar la temporada para la batalla {battle_id}. Saltando.")
                skipped_count += 1
                continue
            
            # Determinar el formato final de la batalla
            final_format = determine_battle_format(battle, match_type, game_format)

            # Preparar datos para la base de datos estructurada
            battle_data_processed = {
                'battle_id': battle_id,
                'player_1': battle.get('player_1'),
                'player_2': battle.get('player_2'),
                'winner': battle.get('winner'),
                'loser': battle.get('loser'),
                'match_type': match_type,
                'format': final_format,
                'mana_cap': battle.get('mana_cap'),
                'ruleset': battle.get('ruleset'),
                'created_date': created_date,
                'player_1_rating_initial': battle.get('player_1_rating_initial'),
                'player_2_rating_initial': battle.get('player_2_rating_initial'),
                'player_1_rating_final': battle.get('player_1_rating_final'),
                'player_2_rating_final': battle.get('player_2_rating_final'),
                'original_json_data': battle # Añadir el JSON original completo
            }

            # Conectar e inicializar la base de datos estructurada para esta batalla
            structured_db_conn = database.get_structured_db_connection(season_id, final_format)
            if not structured_db_conn:
                logging.error(f"No se pudo conectar a la DB estructurada para Temporada {season_id}, Formato {final_format}. Saltando batalla {battle_id}.")
                skipped_count += 1
                continue
            
            database.initialize_structured_battle_table(structured_db_conn)

            # Insertar la batalla procesada
            if database.insert_processed_battle(structured_db_conn, battle_data_processed):
                logging.info(f"Batalla {battle_id} (T{season_id}, F:{final_format}) procesada y guardada.")
                # Añadir al nuevo índice centralizado
                database.add_battle_id_to_index(index_conn, battle_id)
                processed_ids.append(battle_id)
            else:
                logging.error(f"Fallo al insertar batalla {battle_id} en la DB estructurada.")
                skipped_count += 1
            
            structured_db_conn.close()

        except Exception as e:
            logging.error(f"Error inesperado al procesar la batalla {battle_id}: {e}")
            skipped_count += 1

    # Eliminar las batallas procesadas de raw_battles.db
    if processed_ids:
        raw_battles_conn = database.get_raw_battles_db_connection()
        if raw_battles_conn:
            try:
                cursor = raw_battles_conn.cursor()
                cursor.executemany("DELETE FROM raw_battles WHERE battle_id = ?", [(pid,) for pid in processed_ids])
                raw_battles_conn.commit()
                logging.info(f"{len(processed_ids)} batallas procesadas eliminadas de raw_battles.db.")
            except sqlite3.Error as e:
                logging.error(f"Error al eliminar batallas procesadas de raw_battles.db: {e}")
            finally:
                raw_battles_conn.close()

    logging.info(f"Procesador de batallas crudas finalizado. Procesadas: {len(processed_ids)}, Saltadas: {skipped_count}.")

if __name__ == "__main__":
    process_raw_battles()