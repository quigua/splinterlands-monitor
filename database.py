import os
import sqlite3
import glob
import logging
import json
import time

# Definiciones de rutas
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_FOLDER = os.path.join(PROJECT_ROOT, 'data')
PLAYERS_DB = os.path.join(DB_FOLDER, 'players.db')
RAW_BATTLES_DB = os.path.join(DB_FOLDER, 'raw_battles.db')

STRUCTURED_BATTLES_ROOT = os.path.join(PROJECT_ROOT, 'Season')
STRUCTURED_BATTLES_DB_PATTERN = os.path.join(STRUCTURED_BATTLES_ROOT, '*', '*', '*.db')

def get_raw_battles_db_connection():
    conn = sqlite3.connect(os.path.join(DB_FOLDER, 'raw_battles.db'))
    conn.execute('PRAGMA journal_mode=WAL') # Habilitar WAL para concurrencia
    return conn

def get_battle_index_connection():
    """Retorna una conexión a la base de datos del índice de batallas."""
    conn = sqlite3.connect(os.path.join(DB_FOLDER, 'battle_index.db'))
    conn.execute('PRAGMA journal_mode=WAL') # Habilitar WAL para concurrencia
    return conn

def get_players_db_connection():
    conn = sqlite3.connect(PLAYERS_DB)
    conn.execute('PRAGMA journal_mode=WAL') # Habilitar WAL para concurrencia (ya existe)
    return conn

def get_structured_db_connection(season, match_type):
    # Construye la ruta exacta: /mnt/ssd/Splinterlands/Season/XXX/files.db
    db_path = os.path.join(STRUCTURED_BATTLES_ROOT, str(season), f'{match_type}.db')
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=WAL') # Habilitar WAL para concurrencia
    return conn

def initialize_structured_battle_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS battles (
            battle_id TEXT PRIMARY KEY,
            player_1 TEXT NOT NULL,
            player_2 TEXT NOT NULL,
            winner TEXT,
            loser TEXT,
            match_type TEXT NOT NULL,
            format TEXT NOT NULL,
            mana_cap INTEGER,
            ruleset TEXT,
            created_date TEXT NOT NULL,
            player_1_rating_initial INTEGER,
            player_2_rating_initial INTEGER,
            player_1_rating_final INTEGER,
            player_2_rating_final INTEGER,
            full_battle_json TEXT -- Nueva columna para el JSON completo
        )
    ''')
    conn.commit()

def insert_processed_battle(conn, battle_data):
    try:
        cursor = conn.cursor()
        full_battle_json_str = json.dumps(battle_data.get('original_json_data'))

        cursor.execute('''
            INSERT OR IGNORE INTO battles (
                battle_id, player_1, player_2, winner, loser, match_type, format,
                mana_cap, ruleset, created_date, player_1_rating_initial,
                player_2_rating_initial, player_1_rating_final, player_2_rating_final,
                full_battle_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            battle_data.get('battle_id'),
            battle_data.get('player_1'),
            battle_data.get('player_2'),
            battle_data.get('winner'),
            battle_data.get('loser'),
            battle_data.get('match_type'),
            battle_data.get('format'),
            battle_data.get('mana_cap'),
            battle_data.get('ruleset'),
            battle_data.get('created_date'),
            battle_data.get('player_1_rating_initial'),
            battle_data.get('player_2_rating_initial'),
            battle_data.get('player_1_rating_final'),
            battle_data.get('player_2_rating_final'),
            full_battle_json_str
        ))
        # conn.commit() # Commit will be handled by the caller for batching
        return True
    except sqlite3.IntegrityError:
        logging.warning(f"Batalla {battle_data.get('battle_id')} ya existe en la base de datos estructurada. Saltando inserción.")
        return False
    except Exception as e:
        logging.error(f"Error al insertar batalla procesada {battle_data.get('battle_id')}: {e}")
        return False

def battle_exists_in_structured_dbs(battle_id):
    """
    Verifica si un battle_id dado ya existe en alguna de las bases de datos estructuradas.
    """
    structured_db_files = glob.glob(STRUCTURED_BATTLES_DB_PATTERN)
    for db_file in structured_db_files:
        try:
            conn = sqlite3.connect(db_file, timeout=5) # Usar un timeout corto
            conn.execute('PRAGMA journal_mode=WAL') # Ensure WAL for this check
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM battles WHERE battle_id = ?", (battle_id,))
            if cursor.fetchone():
                conn.close()
                return True
            conn.close()
        except sqlite3.Error as e:
            logging.warning(f"Error al verificar batalla {battle_id} en {db_file}: {e}")
    return False

def battle_exists_in_index(conn, battle_id):
    """Verifica de forma rápida si un battle_id existe en el índice centralizado."""
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_battles WHERE battle_id = ?", (battle_id,))
    return cursor.fetchone() is not None

def add_battle_id_to_index(conn, battle_id):
    """
    Añade un battle_id al índice centralizado."""
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO processed_battles (battle_id) VALUES (?)", (battle_id,))
        # conn.commit() # Commit will be handled by the caller for batching
    except sqlite3.Error as e:
        logging.error(f"Error al añadir battle_id {battle_id} al índice: {e}")

def initialize_players_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS players (
            player_name TEXT PRIMARY KEY,
            last_scanned_timestamp INTEGER DEFAULT 0
        )
    ''')
    conn.commit()

def initialize_raw_battles_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_battles (
            battle_id TEXT PRIMARY KEY,
            battle_data TEXT
        )
    ''')
    conn.commit()

def get_total_players(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM players")
    return cursor.fetchone()[0]

def add_or_update_player(conn, player_name):
    """
    Adds or updates a single player. Does NOT commit. For batching, use add_or_update_players_batch.
    """
    cursor = conn.cursor()
    current_time = int(time.time())
    cursor.execute('''
        INSERT OR REPLACE INTO players (player_name, last_scanned_timestamp)
        VALUES (?, ?)
    ''', (player_name, current_time))
    # conn.commit() # Commit will be handled by the caller for batching

def add_or_update_players_batch(conn, player_names_list):
    """
    Adds or updates a list of players in a single batch operation.
    """
    if not player_names_list: return
    cursor = conn.cursor()
    current_time = int(time.time())
    # Use set to avoid duplicates and ensure each player is processed once per batch
    data_to_insert = [(name, current_time) for name in set(player_names_list)] 
    cursor.executemany('''
        INSERT OR REPLACE INTO players (player_name, last_scanned_timestamp)
        VALUES (?, ?)
    ''', data_to_insert)
    conn.commit() # Commit the batch
    logging.info(f"Batch updated {len(data_to_insert)} players.")

def get_priority_player_to_scan(conn, priority_players_names):
    cursor = conn.cursor()
    for player_name in priority_players_names:
        cursor.execute("SELECT player_name FROM players WHERE player_name = ? ORDER BY last_scanned_timestamp ASC LIMIT 1", (player_name, ))
        result = cursor.fetchone()
        if result:
            return result[0]
    return None

def get_player_to_scan(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT player_name FROM players ORDER BY last_scanned_timestamp ASC LIMIT 1")
    result = cursor.fetchone()
    return result[0] if result else None

def insert_raw_battle(conn, battle):
    """
    Inserts a single raw battle. Does NOT commit. For batching, use insert_raw_battles_batch.
    """
    try:
        battle_id = battle.get('battle_queue_id_1')
        if not battle_id:
            logging.warning("Intento de insertar batalla sin battle_queue_id_1. Saltando.")
            return False
        
        battle_data_json = json.dumps(battle)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO raw_battles (battle_id, battle_data)
            VALUES (?, ?)
        ''', (battle_id, battle_data_json))
        # conn.commit() # Commit will be handled by the caller for batching
        return True
    except Exception as e:
        logging.error(f"Error al insertar batalla cruda {battle.get('battle_queue_id_1')}: {e}")
        return False

def insert_raw_battles_batch(conn, battles_list):
    """
    Inserts a list of raw battles in a single batch operation.
    """
    if not battles_list: return
    cursor = conn.cursor()
    data_to_insert = []
    for battle in battles_list:
        battle_id = battle.get('battle_queue_id_1')
        if battle_id:
            battle_data_json = json.dumps(battle)
            data_to_insert.append((battle_id, battle_data_json))
        else:
            logging.warning("Batalla en lote sin battle_queue_id_1. Saltando.")

    if data_to_insert:
        cursor.executemany('''
            INSERT OR IGNORE INTO raw_battles (battle_id, battle_data)
            VALUES (?, ?)
        ''', data_to_insert)
        conn.commit() # Commit the batch
        logging.info(f"Batch inserted {len(data_to_insert)} raw battles.")

