import os
import sqlite3
import glob
import logging

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler() # Log to console
    ]
)

# --- Definiciones de Rutas ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_FOLDER = os.path.join(PROJECT_ROOT, 'data')
BATTLE_INDEX_DB = os.path.join(DB_FOLDER, 'battle_index.db')
STRUCTURED_BATTLES_ROOT = os.path.join(PROJECT_ROOT, 'Season')
# El patrón correcto para encontrar las bases de datos de temporada/formato
STRUCTURED_DB_PATTERN = os.path.join(STRUCTURED_BATTLES_ROOT, '*', '*.db')

def create_index_db_and_table():
    """Crea la base de datos del índice y la tabla si no existen."""
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)
    
    conn = sqlite3.connect(BATTLE_INDEX_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_battles (
            battle_id TEXT PRIMARY KEY
        )
    ''')
    # NEW: Clear the table before rebuilding the index
    cursor.execute("DELETE FROM processed_battles")
    conn.commit()
    return conn

def main():
    """Función principal para construir el índice de batallas."""
    logging.info("Iniciando la creación del índice de batallas procesadas.")
    
    index_conn = create_index_db_and_table()
    index_cursor = index_conn.cursor()

    structured_db_files = glob.glob(STRUCTURED_DB_PATTERN)
    
    if not structured_db_files:
        logging.warning("No se encontraron bases de datos estructuradas. El índice estará vacío.")
        index_conn.close()
        return

    logging.info(f"Se encontraron {len(structured_db_files)} bases de datos estructuradas para procesar.")

    for db_file in structured_db_files:
        try:
            logging.info(f"Procesando archivo: {os.path.basename(db_file)}")
            # Conectar en modo de solo lectura para seguridad
            source_conn = sqlite3.connect(f'file:{db_file}?mode=ro', uri=True)
            source_cursor = source_conn.cursor()
            
            # Verificar que la tabla 'battles' exista
            source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='battles'")
            if source_cursor.fetchone() is None:
                logging.warning(f"La tabla 'battles' no existe en {db_file}. Saltando archivo.")
                source_conn.close()
                continue

            source_cursor.execute("SELECT battle_id FROM battles")
            battle_ids = source_cursor.fetchall()
            
            if battle_ids:
                # Usar executemany para una inserción masiva y eficiente
                index_cursor.executemany("INSERT OR IGNORE INTO processed_battles (battle_id) VALUES (?)", battle_ids)
                index_conn.commit()
                logging.info(f"Se encontraron e insertaron {len(battle_ids)} IDs de batalla.")
            else:
                logging.info(f"No se encontraron batallas en este archivo.")
            
            source_conn.close()

        except sqlite3.Error as e:
            logging.error(f"Error de SQLite al procesar {db_file}: {e}")
        except Exception as e:
            logging.error(f"Error inesperado al procesar {db_file}: {e}")

    # Obtener el conteo final directamente del índice para mayor precisión
    index_cursor.execute("SELECT COUNT(*) FROM processed_battles")
    final_count = index_cursor.fetchone()[0]
    logging.info(f"Creación del índice completada. Total de IDs únicas en el índice: {final_count}.")
    
    index_conn.close()

if __name__ == "__main__":
    main()