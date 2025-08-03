import requests
import json
from datetime import datetime, timezone
import time
import os # Added for os.path.exists and os.path.join

API_BASE_URL = "https://api.splinterlands.com"
SEASONS_FILE = "seasons_data.json"

def get_season_data(season_id):
    endpoint = f"{API_BASE_URL}/season?id={season_id}"
    try:
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # print(f"Error al obtener datos para la temporada {season_id}: {e}") # Keep this for debugging if needed
        return None

def load_existing_seasons():
    if os.path.exists(SEASONS_FILE):
        try:
            with open(SEASONS_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Advertencia: Error al decodificar {SEASONS_FILE}. Se ignorará el contenido existente.")
            return []
    return []

def get_all_seasons():
    existing_seasons = load_existing_seasons()
    all_seasons_dict = {s['id']: s for s in existing_seasons} # Use a dict to handle potential duplicates and easy updates
    
    max_season_id = 0
    if existing_seasons:
        max_season_id = max(s['id'] for s in existing_seasons)

    season_id_to_start = max_season_id + 1 if max_season_id > 0 else 1
    
    print(f"Obteniendo información de temporadas a partir de la temporada {season_id_to_start}...")

    current_time = datetime.now(timezone.utc)

    while True:
        data = get_season_data(season_id_to_start)
        if data is None:
            # If there's an error getting data, assume we've reached the end or there's a problem.
            break

        current_ends_date_str = data.get('ends')
        reset_block_num = data.get('reset_block_num')

        if current_ends_date_str:
            current_ends_date = datetime.fromisoformat(current_ends_date_str.replace('Z', '+00:00'))
            
            # Si la fecha de finalización es en el futuro, hemos encontrado la temporada actual.
            if current_ends_date > current_time:
                all_seasons_dict[data['id']] = data # Incluir la temporada actual
                break # Detener la búsqueda después de encontrar la primera temporada futura
            else:
                # Si la temporada ya ha terminado, la añadimos
                all_seasons_dict[data['id']] = data
        else:
            # Si no hay fecha de finalización, algo está mal o hemos llegado al final.
            break

        season_id_to_start += 1
        time.sleep(0.1) # Small pause to not saturate the API

        # Safety limit to avoid infinite loops in case of unexpected API behavior
        # This limit should be relative to the last known season, not a fixed number.
        # Let's say we don't expect more than 50 new seasons at a time.
        if season_id_to_start > max_season_id + 50:
            print("Advertencia: Límite de búsqueda de nuevas temporadas alcanzado. Podría haber más temporadas.")
            break

    sorted_unique_seasons = sorted(all_seasons_dict.values(), key=lambda x: x['id'])

    print(f"Se obtuvieron {len(sorted_unique_seasons)} temporadas.")
    return sorted_unique_seasons

if __name__ == "__main__":
    seasons = get_all_seasons()
    if seasons:
        with open(SEASONS_FILE, 'w') as f:
            json.dump(seasons, f, indent=2)
        print(f"Datos de temporadas guardados en {SEASONS_FILE}")
    else:
        print("No se pudieron obtener datos de temporadas para guardar.")