import sqlite3
import os
import time
from datetime import datetime, timedelta

DB_PATH = "/mnt/ssd/Splinterlands/data/players.db"

def get_oldest_scan_time():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database file not found at {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT MIN(last_scanned_timestamp) FROM players")
        
        min_timestamp = cursor.fetchone()[0]
        conn.close()

        if min_timestamp is None:
            print("No players found in the database or no scan timestamps recorded.")
            return

        # Explicitly cast to int to avoid type errors
        min_timestamp = int(min_timestamp)

        current_time = int(time.time())
        time_diff_seconds = current_time - min_timestamp

        td = timedelta(seconds=time_diff_seconds)
        
        print(f"The player with the oldest scan was processed {td} ago.")
        print(f"  (Timestamp: {datetime.fromtimestamp(min_timestamp).strftime('%Y-%m-%d %H:%M:%S UTC')})")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    get_oldest_scan_time()
