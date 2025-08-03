## Work Log

### 2025-07-03
- Read log file: `/mnt/ssd/Splinterlands_Services/desarrollo.log`
- Created `user_stats_service.py` with a placeholder `get_user_stats` function.
- Updated `user_stats_service.py` to calculate battles played, wins, losses, and win rate from season-specific ranked battle databases.
- Modified `user_stats_service.py` to determine the current season from `seasons_data.json` and fetch stats for that season by default, with an option to specify a season ID.
- Fixed timezone comparison error in `get_current_season_id`.
- Confirmed that `get_user_stats` returns 0 battles/wins for 'quigua' because no battle data exists for this user in the `season_162_ranked_ranked.db`.
- Modified `user_stats_service.py` to calculate and display battle statistics for 'modern', 'wild' (ranked), and 'survival' formats.
- Updated the `if __name__ == "__main__":` block to format the output of user statistics, including a breakdown by battle format.
- Added `season_id` to the returned `stats` dictionary.
- Updated `user_stats_service.py` to sum up battles and wins from all formats to calculate overall stats.
- Corrected `season_id` assignment in `get_user_stats` to ensure it's always set in the returned dictionary.
- Implemented `get_rank_from_points` function to convert points to rank.
- Modified `get_user_stats` to fetch and calculate rank for each battle format by parsing `battle_data` JSON.
- Updated the main execution block to display rank per format and remove overall rank and collection power from the summary.
- Successfully included total glints by correctly parsing the nested `dec_info` JSON within `battle_data` and summing glints from all won battles.

### 2025-07-04
- Reduced `MIN_SCAN_INTERVAL_HOURS` to `MIN_SCAN_INTERVAL_MINUTES` (10 minutes) in `database.py`.
- Fixed `PLAYERS_DB` path in `database.py` to be absolute, resolving issues with database visibility.
- Enabled WAL (Write-Ahead Logging) mode in `database.py` for concurrent database access.
- Added `format_stats_as_markdown` function to `user_stats_service.py` for generating formatted Markdown responses.
- Included `last_updated` timestamp in `user_stats_service.py` stats and Markdown output.
- Implemented player prioritization logic:
    - Added `get_priority_player_to_scan` function to `database.py`.
    - Modified `main.py` to load pending requests and prioritize scanning players from `pending_requests.json` who have requested 'stats' and need updating.
    - Added `save_pending_requests` function to `main.py`.
    - Modified `main.py` to update the status of priority requests to `READY_FOR_PROCESSING` after scanning the player.
- Integrated request processing in `service.py`:
    - Removed `is_player_data_fresh` function from `service.py`.
    - Modified `service.py` to process 'stats' requests from `pending_requests.json` only when their status is `READY_FOR_PROCESSING`.
    - Simulated comment publication and updated request status to `REPLY_SENT`.

### 2025-07-26
- Diagnosed "database is locked" error in `process_raw_battles.py` due to concurrent access with `main.py`.
- Attempted to resolve with `timeout` parameter in `database.py` connections, but it was insufficient.
- Identified the root cause: `process_raw_battles.py` was holding a lock on `raw_battles.db` while processing.
- Modified `process_raw_battles.py` to read all battles into memory, close the connection, process, and then re-open to delete, eliminating locking conflicts.
- Created `cleanup_processed_battles.py` to remove already processed battles from `raw_battles.db`.
- Executed `cleanup_processed_battles.py` to significantly reduce `raw_battles.db` size (from 3.1GB to 5.3MB).
- Modified `main.py` to include a check (`database.battle_exists_in_structured_dbs`) before inserting battles into `raw_battles.db`, preventing re-insertion of already processed battles.
- Verified that `main.py` no longer inserts duplicates into `raw_battles.db`.
- Executed `VACUUM` on `raw_battles.db` to reclaim disk space.
- Restarted both `main.py` and `process_raw_battles.py` services.
- Confirmed both services are running and the system is stable.

### 2025-07-27 (Sesión Actual)
- Diagnosed `AttributeError: cannot import name 'get_player_details'` in `main.py`.
- **Fixed `main.py`**: Replaced `get_player_details` with `get_player_login`.
- Diagnosed `NameError: name 'os' is not defined` in `database.py`.
- **Fixed `database.py`**: Added missing imports (`os`, `sqlite3`, `glob`, `logging`).
- Diagnosed `AttributeError: module 'database' has no attribute 'get_raw_battles_db_connection'`.
- **Fixed `database.py`**: Added `get_raw_battles_db_connection` function.
- Diagnosed `AttributeError: module 'database' has no attribute 'get_structured_db_connection'`.
- **Fixed `database.py`**: Added `get_structured_db_connection` function (including `os.makedirs` for directory creation).
- Diagnosed `AttributeError: module 'database' has no attribute 'initialize_structured_battle_table'`.
- **Fixed `database.py`**: Added `initialize_structured_battle_table` function.
- Diagnosed `AttributeError: module 'database' has no attribute 'insert_processed_battle'`.
- **Fixed `database.py`**: Added `insert_processed_battle` function (and `import json`).
- Diagnosed `sqlite3.OperationalError: no such column: username` in `players.db`.
- **Identified schema mismatch in `players.db`**: `player_name`, `last_scanned_timestamp` vs `username`, `last_scanned`.
- **Fixed `database.py`**: Modified `initialize_players_table`, `add_or_update_player`, `get_priority_player_to_scan`, `get_player_to_scan` to use `player_name` and `last_scanned_timestamp`.
- Diagnosed `NameError: name 'time' is not defined` in `database.py`.
- **Fixed `database.py`**: Added `import time`.
- Diagnosed `sqlite3.OperationalError: NOT NULL constraint failed` during timestamp update in `players.db`.
- **Fixed `players.db` data inconsistency**: Updated all `last_scanned_timestamp` to current Unix timestamp using `sqlite3` command.
- **Addressed data storage requirement**: User requested to store all battle data, including nested info.
- **Fixed `database.py`**: Modified `initialize_structured_battle_table` to include `full_battle_json` column.
- **Fixed `database.py`**: Modified `insert_processed_battle` to insert the full battle JSON into `full_battle_json`.
- **Fixed `process_raw_battles.py`**: Modified to pass the original full battle JSON to `insert_processed_battle`.
- **Addressed structured DB path**: User requested `/mnt/ssd/Splinterlands/Season/XXX/files.db`.
- **Fixed `database.py`**: Updated `STRUCTURED_BATTLES_ROOT` and `get_structured_db_connection` to use the specified path.
- **Simplified player prioritization**: User requested to remove `command` check from `main.py` priority logic.
- **Fixed `main.py`**: Modified priority logic to only check `status` and `target_username`.
- **Prioritized "quigua"**: Added a `DETECTED` entry for "quigua" in `pending_requests.json`.
- **Created `migrate_one_battle.py`**: Script for testing single battle migration to new structure.
- **Verified `migrate_one_battle.py`**: Confirmed successful migration of a test battle with full JSON.
- **Created `migrate_all_old_data.py`**: Script for full migration of all old structured data to the new structure.
- **Executed `migrate_all_old_data.py`**: Script completed, migrating 577,286 battles with 1 error (`database is locked`). Verified total battle count in new structure matches migrated count.
- **Cleaned up directory**: Moved non-essential files and temporary scripts from `/mnt/ssd/Splinterlands/` to `/mnt/ssd/tmp/`.
- **Identified and restored `get_all_seasons.py`**: Moved it back to `/mnt/ssd/Splinterlands/` as it's essential for `seasons_data.json` updates.
- **Discussed `crontab` for `get_all_seasons.py`**: Agreed on a `crontab` entry to run `get_all_seasons.py` every 15 minutes to keep `seasons_data.json` updated.