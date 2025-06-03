import duckdb
import os

# Configuration
NORMALIZED_DIR = "normalized_kexp_jsonl/"
DB_FILE = "kexp_data.db"


def ingest_normalized_data():
    """
    Ingests normalized KEXP JSONL data into a DuckDB database.
    """
    if not os.path.exists(NORMALIZED_DIR):
        print(
            f"Error: Normalized data directory '{NORMALIZED_DIR}' not found.")
        print("Please run the normalization script (normalize_kexp.py) first.")
        return

    print(
        f"Connecting to DuckDB. Database file: {DB_FILE if DB_FILE else 'in-memory'}")
    try:
        if DB_FILE:
            con: duckdb.DuckDBPyConnection | None = duckdb.connect(
                database=DB_FILE, read_only=False)
        else:
            con: duckdb.DuckDBPyConnection | None = duckdb.connect(
                database=':memory:', read_only=False)

        print("Successfully connected to DuckDB.")

        explicit_schemas = {
            "bridge_play_to_artist": {'play_id': 'BIGINT', 'artist_id_internal': 'VARCHAR'},
            "bridge_play_to_label": {'play_id': 'BIGINT', 'label_id_internal': 'VARCHAR'},
            # Add other bridge/fact tables here if they show similar issues
            "dim_artists_master": {"artist_id_internal": "VARCHAR", "primary_name_observed": "VARCHAR", "mb_id": "VARCHAR"},
            "dim_labels_master": {"label_id_internal": "VARCHAR", "primary_name_observed": "VARCHAR", "mb_id": "VARCHAR"},
            "dim_releases_master": {"release_id_internal": "VARCHAR", "primary_album_name_observed": "VARCHAR", "mb_release_id": "VARCHAR", "mb_release_group_id": "VARCHAR", "release_date_iso": "VARCHAR"},
            "dim_tracks": {"track_id_internal": "VARCHAR", "primary_song_title_observed": "VARCHAR", "mb_track_id": "VARCHAR", "mb_recording_id": "VARCHAR", "release_id_internal_on_track": "VARCHAR"},
            # For other bridge tables with integer IDs, auto_detect is usually fine, but can be added if issues arise
            "bridge_show_hosts": {'show_id': 'BIGINT', 'host_id': 'BIGINT'},
            "bridge_timeslot_hosts": {'timeslot_id': 'BIGINT', 'host_id': 'BIGINT'}
        }

        tables_to_create = {
            "dim_hosts": "dim_hosts.jsonl",
            "dim_programs": "dim_programs.jsonl",
            "dim_shows": "dim_shows.jsonl",
            "dim_artists_master": "dim_artists_master.jsonl",
            "dim_labels_master": "dim_labels_master.jsonl",
            "dim_releases_master": "dim_releases_master.jsonl",
            "dim_tracks": "dim_tracks.jsonl",
            "dim_timeslots": "dim_timeslots.jsonl",
            "bridge_artist_id_to_names": "bridge_artist_id_to_names.jsonl",
            "bridge_release_id_to_names": "bridge_release_id_to_names.jsonl",
            "bridge_label_id_to_names": "bridge_label_id_to_names.jsonl",
            "fact_plays": "fact_plays.jsonl",
            "bridge_show_hosts": "bridge_show_hosts.jsonl",
            "bridge_play_to_artist": "bridge_play_to_artist.jsonl",
            "bridge_play_to_label": "bridge_play_to_label.jsonl",
            "bridge_timeslot_hosts": "bridge_timeslot_hosts.jsonl"
        }

        print("\n--- Creating tables ---")
        for table_name, file_name in tables_to_create.items():
            file_path = os.path.join(NORMALIZED_DIR, file_name)
            if not os.path.exists(file_path):
                print(
                    f"Warning: File {file_path} not found. Skipping table {table_name}.")
                continue

            sql_file_path = file_path.replace('\\', '/')

            query_base_function = "read_json_auto"
            options = "format='newline_delimited', auto_detect=True"

            if table_name in explicit_schemas:
                query_base_function = "read_json"
                cols_def = explicit_schemas[table_name]
                cols_str = ", ".join(
                    [f"'{k}': '{v}'" for k, v in cols_def.items()])
                options = f"format='newline_delimited', auto_detect=False, columns={{{cols_str}}}"

            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM {query_base_function}('{sql_file_path}', {options});
            """

            print(f"Creating table: {table_name} from {file_name}...")
            con.execute(query)
            print(f"Table {table_name} created successfully.")

        print("\n--- Altering column types for dates/times and UUIDs ---")
        alter_statements = [
            # Dates and Timestamps
            "ALTER TABLE fact_plays ALTER airdate_iso TYPE TIMESTAMP USING TRY_CAST(airdate_iso AS TIMESTAMP);",
            "ALTER TABLE dim_shows ALTER start_time_iso TYPE TIMESTAMP USING TRY_CAST(start_time_iso AS TIMESTAMP);",
            "ALTER TABLE dim_releases_master ALTER release_date_iso TYPE DATE USING TRY_CAST(release_date_iso AS DATE);",
            "ALTER TABLE dim_timeslots ALTER start_date_iso TYPE DATE USING TRY_CAST(start_date_iso AS DATE);",
            "ALTER TABLE dim_timeslots ALTER end_date_iso TYPE DATE USING TRY_CAST(end_date_iso AS DATE);",
            "ALTER TABLE dim_timeslots ALTER start_time_str TYPE TIME USING TRY_CAST(start_time_str AS TIME);",
            "ALTER TABLE dim_timeslots ALTER end_time_str TYPE TIME USING TRY_CAST(end_time_str AS TIME);",
            "ALTER TABLE dim_timeslots ALTER duration_str TYPE INTERVAL USING TRY_CAST(duration_str AS INTERVAL);",
            # UUIDs from VARCHAR (for tables where schema was explicit)
            "ALTER TABLE bridge_play_to_artist ALTER artist_id_internal TYPE UUID USING TRY_CAST(artist_id_internal AS UUID);",
            "ALTER TABLE bridge_play_to_label ALTER label_id_internal TYPE UUID USING TRY_CAST(label_id_internal AS UUID);",
            "ALTER TABLE dim_artists_master ALTER artist_id_internal TYPE UUID USING TRY_CAST(artist_id_internal AS UUID);",
            "ALTER TABLE dim_artists_master ALTER mb_id TYPE UUID USING TRY_CAST(mb_id AS UUID);",
            "ALTER TABLE dim_labels_master ALTER label_id_internal TYPE UUID USING TRY_CAST(label_id_internal AS UUID);",
            "ALTER TABLE dim_labels_master ALTER mb_id TYPE UUID USING TRY_CAST(mb_id AS UUID);",
            "ALTER TABLE dim_releases_master ALTER release_id_internal TYPE UUID USING TRY_CAST(release_id_internal AS UUID);",
            "ALTER TABLE dim_releases_master ALTER mb_release_id TYPE UUID USING TRY_CAST(mb_release_id AS UUID);",
            "ALTER TABLE dim_releases_master ALTER mb_release_group_id TYPE UUID USING TRY_CAST(mb_release_group_id AS UUID);",
            "ALTER TABLE dim_tracks ALTER track_id_internal TYPE UUID USING TRY_CAST(track_id_internal AS UUID);",
            "ALTER TABLE dim_tracks ALTER mb_track_id TYPE UUID USING TRY_CAST(mb_track_id AS UUID);",
            "ALTER TABLE dim_tracks ALTER mb_recording_id TYPE UUID USING TRY_CAST(mb_recording_id AS UUID);",
            "ALTER TABLE dim_tracks ALTER release_id_internal_on_track TYPE UUID USING TRY_CAST(release_id_internal_on_track AS UUID);"
        ]

        for stmt in alter_statements:
            try:

                table_name_for_alter = stmt.split("ALTER TABLE ")[
                    1].split(" ")[0]
                # Check if table exists before trying to alter it using duckdb_tables()
                result = con.execute(
                    f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table_name_for_alter}'").fetchone()
                table_exists = result[0] > 0 if result else False

                if table_exists:
                    print(f"Executing: {stmt}")
                    con.execute(stmt)
                    print("Alteration successful.")
                else:
                    print(
                        f"Skipping alteration: Table '{table_name_for_alter}' does not exist (likely its source JSONL was missing).")
            except Exception as e:
                print(f"Warning: Could not execute alter statement: {stmt}")
                print(f"Error: {e}")

        print("\n--- Data ingestion complete ---")

        print("\n--- Sample counts ---")
        for table_name in tables_to_create.keys():
            result = con.execute(
                f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table_name}'").fetchone()
            table_exists = result[0] > 0 if result else False
            if table_exists:
                count_df = con.execute(
                    f"SELECT COUNT(*) AS count FROM {table_name};").fetchdf()
                print(f"Table '{table_name}': {count_df['count'][0]} rows")
            else:
                print(
                    f"Table '{table_name}': Does not exist (source JSONL likely missing).")

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if con:
            print("Closing DuckDB connection.")
            con.close()


if __name__ == '__main__':
    ingest_normalized_data()
