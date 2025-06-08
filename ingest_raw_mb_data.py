import duckdb
import os
import time

# --- Configuration ---
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
MB_ARTIST_DUMP_PATH = "data/kb_dumps/mb_dumps/artist/mbdump/artist"
RAW_TABLE_NAME = "mb_artists_raw"


def ingest_raw_data():
    """
    Ingests the entire MusicBrainz artist dump into a single DuckDB table,
    letting DuckDB auto-detect the schema.
    """
    if not os.path.exists(MB_ARTIST_DUMP_PATH):
        print(
            f"❌ ERROR: MusicBrainz dump file not found at: {MB_ARTIST_DUMP_PATH}")
        return

    conn = duckdb.connect(DB_PATH)
    print(f"✅ Connected to DuckDB at {DB_PATH}.")

    try:
        print(f"Dropping table '{RAW_TABLE_NAME}' if it exists...")
        conn.execute(f"DROP TABLE IF EXISTS {RAW_TABLE_NAME};")

        print(
            f"Starting ingestion of {MB_ARTIST_DUMP_PATH} into '{RAW_TABLE_NAME}'...")
        print("This may take several minutes. DuckDB's CLI progress bar may be displayed.")

        start_time = time.time()

        conn.execute("PRAGMA enable_logging;")
        conn.execute("SET logging_level = 'DEBUG';")

        # Use read_json with auto-detection. This is the most robust way to load
        # complex JSON. We pass maximum_object_size directly as a parameter.
        conn.execute(f"""
            CREATE TABLE {RAW_TABLE_NAME} AS
            SELECT * FROM read_json('{MB_ARTIST_DUMP_PATH}',
                format = 'newline_delimited',
                records = true, 
                auto_detect = true,
                maximum_object_size = 33554432,
                ignore_errors = true
            );
        """)

        duration = time.time() - start_time

        count = conn.execute(
            f"SELECT COUNT(*) FROM {RAW_TABLE_NAME}").fetchone()[0]
        print(
            f"\n✅ Success! Ingested {count:,} records in {duration:.2f} seconds.")

    except Exception as e:
        print(f"❌ An error occurred during ingestion: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("🔐 Database connection closed.")


if __name__ == '__main__':
    ingest_raw_data()
