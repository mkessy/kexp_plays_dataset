#!/usr/bin/env python3
"""
KEXP Knowledge Base - Data Ingestion
===================================
Ingests normalized KEXP JSONL data into a DuckDB database.

This script is part of the KEXP Knowledge Base Pipeline and should be run
after normalize_kexp.py has processed the raw KEXP data.

The script:
1. Safely drops existing tables with CASCADE to handle dependencies
2. Creates DuckDB tables for all normalized JSONL files
3. Applies proper type conversions (dates, timestamps, UUIDs)
4. Validates the ingestion process
"""

import duckdb
import os
import logging
import traceback
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

# Configuration
NORMALIZED_DIR = os.getenv("NORMALIZED_DIR", "normalized_kexp_jsonl/")
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Table schemas requiring explicit column definitions
EXPLICIT_SCHEMAS = {
    "bridge_play_to_artist": {'play_id': 'BIGINT', 'artist_id_internal': 'VARCHAR'},
    "bridge_play_to_label": {'play_id': 'BIGINT', 'label_id_internal': 'VARCHAR'},
    "dim_artists_master": {"artist_id_internal": "VARCHAR", "primary_name_observed": "VARCHAR", "mb_id": "VARCHAR"},
    "dim_labels_master": {"label_id_internal": "VARCHAR", "primary_name_observed": "VARCHAR", "mb_id": "VARCHAR"},
    "dim_releases_master": {"release_id_internal": "VARCHAR", "primary_album_name_observed": "VARCHAR",
                            "mb_release_id": "VARCHAR", "mb_release_group_id": "VARCHAR", "release_date_iso": "VARCHAR"},
    "dim_tracks": {"track_id_internal": "VARCHAR", "primary_song_title_observed": "VARCHAR",
                   "mb_track_id": "VARCHAR", "mb_recording_id": "VARCHAR", "release_id_internal_on_track": "VARCHAR"},
    "bridge_show_hosts": {'show_id': 'BIGINT', 'host_id': 'BIGINT'},
    "bridge_timeslot_hosts": {'timeslot_id': 'BIGINT', 'host_id': 'BIGINT'}
}

# Tables to create and their source files (in dependency order)
TABLES_TO_CREATE = {
    "dim_hosts": "dim_hosts.jsonl",
    "dim_programs": "dim_programs.jsonl",
    "dim_shows": "dim_shows.jsonl",
    "dim_timeslots": "dim_timeslots.jsonl",
    "dim_artists_master": "dim_artists_master.jsonl",
    "dim_labels_master": "dim_labels_master.jsonl",
    "dim_releases_master": "dim_releases_master.jsonl",
    "dim_tracks": "dim_tracks.jsonl",
    "bridge_artist_id_to_names": "bridge_artist_id_to_names.jsonl",
    "bridge_release_id_to_names": "bridge_release_id_to_names.jsonl",
    "bridge_label_id_to_names": "bridge_label_id_to_names.jsonl",
    "fact_plays": "fact_plays.jsonl",
    "bridge_show_hosts": "bridge_show_hosts.jsonl",
    "bridge_play_to_artist": "bridge_play_to_artist.jsonl",
    "bridge_play_to_label": "bridge_play_to_label.jsonl",
    "bridge_timeslot_hosts": "bridge_timeslot_hosts.jsonl"
}

# Tables that might have KB dependencies - need CASCADE drop
TABLES_WITH_DEPENDENCIES = [
    "dim_artists_master",
    "dim_tracks",
    "dim_releases_master",
    "dim_labels_master"
]

# Type conversion statements to be executed after table creation
TYPE_CONVERSION_STATEMENTS = [
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
    "ALTER TABLE dim_labels_master ALTER label_id_internal TYPE UUID USING TRY_CAST(label_id_internal AS UUID);",
    "ALTER TABLE dim_labels_master ALTER mb_id TYPE UUID USING TRY_CAST(mb_id AS UUID);",
    "ALTER TABLE dim_releases_master ALTER release_id_internal TYPE UUID USING TRY_CAST(release_id_internal AS UUID);",
    "ALTER TABLE dim_releases_master ALTER mb_release_id TYPE UUID USING TRY_CAST(mb_release_id AS UUID);",
    "ALTER TABLE dim_releases_master ALTER mb_release_group_id TYPE UUID USING TRY_CAST(mb_release_group_id AS UUID);"
]

# Required tables that must exist after ingestion
REQUIRED_TABLES = [
    "dim_artists_master",
    "dim_tracks",
    "fact_plays"
]


def validate_input_files() -> bool:
    """Validate that required input files exist before attempting ingestion."""
    normalized_dir = Path(NORMALIZED_DIR)

    if not normalized_dir.exists():
        logger.error(
            f"Normalized data directory '{NORMALIZED_DIR}' not found.")
        logger.error(
            "Please run the normalization script (normalize_kexp.py) first.")
        return False

    # Check for essential files
    missing_files = []
    essential_files = ["dim_artists_master.jsonl",
                       "dim_tracks.jsonl", "fact_plays.jsonl"]

    for file_name in essential_files:
        file_path = normalized_dir / file_name
        if not file_path.exists():
            missing_files.append(file_name)

    if missing_files:
        logger.error(
            f"Essential files are missing: {', '.join(missing_files)}")
        logger.error(
            "Please run the normalization script (normalize_kexp.py) first.")
        return False

    return True


def safely_drop_table(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Safely drop a table with CASCADE if it has dependencies."""
    try:
        # First check if table exists
        result = conn.execute(
            f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table_name}'"
        ).fetchone()
        table_exists = result[0] > 0 if result else False

        if table_exists:
            if table_name in TABLES_WITH_DEPENDENCIES:
                # Use CASCADE for tables that might have KB dependencies
                logger.info(f"Dropping table {table_name} with CASCADE...")
                conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            else:
                # Regular drop for other tables
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"Successfully dropped table {table_name}")
        else:
            logger.debug(f"Table {table_name} does not exist, skipping drop")

    except Exception as e:
        logger.warning(f"Could not drop table {table_name}: {e}")
        # Continue anyway - the CREATE might still work


def create_tables(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Create tables from normalized JSONL files."""
    logger.info("Creating tables from normalized JSONL files...")

    table_counts = {}

    for table_name, file_name in TABLES_TO_CREATE.items():
        file_path = os.path.join(NORMALIZED_DIR, file_name)
        if not os.path.exists(file_path):
            logger.warning(
                f"File {file_path} not found. Skipping table {table_name}.")
            continue

        # Safely drop existing table first
        safely_drop_table(conn, "bridge_kb_song_to_kexp")
        safely_drop_table(conn, "bridge_kb_artist_to_kexp")
        safely_drop_table(conn, table_name)

        sql_file_path = file_path.replace('\\', '/')

        # Determine if we need to use explicit schema or auto-detection
        if table_name in EXPLICIT_SCHEMAS:
            query_base_function = "read_json"
            cols_def = EXPLICIT_SCHEMAS[table_name]
            cols_str = ", ".join(
                [f"'{k}': '{v}'" for k, v in cols_def.items()])
            options = f"format='newline_delimited', auto_detect=False, columns={{{cols_str}}}"
        else:
            query_base_function = "read_json_auto"
            options = "format='newline_delimited', auto_detect=True"

        # Use CREATE TABLE instead of CREATE OR REPLACE since we manually dropped
        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM {query_base_function}('{sql_file_path}', {options});
        """

        logger.info(f"Creating table: {table_name} from {file_name}...")

        try:
            conn.execute(query)

            # Get row count
            count_result = conn.execute(
                f"SELECT COUNT(*) FROM {table_name}").fetchone()
            count = count_result[0] if count_result else 0
            table_counts[table_name] = count

            logger.info(
                f"Table {table_name} created successfully with {count:,} rows.")

        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")

    return table_counts


def apply_type_conversions(conn: duckdb.DuckDBPyConnection) -> None:
    """Apply type conversions to the created tables."""
    logger.info("Applying column type conversions...")

    for stmt in TYPE_CONVERSION_STATEMENTS:
        try:
            # Extract table name from the statement
            table_name_for_alter = stmt.split("ALTER TABLE ")[1].split(" ")[0]

            # Check if table exists before trying to alter it
            result = conn.execute(
                f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table_name_for_alter}'"
            ).fetchone()
            table_exists = result[0] > 0 if result else False

            if table_exists:
                logger.info(f"Executing: {stmt}")
                conn.execute(stmt)
                logger.info("Type conversion successful.")
            else:
                logger.warning(
                    f"Skipping alteration: Table '{table_name_for_alter}' does not exist.")

        except Exception as e:
            logger.error(f"Failed to execute type conversion: {stmt}")
            logger.error(f"Error: {e}")


def validate_ingestion(conn: duckdb.DuckDBPyConnection, table_counts: Dict[str, int]) -> bool:
    """Validate that the ingestion was successful."""
    logger.info("Validating ingestion results...")

    # Check that required tables exist and have data
    validation_passed = True

    for table_name in REQUIRED_TABLES:
        if table_name not in table_counts:
            logger.error(f"Required table {table_name} was not created.")
            validation_passed = False
        elif table_counts[table_name] == 0:
            logger.error(f"Required table {table_name} has 0 rows.")
            validation_passed = False

    # Check for relationship consistency
    try:
        # Check if play-to-artist mappings are consistent
        if "bridge_play_to_artist" in table_counts and "fact_plays" in table_counts:
            play_count = table_counts["fact_plays"]
            bridge_count = table_counts["bridge_play_to_artist"]

            if bridge_count == 0:
                logger.warning(
                    "bridge_play_to_artist has 0 rows - plays will not be linked to artists.")

            # Query to check if all play IDs in the bridge table exist in fact_plays
            valid_bridge_result = conn.execute("""
                SELECT 
                    COUNT(*) as total_bridges,
                    COUNT(CASE WHEN EXISTS (SELECT 1 FROM fact_plays p WHERE p.play_id = b.play_id) THEN 1 END) as valid_bridges
                FROM bridge_play_to_artist b
            """).fetchone()

            if valid_bridge_result:
                total_bridges, valid_bridges = valid_bridge_result
                if total_bridges > 0:
                    valid_pct = (valid_bridges * 100.0) / total_bridges
                    if valid_pct < 98:  # Allow for small inconsistencies
                        logger.warning(
                            f"Only {valid_pct:.1f}% of play-to-artist bridges match a valid play.")
                    else:
                        logger.info(
                            f"✅ {valid_pct:.1f}% of play-to-artist bridges are valid")

    except Exception as e:
        logger.error(f"Error during validation: {e}")
        validation_passed = False

    if validation_passed:
        logger.info("✅ Ingestion validation passed.")
    else:
        logger.error("❌ Ingestion validation failed.")

    return validation_passed


def ingest_normalized_data() -> bool:
    """
    Main function to ingest normalized KEXP JSONL data into DuckDB.
    Returns True if successful, False otherwise.
    """
    # Validate input files
    if not validate_input_files():
        return False

    logger.info(f"Connecting to DuckDB database: {DB_PATH}")
    conn: Optional[duckdb.DuckDBPyConnection] = None

    try:
        # Connect to the database
        conn = duckdb.connect(database=DB_PATH, read_only=False)
        conn.execute("PRAGMA enable_progress_bar = true")
        logger.info("Successfully connected to DuckDB.")

        # Create tables
        table_counts = create_tables(conn)

        # Apply type conversions
        apply_type_conversions(conn)

        # Validate ingestion
        validation_result = validate_ingestion(conn, table_counts)

        if validation_result:
            logger.info("🎉 KEXP data ingestion completed successfully.")
            return True
        else:
            logger.error(
                "❌ KEXP data ingestion completed with validation errors.")
            return False

    except Exception as e:
        logger.error(f"An error occurred during data ingestion: {e}")
        traceback.print_exc()
        return False

    finally:
        if conn:
            logger.info("Closing DuckDB connection.")
            conn.close()


if __name__ == '__main__':
    success = ingest_normalized_data()
    exit(0 if success else 1)
