#!/usr/bin/env python3
"""
Ingest BERTopic Modeling Results into DuckDB

This script reads the output files from a BERTopic analysis run
(specifically topic info, document-topic assignments, and hierarchy)
and ingests them into a DuckDB database for further analysis.
It handles multiple topic representations (Main, MMR, POS).
"""

import duckdb
import pandas as pd
import os
import argparse
import logging
from pathlib import Path

# --- Configuration ---
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
DEFAULT_RESULTS_DIR = Path("bertopic_kexp_results")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def connect_db(db_path: str) -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB database."""
    try:
        conn = duckdb.connect(db_path)
        logger.info(f"✅ Connected to database: {db_path}")
        return conn
    except Exception as e:
        logger.error(f"❌ Failed to connect to database: {e}")
        raise


def create_topic_tables(conn: duckdb.DuckDBPyConnection):
    """
    Create or update the necessary tables for storing topic modeling results.
    """
    logger.info("🏗️  Creating or updating topic modeling tables...")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS bertopic_run_id_seq START 1;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bertopic_runs (
            run_id INTEGER PRIMARY KEY DEFAULT nextval('bertopic_run_id_seq'),
            model_run_name VARCHAR UNIQUE NOT NULL,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Drop the old table if it exists to update the schema
    # A more advanced migration would use ALTER TABLE
    conn.execute("DROP TABLE IF EXISTS bertopic_topics;")
    conn.execute("""
        CREATE TABLE bertopic_topics (
            run_id INTEGER NOT NULL,
            topic_id INTEGER NOT NULL,
            name VARCHAR,
            count INTEGER,
            representation_main VARCHAR[],
            representation_mmr VARCHAR[],
            representation_pos VARCHAR[],
            representative_docs VARCHAR[],
            llm_summary VARCHAR,
            PRIMARY KEY (run_id, topic_id)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bridge_chunk_topic (
            run_id INTEGER NOT NULL,
            chunk_id BIGINT NOT NULL,
            topic_id INTEGER NOT NULL,
            PRIMARY KEY (run_id, chunk_id)
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bertopic_hierarchy (
            run_id INTEGER NOT NULL,
            parent_id INTEGER,
            parent_name VARCHAR,
            child_left_id INTEGER,
            child_left_name VARCHAR,
            child_right_id INTEGER,
            child_right_name VARCHAR,
            distance DOUBLE
        );
    """)
    logger.info("✅ Tables created or schema updated.")


def safe_eval_list(text: str) -> list:
    """Safely evaluate a string that should be a list, returning an empty list on failure."""
    if isinstance(text, str) and text.strip().startswith('['):
        try:
            return eval(text)
        except (SyntaxError, NameError, ValueError):
            return []  # Return empty list if eval fails
    return []


def ingest_data(conn: duckdb.DuckDBPyConnection, model_run_name: str, results_dir: Path):
    """
    Ingests all data associated with a given model run name from the specified directory.
    """
    logger.info(
        f"Processing model run: '{model_run_name}' from directory '{results_dir}'")
    conn.execute(
        "INSERT INTO bertopic_runs (model_run_name) VALUES (?) ON CONFLICT (model_run_name) DO NOTHING", (model_run_name,))
    run_id_result = conn.execute(
        "SELECT run_id FROM bertopic_runs WHERE model_run_name = ?", (model_run_name,)).fetchone()
    if not run_id_result:
        logger.error(f"Could not retrieve run_id for {model_run_name}")
        return
    run_id = run_id_result[0]
    logger.info(f"Using run_id: {run_id} for this ingestion.")

    # --- Ingest Topic Info ---
    topic_info_path = results_dir / f"{model_run_name}_topic_info.csv"
    if topic_info_path.exists():
        logger.info(f"Ingesting topic info from {topic_info_path}...")
        df_topics = pd.read_csv(topic_info_path)

        # Define renames for all potential columns
        column_renames = {
            'Topic': 'topic_id', 'Name': 'name', 'Count': 'count',
            'Representation': 'representation_main', 'MMR': 'representation_mmr',
            'POS': 'representation_pos', 'Representative_Docs': 'representative_docs'
        }
        df_topics.rename(columns=column_renames, inplace=True)

        # Define representation columns to parse
        repr_cols = ['representation_main', 'representation_mmr',
                     'representation_pos', 'representative_docs']
        for col in repr_cols:
            if col in df_topics.columns:
                df_topics[col] = df_topics[col].apply(safe_eval_list)
            else:
                # Add missing representation columns as empty lists
                df_topics[col] = [[] for _ in range(len(df_topics))]

        df_topics['run_id'] = run_id

        # Check for LLM summary in name and split it
        if 'LLM' in df_topics['name'].astype(str).iloc[0]:
            df_topics['llm_summary'] = df_topics['name'].apply(lambda x: x.split(
                'LLM: ')[1] if isinstance(x, str) and 'LLM: ' in x else None)
        else:
            df_topics['llm_summary'] = None

        # Ensure all columns for the table exist before inserting
        db_cols = ['run_id', 'topic_id', 'name', 'count', 'representation_main',
                   'representation_mmr', 'representation_pos', 'representative_docs', 'llm_summary']
        df_insert = df_topics[[c for c in db_cols if c in df_topics.columns]]

        # Clear old data for this run and insert new
        conn.execute("DELETE FROM bertopic_topics WHERE run_id = ?", (run_id,))
        conn.execute(f"INSERT INTO bertopic_topics SELECT * FROM df_insert")

        logger.info(
            f"✅ Ingested {len(df_insert)} topics with all representations.")
    else:
        logger.warning(
            f"File not found, skipping topic info: {topic_info_path}")

    # --- Ingest Document-Topic Assignments ---
    doc_topics_path = results_dir / f"{model_run_name}_document_topics.csv"
    if doc_topics_path.exists():
        logger.info(
            f"Ingesting document-topic assignments from {doc_topics_path}...")
        df_full_docs = pd.read_csv(doc_topics_path)

        if 'topic' in df_full_docs.columns:
            topic_col_name = 'topic'
        elif 'topic_x' in df_full_docs.columns:
            logger.warning(
                "Found 'topic_x' column, using it as the topic identifier. This is expected for reduced models.")
            topic_col_name = 'topic_x'
        else:
            logger.error(
                f"FATAL: Could not find 'topic' or 'topic_x' in {doc_topics_path}. Aborting assignment ingestion.")
            return

        df_docs = df_full_docs[['chunk_id', topic_col_name]].copy()
        df_docs.rename(columns={topic_col_name: 'topic_id'}, inplace=True)
        df_docs['run_id'] = run_id

        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE tmp_bridge AS SELECT * FROM df_docs")
        conn.execute("""
            INSERT INTO bridge_chunk_topic(run_id, chunk_id, topic_id)
            SELECT run_id, chunk_id, topic_id FROM tmp_bridge
            ON CONFLICT (run_id, chunk_id) DO UPDATE SET topic_id = EXCLUDED.topic_id;
        """)
        logger.info(
            f"✅ Ingested/Updated {len(df_docs)} chunk-topic assignments.")
    else:
        logger.warning(
            f"File not found, skipping assignments: {doc_topics_path}")

    # --- Ingest Topic Hierarchy ---
    hierarchy_path = results_dir / \
        f"{model_run_name}_hierarchical_topic_info.csv"
    if hierarchy_path.exists():
        logger.info(f"Ingesting hierarchy from {hierarchy_path}...")
        conn.execute(
            "DELETE FROM bertopic_hierarchy WHERE run_id = ?", (run_id,))
        df_hierarchy = pd.read_csv(hierarchy_path)
        df_hierarchy.rename(columns={
            'Parent_ID': 'parent_id', 'Parent_Name': 'parent_name',
            'Child_Left_ID': 'child_left_id', 'Child_Left_Name': 'child_left_name',
            'Child_Right_ID': 'child_right_id', 'Child_Right_Name': 'child_right_name',
            'Distance': 'distance'
        }, inplace=True)
        df_hierarchy['run_id'] = run_id

        cols_to_insert = ['run_id', 'parent_id', 'parent_name', 'child_left_id',
                          'child_left_name', 'child_right_id', 'child_right_name', 'distance']
        df_insert_hierarchy = df_hierarchy[[
            c for c in cols_to_insert if c in df_hierarchy.columns]]

        conn.execute(
            "INSERT INTO bertopic_hierarchy SELECT * FROM df_insert_hierarchy;")
        logger.info(f"✅ Ingested {len(df_hierarchy)} hierarchy relationships.")
    else:
        logger.warning(f"File not found, skipping hierarchy: {hierarchy_path}")


def main():
    """Main function to drive the ingestion process."""
    parser = argparse.ArgumentParser(
        description="Ingest BERTopic results into DuckDB.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "model_run_name",
        type=str,
        help="The file prefix for the model run to ingest.\n"
             "Example: bertopic_results_20250606_203045_reduced_llm"
    )
    parser.add_argument(
        '--results-dir',
        type=str,
        default=DEFAULT_RESULTS_DIR,
        help=f"The directory where the result files are stored. (default: {DEFAULT_RESULTS_DIR})"
    )
    args = parser.parse_args()

    results_path = Path(args.results_dir)
    conn = None
    try:
        conn = connect_db(DB_PATH)
        create_topic_tables(conn)
        ingest_data(conn, args.model_run_name, results_path)
    except Exception as e:
        logger.error(f"A critical error occurred: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("🔐 Database connection closed.")


if __name__ == "__main__":
    main()
