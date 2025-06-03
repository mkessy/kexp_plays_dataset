#!/usr/bin/env python3
"""
Generates vector embeddings for DJ comment chunks from the KEXP dataset
using MLX embedding models and stores them in DuckDB.

This script is designed to be run on a Mac with Apple Silicon,
as MLX is optimized for this hardware.
"""

import duckdb
import sys
import os
import time
import pandas as pd  # For fetching data in chunks easily
from dotenv import load_dotenv
from mlx_embeddings.utils import load as load_mlx_model
import argparse

# --- Configuration ---
load_dotenv()  # Load variables from .env file

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME",
                       "mlx-community/nomicai-modernbert-embed-base-4bit")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIMENSION", 768))
BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", 256))
CHUNK_EMBEDDING_TABLE_NAME = os.getenv(
    "CHUNK_EMBEDDING_TABLE_NAME", "chunk_embeddings")
CONSERVATIVE_STRATEGY_ID = 3  # As determined by analysis

# Quality filters for chunks
MIN_CHUNK_LENGTH = int(os.getenv("MIN_CHUNK_LENGTH", 10))
MIN_ALPHA_RATIO = float(os.getenv("MIN_ALPHA_RATIO", 0.3))
MIN_ALPHANUM_RATIO = float(os.getenv("MIN_ALPHANUM_RATIO", 0.5))

SQL_CREATE_CHUNK_EMBEDDINGS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {CHUNK_EMBEDDING_TABLE_NAME} (
    chunk_id INTEGER PRIMARY KEY,
    embedding FLOAT[{EMBEDDING_DIM}] NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SQL_CREATE_CHUNK_EMBEDDINGS_VIEW = f"""
CREATE VIEW IF NOT EXISTS chunk_embeddings_with_metadata AS
SELECT
    ce.chunk_id,
    ce.embedding,
    ce.created_at,
    c.play_id,
    c.strategy_id,
    c.chunk_index,
    c.chunk_text,
    c.normalized_chunk_text,
    c.chunk_length,
    c.alpha_ratio,
    c.alphanum_ratio,
    c.is_url_only,
    c.contains_url,
    fp.original_artist_text,
    fp.original_song_text,
    fp.airdate_iso
FROM {CHUNK_EMBEDDING_TABLE_NAME} ce
JOIN comment_chunks_raw c ON ce.chunk_id = c.chunk_id
JOIN fact_plays fp ON c.play_id = fp.play_id
WHERE c.strategy_id = {CONSERVATIVE_STRATEGY_ID};
"""


def connect_db(db_path: str) -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB database and ensure schema exists."""
    try:
        conn = duckdb.connect(db_path)
        print(f"✅ Connected to database: {db_path}")

        # Ensure unique index on chunk_id for foreign key constraint
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_comment_chunks_raw_chunk_id ON comment_chunks_raw(chunk_id);
        """)
        print(f"✅ Ensured unique index on comment_chunks_raw(chunk_id) exists.")

        # Create table and view if they don't exist
        conn.execute(SQL_CREATE_CHUNK_EMBEDDINGS_TABLE)
        print(f"✅ Ensured table '{CHUNK_EMBEDDING_TABLE_NAME}' exists.")
        conn.execute(SQL_CREATE_CHUNK_EMBEDDINGS_VIEW)
        print(f"✅ Ensured view 'chunk_embeddings_with_metadata' exists.")

        # Verify the target table and its embedding column exist
        conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{CHUNK_EMBEDDING_TABLE_NAME}' AND column_name = 'embedding';
        """)
        result = conn.fetchone()
        if not result:
            print(
                f"❌ Error: Table '{CHUNK_EMBEDDING_TABLE_NAME}' or column 'embedding' not found even after creation attempt.")
            sys.exit(1)

        if 'FLOAT[]' not in result[1].upper() and 'ARRAY(FLOAT)' not in result[1].upper() and f'FLOAT[{EMBEDDING_DIM}]' not in result[1].upper():
            print(
                f"⚠️ Warning: Column 'embedding' in '{CHUNK_EMBEDDING_TABLE_NAME}' does not appear to be a float array type (expected FLOAT[{EMBEDDING_DIM}], got {result[1]}).")
            print(
                f"   This might cause issues during insertion. Please verify the table schema.")

        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database or verify/create schema: {e}")
        sys.exit(1)


def get_already_embedded_chunk_ids(conn: duckdb.DuckDBPyConnection) -> set[int]:
    """Fetch all chunk_ids that are already in the embeddings table."""
    try:
        return set(row[0] for row in conn.execute(f"SELECT chunk_id FROM {CHUNK_EMBEDDING_TABLE_NAME}").fetchall())
    except Exception as e:
        print(
            f"⚠️ Warning: Could not fetch existing embedded chunk IDs from {CHUNK_EMBEDDING_TABLE_NAME}. Assuming none exist. Error: {e}")
        return set()


def load_embedding_model(model_name: str):
    """Load the MLX embedding model and tokenizer."""
    print(f"🤖 Loading embedding model: {model_name}...")
    try:
        model, tokenizer = load_mlx_model(model_name)
        print("✅ Model and tokenizer loaded successfully.")
        return model, tokenizer
    except Exception as e:
        print(f"❌ Failed to load MLX model '{model_name}': {e}")
        print("   Ensure the model name is correct and accessible, and MLX is set up correctly.")
        sys.exit(1)


def fetch_chunks_for_embedding(conn: duckdb.DuckDBPyConnection,
                               batch_size: int,
                               offset: int) -> pd.DataFrame:
    """Fetch a batch of chunk_ids and chunk_texts from comment_chunks_raw that need embedding."""
    query = f"""
        SELECT chunk_id, chunk_text
        FROM comment_chunks_raw
        WHERE strategy_id = {CONSERVATIVE_STRATEGY_ID}
          AND chunk_length >= {MIN_CHUNK_LENGTH}
          AND NOT is_url_only
          AND alpha_ratio >= {MIN_ALPHA_RATIO}
          AND alphanum_ratio >= {MIN_ALPHANUM_RATIO}
          AND chunk_id NOT IN (SELECT chunk_id FROM {CHUNK_EMBEDDING_TABLE_NAME})
        ORDER BY chunk_id -- Important for consistent batching if offset is used, though current main loop doesn't increment offset
        LIMIT {batch_size} OFFSET {offset}
    """
    # print(f"Executing query: {query[:300]}...") # For debugging
    return conn.execute(query).fetchdf()


def count_total_pending_chunks(conn: duckdb.DuckDBPyConnection) -> int:
    """Counts total chunks that meet criteria and need to be embedded."""
    try:
        result = conn.execute(f"""
            SELECT COUNT(ccr.chunk_id)
            FROM comment_chunks_raw ccr
            LEFT JOIN {CHUNK_EMBEDDING_TABLE_NAME} cet ON ccr.chunk_id = cet.chunk_id
            WHERE ccr.strategy_id = {CONSERVATIVE_STRATEGY_ID}
              AND ccr.chunk_length >= {MIN_CHUNK_LENGTH}
              AND NOT ccr.is_url_only
              AND ccr.alpha_ratio >= {MIN_ALPHA_RATIO}
              AND ccr.alphanum_ratio >= {MIN_ALPHANUM_RATIO}
              AND cet.chunk_id IS NULL
        """).fetchone()
        return result[0] if result else 0
    except Exception as e:
        print(f"❌ Error counting pending chunks: {e}")
        return 0


def generate_embeddings_batch(model, tokenizer, texts: list[str]) -> list[list[float]]:
    """Generate embeddings for a batch of texts using MLX model and tokenizer."""
    if not texts:
        return []
    try:
        # Tokenize the batch
        inputs = tokenizer.batch_encode_plus(
            texts,
            return_tensors="mlx",
            padding=True,
            truncation=True,
            max_length=256
        )
        # Forward pass
        outputs = model(**inputs)
        # Get mean pooled, normalized embeddings
        embeddings_mx = outputs.text_embeds
        # Convert to Python lists
        embeddings_list = [emb.tolist() for emb in embeddings_mx]
        if embeddings_list and len(embeddings_list[0]) != EMBEDDING_DIM:
            print(
                f"❌ Critical Error: Embedding dimension mismatch! Expected {EMBEDDING_DIM}, got {len(embeddings_list[0])}.")
            sys.exit(1)
        return embeddings_list
    except Exception as e:
        print(f"❌ Error during embedding generation: {e}")
        return []


def insert_chunk_embeddings_to_db(conn: duckdb.DuckDBPyConnection, chunk_ids: list[int], embeddings: list[list[float]]):
    """Insert generated embeddings into the DuckDB table."""
    if not chunk_ids or not embeddings or len(chunk_ids) != len(embeddings):
        print("⚠️ No data to insert or mismatched chunk_ids and embeddings count.")
        return 0

    try:
        data_to_insert = list(zip(chunk_ids, embeddings))
        conn.executemany(
            f"INSERT INTO {CHUNK_EMBEDDING_TABLE_NAME} (chunk_id, embedding) VALUES (?, ?)", data_to_insert)
        return len(chunk_ids)
    except Exception as e:
        print(f"❌ Error inserting chunk embeddings into database: {e}")
        print("   Problematic chunk_ids (first 5):", chunk_ids[:5])
        return 0


def prewarm_mlx(model, tokenizer):
    dummy = ["Prewarm embedding run."]
    inputs = tokenizer.batch_encode_plus(
        dummy,
        return_tensors="mlx",
        padding=True,
        truncation=True,
        max_length=256
    )
    _ = model(**inputs)
    print("✅ MLX kernels prewarmed.")


def fetch_and_bucket_chunks(conn, tokenizer, desired_batch):
    # Fetch a large pool to allow bucketing
    pool_size = desired_batch * 4
    rows = conn.execute(f"""
        SELECT chunk_id, chunk_text
        FROM comment_chunks_raw
        WHERE strategy_id = {CONSERVATIVE_STRATEGY_ID}
          AND chunk_length >= {MIN_CHUNK_LENGTH}
          AND NOT is_url_only
          AND alpha_ratio >= {MIN_ALPHA_RATIO}
          AND alphanum_ratio >= {MIN_ALPHANUM_RATIO}
          AND chunk_id NOT IN (SELECT chunk_id FROM {CHUNK_EMBEDDING_TABLE_NAME})
        ORDER BY chunk_id
        LIMIT {pool_size}
    """).fetchdf()
    if rows.empty:
        return rows  # Empty DataFrame

    # Tokenize to get token counts
    token_counts = [
        len(tokenizer.encode(text, truncation=True, max_length=512))
        for text in rows['chunk_text']
    ]
    rows['token_count'] = token_counts

    # Buckets
    buckets = {
        'A': [],  # ≤ 64 tokens
        'B': [],  # 65–128
        'C': [],  # 129–256
        'D': []   # >256
    }
    for idx, row in rows.iterrows():
        L = row['token_count']
        if L <= 64:
            buckets['A'].append(row)
        elif 65 <= L <= 128:
            buckets['B'].append(row)
        elif 129 <= L <= 256:
            buckets['C'].append(row)
        else:
            buckets['D'].append(row)

    # Priority: A (512), B (256), C (128), D (64)
    bucket_order = [
        ('A', 512),
        ('B', 256),
        ('C', 128),
        ('D', 64)
    ]
    for bucket_name, max_batch in bucket_order:
        bucket = buckets[bucket_name]
        if len(bucket) >= max_batch:
            selected = bucket[:max_batch]
            return pd.DataFrame(selected)
    # If no bucket has enough, return the largest available bucket (if any)
    for bucket_name, _ in bucket_order:
        bucket = buckets[bucket_name]
        if len(bucket) > 0:
            return pd.DataFrame(bucket)
    # Fallback: return empty
    return pd.DataFrame([])


def main():
    """Main script execution."""
    parser = argparse.ArgumentParser(
        description="Generate embeddings for KEXP comment chunks.")
    parser.add_argument('--overwrite', action='store_true',
                        help='Drop and recreate the chunk_embeddings table before embedding generation.')
    args = parser.parse_args()

    print("🚀 Starting KEXP Comment Chunk Embedding Generation...")
    print(f"   Database: {DB_PATH}")
    print(f"   Model: {MODEL_NAME}")
    print(f"   Embedding Dimension: {EMBEDDING_DIM}")
    print(f"   Batch Size: {BATCH_SIZE}")
    print(f"   Target Table: {CHUNK_EMBEDDING_TABLE_NAME}")
    print(f"   Chunk Strategy ID: {CONSERVATIVE_STRATEGY_ID}")
    print(
        f"   Quality Filters: Min Length={MIN_CHUNK_LENGTH}, Min Alpha={MIN_ALPHA_RATIO}, Min Alphanum={MIN_ALPHANUM_RATIO}, Not URL-only")

    conn = connect_db(DB_PATH)

    if args.overwrite:
        print(
            f"⚠️  Overwrite mode enabled: Dropping and recreating table '{CHUNK_EMBEDDING_TABLE_NAME}'.")
        conn.execute(f"DROP TABLE IF EXISTS {CHUNK_EMBEDDING_TABLE_NAME};")
        conn.execute(SQL_CREATE_CHUNK_EMBEDDINGS_TABLE)
        print(f"✅ Table '{CHUNK_EMBEDDING_TABLE_NAME}' recreated.")
        # Also recreate the view
        conn.execute(SQL_CREATE_CHUNK_EMBEDDINGS_VIEW)
        print(f"✅ View 'chunk_embeddings_with_metadata' recreated.")

    model, tokenizer = load_embedding_model(MODEL_NAME)
    prewarm_mlx(model, tokenizer)

    total_pending_initial = count_total_pending_chunks(conn)
    if total_pending_initial == 0:
        print("✅ No new chunks found to embed. Database is up-to-date for the conservative strategy with quality filters.")
        conn.close()
        return

    print(f"Total chunks to process: {total_pending_initial:,}")

    processed_count_session = 0
    start_time_session = time.time()

    while True:
        batch_df = fetch_and_bucket_chunks(conn, tokenizer, BATCH_SIZE)
        if batch_df.empty:
            print("✅ No more chunks to process in this run.")
            break
        chunk_ids_batch = batch_df['chunk_id'].tolist()
        chunk_texts_batch = batch_df['chunk_text'].tolist()
        print(f"   Processing batch of {len(chunk_texts_batch)} chunks...")
        batch_start_time = time.time()
        embeddings_batch = generate_embeddings_batch(
            model, tokenizer, chunk_texts_batch)
        batch_end_time = time.time()
        if not embeddings_batch or len(embeddings_batch) != len(chunk_ids_batch):
            print(
                f"⚠️ Skipping batch due to embedding generation error or count mismatch.")
            if not batch_df.empty:
                print(f"Failed to process chunk_ids: {chunk_ids_batch[:5]}")
            continue
        inserted_count = insert_chunk_embeddings_to_db(
            conn, chunk_ids_batch, embeddings_batch)
        if inserted_count > 0:
            processed_count_session += inserted_count
            total_pending_now = count_total_pending_chunks(conn)
            elapsed_time_batch = batch_end_time - batch_start_time
            print(
                f"   Batch processed in {elapsed_time_batch:.2f}s. Inserted {inserted_count} chunk embeddings.")
            print(
                f"   Session total: {processed_count_session:,}. Remaining (approx): {total_pending_now:,}")
        else:
            print(
                f"⚠️ Failed to insert embeddings for the current batch of {len(chunk_ids_batch)} items.")
            print(
                f"   Problematic chunk_ids (first 5 of this batch): {chunk_ids_batch[:5]}")
    session_duration = time.time() - start_time_session
    print(f"\n🎉 Chunk embedding generation complete for this session.")
    print(
        f"   Processed {processed_count_session:,} chunks in {session_duration:.2f} seconds.")
    final_pending_count = count_total_pending_chunks(conn)
    if final_pending_count == 0:
        print("✅ All eligible chunks have been successfully embedded.")
    else:
        print(
            f"⚠️ {final_pending_count:,} chunks still pending (possibly due to errors during processing).")
    conn.close()
    print("🔐 Database connection closed.")


if __name__ == "__main__":
    main()
