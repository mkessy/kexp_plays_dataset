import duckdb
from typing import List, Optional
from generate_comment_embeddings import EMBEDDING_DIM
import os
import numpy as np

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
TABLE_NAME = "chunk_embeddings"
EMBEDDING_COLUMN = "embedding"
INDEX_NAME = "idx_hnsw_chunk_embeddings"


def connect_db(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    print(f"Connecting to DuckDB at {db_path} ...")
    conn = duckdb.connect(db_path)  # type: ignore
    return conn


def vector_search(
    conn: duckdb.DuckDBPyConnection,
    query_vector: List[float],
    table: str = TABLE_NAME,
    column: str = EMBEDDING_COLUMN,
    top_k: int = 5,
    select_columns: Optional[List[str]] = None
):
    if select_columns is None:
        select_columns = ["chunk_id", "chunk_text", "play_id",
                          "strategy_id", "chunk_index", "embedding"]
    select_cols = ", ".join(select_columns)
    print(f"\n🔎 Running vector search for top {top_k} nearest neighbors...")
    query = f"""
        SELECT {select_cols},
               array_distance({column}, ?::FLOAT[{EMBEDDING_DIM}]) AS distance
        FROM {table}
        ORDER BY distance ASC
        LIMIT {top_k}
    """
    results = conn.execute(query, [query_vector]).fetchdf()
    print(f"✅ Retrieved {len(results)} results.")
    return results

# --- Batch Vector Search using vss_join ---


def batch_vector_search(
    conn: duckdb.DuckDBPyConnection,
    query_vectors: List[List[float]],
    table: str = TABLE_NAME,
    column: str = EMBEDDING_COLUMN,
    top_k: int = 5,
    select_columns: Optional[List[str]] = None
):
    print(
        f"\n🔎 Running batch vector search for {len(query_vectors)} queries, top {top_k} each...")
    # Create a temp table for the queries
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE tmp_queries (query_vec FLOAT[{}])".format(EMBEDDING_DIM))
    conn.executemany("INSERT INTO tmp_queries VALUES (?)",
                     [(v,) for v in query_vectors])
    # Use vss_join macro
    query = f"""
        SELECT *
        FROM vss_join(tmp_queries, {table}, query_vec, {column}, {top_k})
    """
    results = conn.execute(query).fetchdf()
    print(f"✅ Batch search complete. {len(results)} results.")
    # Clean up
    conn.execute("DROP TABLE IF EXISTS tmp_queries")
    return results


# --- Clustering Function ---
def cluster_embeddings(db_path: str, table_name: str, embedding_col: str = "embedding", n_clusters: int = 10):
    """
    Connect to DuckDB, fetch all embeddings from the specified table, 
    and perform KMeans clustering on them.
    """

    from scipy.cluster.vq import kmeans, whiten

    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    # Fetch embeddings as a list of lists
    query = f"SELECT {embedding_col} FROM {table_name};"
    df = conn.execute(query).fetchdf()
    # Convert DataFrame column of arrays to a 2D numpy array
    embeddings = np.vstack(df[embedding_col].values)
    # Whiten the embeddings
    embeddings = whiten(embeddings)
    # Run KMeans clustering
    centroids, _ = kmeans(embeddings, n_clusters)
    labels = np.argmin(np.linalg.norm(embeddings - centroids, axis=1), axis=1)
    # Optionally, write cluster labels back to DuckDB in a new table
    df_labels = df.copy()
    df_labels["cluster_label"] = labels
    # Create a new table or replace existing
    conn.execute(
        f"CREATE OR REPLACE TABLE {table_name}_clusters AS SELECT rowid AS id, cluster_label FROM df_labels;")
    print(
        f"✅ Clustering complete. Found {n_clusters} clusters and stored labels in `{table_name}_clusters` table.")
    conn.close()


# --- Main Demo ---


def main():
    conn = connect_db()
    try:
        cluster_embeddings(DB_PATH, TABLE_NAME)
    finally:
        conn.close()
        print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
