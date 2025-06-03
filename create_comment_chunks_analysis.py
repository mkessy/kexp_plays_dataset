#!/usr/bin/env python3
"""
Comment Chunks Analysis Script

This script creates tables and views for analyzing different comment chunking strategies
using DuckDB's built-in string processing capabilities. It consolidates the comment
parsing logic from normalization.py and comment_parser.py into a DuckDB-based solution.
"""

import duckdb
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration from .env
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


def connect_to_database(db_path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB database."""
    try:
        conn = duckdb.connect(db_path)
        print(f"✅ Connected to database: {db_path}")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)


def create_comment_normalization_functions(conn: duckdb.DuckDBPyConnection) -> None:
    """Create DuckDB functions for comment normalization."""

    print("\n🔧 Creating comment normalization functions...")

    # Create a macro for text normalization (similar to normalize_text from normalization.py)
    conn.execute("""
        CREATE OR REPLACE MACRO normalize_comment_text(text) AS
        CASE 
            WHEN text IS NULL THEN ''
            ELSE 
                -- Remove leading/trailing whitespace after all replacements
                TRIM(
                    -- Normalize all whitespace to single spaces
                    regexp_replace(
                        -- Replace various dash types with spaces
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        CAST(text AS VARCHAR),
                                        '---', ' ', 'g'
                                    ),
                                    '--', ' ', 'g'
                                ),
                                '—', ' ', 'g'  -- em-dash
                            ),
                            '–', ' ', 'g'  -- en-dash
                        ),
                        '\\s+', ' ', 'g'  -- Multiple spaces to single space
                    )
                )
        END
    """)

    # Create a macro to detect if text is URL-only
    conn.execute("""
        CREATE OR REPLACE MACRO is_url_only(text) AS
        CASE
            WHEN text IS NULL OR LENGTH(text) = 0 THEN FALSE
            ELSE regexp_matches(
                text, 
                '^https?://[^\\s/$.?#].[^\\s]*$|^[a-zA-Z0-9.-]+\\.(?:com|org|net|edu|gov|io|ly|eu|info|biz|ws|us|ca|uk|au|de|jp|fr|ch|fm|tv|me|sh|stream|live|watch|listen|download|video|audio|pics|photo|img|image|gallery|news|blog|shop|store|app|co|info|online|site|website|xyz|club|dev|page|link|art|bandcamp|soundcloud|spotify|youtube|youtu\\.be|vimeo|tiktok|instagram|facebook|twitter|patreon|kexp)(?:/[^\\s]*)?$'
            )
        END
    """)

    # Create a macro to check if text contains URLs
    conn.execute("""
        CREATE OR REPLACE MACRO contains_url(text) AS
        CASE
            WHEN text IS NULL OR LENGTH(text) = 0 THEN FALSE
            ELSE regexp_matches(
                text,
                'https?://[^\\s/$.?#].[^\\s]*|[a-zA-Z0-9.-]+\\.(?:com|org|net|edu|gov|io|ly|eu|info|biz|ws|us|ca|uk|au|de|jp|fr|ch|fm|tv|me|sh|stream|live|watch|listen|download|video|audio|pics|photo|img|image|gallery|news|blog|shop|store|app|co|info|online|site|website|xyz|club|dev|page|link|art|bandcamp|soundcloud|spotify|youtube|youtu\\.be|vimeo|tiktok|instagram|facebook|twitter|patreon|kexp)(?:/[^\\s]*)?'
            )
        END
    """)

    print("✅ Normalization functions created!")


def create_comment_chunks_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for storing comment chunks with different splitting strategies."""

    print("\n🏗️ Creating comment chunks tables...")

    # Drop existing tables if they exist
    conn.execute("DROP TABLE IF EXISTS comment_chunks_raw CASCADE")
    conn.execute("DROP TABLE IF EXISTS comment_splitting_strategies CASCADE")
    conn.execute("DROP SEQUENCE IF EXISTS chunk_id_seq")

    # Create splitting strategies reference table
    conn.execute("""
        CREATE TABLE comment_splitting_strategies (
            strategy_id INTEGER PRIMARY KEY,
            strategy_name VARCHAR NOT NULL,
            description VARCHAR,
            split_pattern VARCHAR NOT NULL
        )
    """)

    # Insert splitting strategies
    conn.execute("""
        INSERT INTO comment_splitting_strategies VALUES
        (1, 'standard', 'Split on double newlines or newline with dashes', '\\n\\n+|\\n--+\\n'),
        (2, 'aggressive', 'Split on any newline', '\\n+'),
        (3, 'conservative', 'Split only on triple newlines or newline-triple-dash-newline', '\\n\\n\\n+|\\n---+\\n'),
        (4, 'double_newline', 'Split only on double newlines', '\\n\\n+')
    """)

    # Create raw chunks table
    conn.execute("""
        CREATE TABLE comment_chunks_raw (
            chunk_id BIGINT,
            play_id BIGINT NOT NULL,
            strategy_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_text VARCHAR NOT NULL,
            chunk_length INTEGER NOT NULL,
            normalized_chunk_text VARCHAR NOT NULL,
            is_url_only BOOLEAN NOT NULL,
            contains_url BOOLEAN NOT NULL,
            alpha_ratio DOUBLE,
            alphanum_ratio DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (play_id, strategy_id, chunk_index)
        )
    """)

    # Create sequence for chunk_id
    conn.execute("CREATE SEQUENCE IF NOT EXISTS chunk_id_seq START 1")

    print("✅ Comment chunks tables created!")


def populate_comment_chunks(conn: duckdb.DuckDBPyConnection, strategy_id: int, strategy_name: str, split_pattern: str) -> None:
    """Populate comment chunks for a specific splitting strategy."""

    print(f"\n📝 Processing chunks for strategy: {strategy_name}...")

    # Clear existing data for this strategy
    conn.execute(
        f"DELETE FROM comment_chunks_raw WHERE strategy_id = {strategy_id}")

    # Use a more robust splitting approach with DuckDB
    query = f"""
        INSERT INTO comment_chunks_raw (chunk_id, play_id, strategy_id, chunk_index, chunk_text, 
                                       chunk_length, normalized_chunk_text, is_url_only, 
                                       contains_url, alpha_ratio, alphanum_ratio)
        WITH normalized_comments AS (
            SELECT 
                play_id,
                comment as original_comment,
                normalize_comment_text(comment) as normalized_comment
            FROM fact_plays
            WHERE comment IS NOT NULL 
              AND LENGTH(TRIM(comment)) > 0
        ),
        split_chunks AS (
            SELECT 
                play_id,
                original_comment,
                normalized_comment,
                chunk,
                ROW_NUMBER() OVER (PARTITION BY play_id ORDER BY chunk_order) as chunk_index
            FROM (
                SELECT 
                    play_id,
                    original_comment,
                    normalized_comment,
                    unnest(string_split_regex(original_comment, '{split_pattern}')) as chunk,
                    generate_series as chunk_order
                FROM normalized_comments,
                     LATERAL generate_series(1, array_length(string_split_regex(original_comment, '{split_pattern}')))
            ) sub
        ),
        chunk_analysis AS (
            SELECT
                nextval('chunk_id_seq') as chunk_id,
                play_id,
                {strategy_id} as strategy_id,
                chunk_index,
                chunk as chunk_text,
                LENGTH(chunk) as chunk_length,
                normalize_comment_text(chunk) as normalized_chunk_text,
                is_url_only(chunk) as is_url_only,
                contains_url(chunk) as contains_url,
                CASE 
                    WHEN LENGTH(chunk) = 0 THEN 0
                    ELSE LENGTH(regexp_replace(chunk, '[^a-zA-Z]', '', 'g')) * 1.0 / LENGTH(chunk)
                END as alpha_ratio,
                CASE 
                    WHEN LENGTH(chunk) = 0 THEN 0
                    ELSE LENGTH(regexp_replace(chunk, '[^a-zA-Z0-9]', '', 'g')) * 1.0 / LENGTH(chunk)
                END as alphanum_ratio
            FROM split_chunks
            WHERE LENGTH(TRIM(chunk)) > 0  -- Filter out empty chunks
        )
        SELECT * FROM chunk_analysis
    """

    try:
        result = conn.execute(query)
        row = result.fetchone()
        count = row[0] if row else 0
        print(f"✅ Inserted {count:,} chunks for strategy: {strategy_name}")
    except Exception as e:
        print(f"❌ Error processing strategy {strategy_name}: {e}")


def create_analysis_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create views for analyzing comment chunks."""

    print("\n🔍 Creating analysis views...")

    # View for chunk statistics by strategy
    conn.execute("""
        CREATE OR REPLACE VIEW view_chunk_stats_by_strategy AS
        SELECT 
            s.strategy_name,
            COUNT(DISTINCT c.play_id) as comments_processed,
            COUNT(*) as total_chunks,
            AVG(c.chunk_length) as avg_chunk_length,
            MEDIAN(c.chunk_length) as median_chunk_length,
            MIN(c.chunk_length) as min_chunk_length,
            MAX(c.chunk_length) as max_chunk_length,
            SUM(CASE WHEN c.is_url_only THEN 1 ELSE 0 END) as url_only_chunks,
            SUM(CASE WHEN c.contains_url THEN 1 ELSE 0 END) as chunks_with_urls,
            AVG(c.alpha_ratio) as avg_alpha_ratio,
            AVG(c.alphanum_ratio) as avg_alphanum_ratio
        FROM comment_chunks_raw c
        JOIN comment_splitting_strategies s ON c.strategy_id = s.strategy_id
        GROUP BY s.strategy_name, s.strategy_id
        ORDER BY s.strategy_id
    """)

    # View for chunk distribution analysis
    conn.execute("""
        CREATE OR REPLACE VIEW view_chunks_per_comment_distribution AS
        SELECT 
            s.strategy_name,
            chunks_per_comment,
            COUNT(*) as comment_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY s.strategy_name), 2) as percentage
        FROM (
            SELECT 
                strategy_id,
                play_id,
                COUNT(*) as chunks_per_comment
            FROM comment_chunks_raw
            GROUP BY strategy_id, play_id
        ) chunk_counts
        JOIN comment_splitting_strategies s ON chunk_counts.strategy_id = s.strategy_id
        GROUP BY s.strategy_name, chunks_per_comment
        ORDER BY s.strategy_name, chunks_per_comment
    """)

    # View for filtered chunks (applying quality criteria)
    conn.execute("""
        CREATE OR REPLACE VIEW view_filtered_chunks AS
        SELECT 
            c.*,
            s.strategy_name,
            p.airdate_iso,
            p.show_id,
            p.original_artist_text,
            p.original_song_text
        FROM comment_chunks_raw c
        JOIN comment_splitting_strategies s ON c.strategy_id = s.strategy_id
        JOIN fact_plays p ON c.play_id = p.play_id
        WHERE 
            c.chunk_length >= 10  -- Minimum length
            AND NOT c.is_url_only  -- Not just a URL
            AND c.alpha_ratio >= 0.3  -- At least 30% alphabetic
            AND c.alphanum_ratio >= 0.5  -- At least 50% alphanumeric
    """)

    # View for sample chunks by strategy
    conn.execute("""
        CREATE OR REPLACE VIEW view_sample_chunks AS
        SELECT 
            s.strategy_name,
            c.play_id,
            c.chunk_index,
            c.chunk_length,
            c.normalized_chunk_text,
            c.is_url_only,
            c.contains_url,
            ROUND(c.alpha_ratio, 3) as alpha_ratio,
            ROUND(c.alphanum_ratio, 3) as alphanum_ratio
        FROM comment_chunks_raw c
        JOIN comment_splitting_strategies s ON c.strategy_id = s.strategy_id
        WHERE c.chunk_length BETWEEN 20 AND 200  -- Reasonable length for inspection
        ORDER BY RANDOM()
        LIMIT 100
    """)

    print("✅ Analysis views created!")


def run_analysis_queries(conn: duckdb.DuckDBPyConnection) -> None:
    """Run analysis queries to understand chunking results."""

    print("\n📊 Running analysis queries...")

    # Overall statistics by strategy
    print("\n📈 Chunk Statistics by Strategy:")
    results = conn.execute("""
        SELECT * FROM view_chunk_stats_by_strategy
    """).fetchall()

    if results:
        print(f"{'Strategy':<15} {'Comments':<10} {'Chunks':<10} {'Avg Len':<10} {'URL Only':<10} {'With URLs':<10}")
        print("-" * 65)
        for row in results:
            print(
                f"{row[0]:<15} {row[1]:<10,} {row[2]:<10,} {row[3]:<10.1f} {row[7]:<10,} {row[8]:<10,}")

    # Chunks per comment distribution
    print("\n📊 Chunks per Comment Distribution (top values):")
    results = conn.execute("""
        SELECT strategy_name, chunks_per_comment, comment_count, percentage
        FROM view_chunks_per_comment_distribution
        WHERE chunks_per_comment <= 10
        ORDER BY strategy_name, chunks_per_comment
        LIMIT 20
    """).fetchall()

    current_strategy = None
    for row in results:
        if row[0] != current_strategy:
            current_strategy = row[0]
            print(f"\n{current_strategy}:")
            print(f"  {'Chunks':<10} {'Count':<10} {'%':<10}")
            print("  " + "-" * 30)
        print(f"  {row[1]:<10} {row[2]:<10,} {row[3]:<10.1f}%")

    # Sample of filtered chunks
    print("\n📝 Sample of Filtered Chunks:")
    results = conn.execute("""
        SELECT 
            strategy_name,
            play_id,
            chunk_index,
            chunk_length,
            LEFT(normalized_chunk_text, 100) || '...' as chunk_preview
        FROM view_filtered_chunks
        ORDER BY RANDOM()
        LIMIT 5
    """).fetchall()

    for row in results:
        print(f"\nStrategy: {row[0]}, Play ID: {row[1]}, Chunk {row[2]}")
        print(f"Length: {row[3]} chars")
        print(f"Text: {row[4]}")


def main():
    """Main function to create and analyze comment chunks."""

    print("🎵 KEXP Comment Chunks Analysis")
    print("=" * 40)

    # Connect to database
    conn = connect_to_database()

    try:
        # Create normalization functions
        create_comment_normalization_functions(conn)

        # Create tables
        create_comment_chunks_tables(conn)

        # Get splitting strategies
        strategies = conn.execute("""
            SELECT strategy_id, strategy_name, split_pattern 
            FROM comment_splitting_strategies
            ORDER BY strategy_id
        """).fetchall()

        # Populate chunks for each strategy
        for strategy_id, strategy_name, split_pattern in strategies:
            populate_comment_chunks(
                conn, strategy_id, strategy_name, split_pattern)

        # Create analysis views
        create_analysis_views(conn)

        # Run analysis
        run_analysis_queries(conn)

        print("\n🎉 Comment chunks analysis complete!")
        print("\nYou can now query the following tables and views:")
        print("  - comment_chunks_raw (all chunks)")
        print("  - view_chunk_stats_by_strategy (statistics)")
        print("  - view_chunks_per_comment_distribution (distribution)")
        print("  - view_filtered_chunks (quality-filtered chunks)")
        print("  - view_sample_chunks (random samples)")

    except Exception as e:
        print(f"❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        conn.close()
        print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
