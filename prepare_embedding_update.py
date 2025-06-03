#!/usr/bin/env python3
"""
Preparation script for updating embedding generation to use conservative chunking strategy.
This script analyzes the chunk data and prepares the update requirements.
"""

import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


def analyze_conservative_chunks():
    """Analyze the conservative strategy chunks to prepare for embedding generation."""
    conn = duckdb.connect(DB_PATH)

    print("🔍 Analyzing Conservative Strategy Chunks")
    print("=" * 60)

    # Get conservative strategy ID
    strategy_info = conn.execute("""
        SELECT strategy_id, strategy_name, description 
        FROM comment_splitting_strategies 
        WHERE strategy_name = 'conservative'
    """).fetchone()

    print(f"\nStrategy: {strategy_info[1]} (ID: {strategy_info[0]})")
    print(f"Description: {strategy_info[2]}")

    # Get chunk statistics
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_chunks,
            COUNT(DISTINCT play_id) as unique_plays,
            AVG(chunk_length) as avg_length,
            MIN(chunk_length) as min_length,
            MAX(chunk_length) as max_length,
            SUM(CASE WHEN is_url_only THEN 1 ELSE 0 END) as url_only_chunks,
            SUM(CASE WHEN chunk_length >= 10 
                     AND NOT is_url_only 
                     AND alpha_ratio >= 0.3 
                     AND alphanum_ratio >= 0.5 
                THEN 1 ELSE 0 END) as quality_chunks
        FROM comment_chunks_raw
        WHERE strategy_id = 3
    """).fetchone()

    print(f"\n📊 Chunk Statistics:")
    print(f"  Total chunks: {stats[0]:,}")
    print(f"  Unique plays: {stats[1]:,}")
    print(f"  Average length: {stats[2]:.1f} chars")
    print(f"  Length range: {stats[3]} - {stats[4]} chars")
    print(f"  URL-only chunks: {stats[5]:,} ({stats[5]/stats[0]*100:.1f}%)")
    print(f"  Quality chunks: {stats[6]:,} ({stats[6]/stats[0]*100:.1f}%)")

    # Sample chunks
    print(f"\n📝 Sample Quality Chunks:")
    samples = conn.execute("""
        SELECT 
            chunk_id,
            play_id,
            chunk_index,
            chunk_text,
            chunk_length,
            alpha_ratio,
            alphanum_ratio
        FROM comment_chunks_raw
        WHERE strategy_id = 3
          AND chunk_length >= 10 
          AND NOT is_url_only 
          AND alpha_ratio >= 0.3 
          AND alphanum_ratio >= 0.5
        ORDER BY RANDOM()
        LIMIT 3
    """).fetchall()

    for i, sample in enumerate(samples, 1):
        print(f"\n  Sample {i}:")
        print(f"    Chunk ID: {sample[0]}")
        print(f"    Play ID: {sample[1]}")
        print(f"    Chunk Index: {sample[2]}")
        print(f"    Length: {sample[4]} chars")
        print(f"    Alpha ratio: {sample[5]:.3f}")
        print(f"    Text preview: {sample[3][:100]}...")

    # Check existing embeddings table structure
    print(f"\n🔧 Embedding Table Requirements:")
    try:
        table_info = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'play_comment_embeddings'
            ORDER BY ordinal_position
        """).fetchall()

        if table_info:
            print(f"  Current table structure:")
            for col in table_info:
                print(f"    - {col[0]}: {col[1]}")

            print(f"\n  ⚠️  Need to modify table to support chunks:")
            print(f"    - Add chunk_id column")
            print(f"    - Update primary key to chunk_id")
            print(f"    - Consider renaming to 'chunk_embeddings'")
        else:
            print(f"  ✅ Table doesn't exist yet - can create with chunk support")
    except:
        print(f"  ✅ Table doesn't exist yet - can create with chunk support")

    # Proposed embedding approach
    print(f"\n💡 Proposed Embedding Approach:")
    print(f"  1. Create new table 'chunk_embeddings' with:")
    print(f"     - chunk_id (PRIMARY KEY)")
    print(f"     - embedding FLOAT[768]")
    print(f"     - created_at TIMESTAMP")
    print(f"  2. Query quality chunks from conservative strategy")
    print(f"  3. Generate embeddings in batches")
    print(f"  4. Store with chunk_id reference")
    print(f"  5. Create view joining chunks with embeddings")

    # SQL for new table
    print(f"\n📋 SQL for New Embedding Table:")
    print("""
    CREATE TABLE IF NOT EXISTS chunk_embeddings (
        chunk_id INTEGER PRIMARY KEY,
        embedding FLOAT[768] NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chunk_id) REFERENCES comment_chunks_raw(chunk_id)
    );
    
    -- View for easy querying
    CREATE VIEW chunk_embeddings_with_metadata AS
    SELECT 
        ce.chunk_id,
        ce.embedding,
        ce.created_at,
        c.play_id,
        c.chunk_index,
        c.chunk_text,
        c.normalized_chunk_text,
        c.chunk_length,
        c.alpha_ratio,
        c.alphanum_ratio,
        fp.original_artist_text,
        fp.original_song_text,
        fp.airdate_iso
    FROM chunk_embeddings ce
    JOIN comment_chunks_raw c ON ce.chunk_id = c.chunk_id
    JOIN fact_plays fp ON c.play_id = fp.play_id
    WHERE c.strategy_id = 3;  -- Conservative strategy only
    """)

    conn.close()


if __name__ == "__main__":
    analyze_conservative_chunks()
