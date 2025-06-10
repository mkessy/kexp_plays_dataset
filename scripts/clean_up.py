#!/usr/bin/env python3
"""
Database cleanup script - remove intermediate and unused tables
"""

import duckdb
import logging
import os

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Tables to drop - safe for removal
TABLES_TO_DROP = {
    'stage_tables': [
        'stage_album_extraction', 'stage_artist_extraction', 'stage_artist_instrument',
        'stage_artist_performs_song', 'stage_external_links', 'stage_genre_extraction',
        'stage_instrument_extraction', 'stage_labels', 'stage_labels_unique',
        'stage_location_extraction', 'stage_member_of_band', 'stage_person_extraction',
        'stage_production_credits', 'stage_release_extraction', 'stage_role_extraction',
        'stage_song_extraction'
    ],
    'extract_tables': [
        'extract_genres', 'extract_instruments', 'extract_instruments_fixed',
        'extract_locations', 'extract_performance_roles', 'extract_roles'
    ],
    'empty_rel_tables': [
        'rel_Album_Has_Genre', 'rel_Artist_Has_Genre'
    ],
    'unused_kb_tables': [
        'kb_Person', 'kb_Role', 'kb_Instrument', 'kb_Artist_Person_Role',
        'kb_Date_Entity', 'kb_Event', 'kb_Venue', 'kb_URL', 'kb_Work'
    ],
    'duplicate_tables': [
        'canonical_labels'
    ],
    'unused_rel_tables': [
        'rel_Artist_Member_Of_Artist', 'rel_Artist_Originates_From_Location',
        'rel_Artist_Performed_At_Event', 'rel_Artist_Performed_Song',
        'rel_Artist_Person_Role_Played_Role', 'rel_Artist_Plays_Instrument',
        'rel_Entity_Has_URL', 'rel_Song_Appears_On_Release', 'rel_Song_Based_On_Work',
        'rel_Song_Has_Genre'
    ]
}


def check_table_exists(conn, table_name):
    """Check if table exists"""
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except:
        return False


def get_table_count(conn, table_name):
    """Get row count for table"""
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except:
        return 0


def cleanup_database():
    """Remove intermediate and unused tables"""
    logger.info("Starting database cleanup...")

    conn = duckdb.connect(DB_PATH)

    total_dropped = 0
    total_rows_freed = 0

    for category, tables in TABLES_TO_DROP.items():
        logger.info(f"\nCleaning up {category}...")

        for table in tables:
            if check_table_exists(conn, table):
                row_count = get_table_count(conn, table)
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
                    logger.info(f"  ✅ Dropped {table} ({row_count:,} rows)")
                    total_dropped += 1
                    total_rows_freed += row_count
                except Exception as e:
                    logger.warning(f"  ❌ Failed to drop {table}: {e}")
            else:
                logger.info(f"  ⏭️  {table} (already removed)")

    # Vacuum to reclaim space
    logger.info("\nReclaiming database space...")
    conn.execute("VACUUM")

    # Final validation
    remaining_tables = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
    """).fetchone()[0]

    logger.info(f"\n🎉 Cleanup complete!")
    logger.info(f"   Dropped: {total_dropped} tables")
    logger.info(f"   Freed: {total_rows_freed:,} rows")
    logger.info(f"   Remaining: {remaining_tables} tables")

    conn.close()


if __name__ == "__main__":
    cleanup_database()
