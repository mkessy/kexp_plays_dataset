#!/usr/bin/env python3
"""
Complete database cleanup - handle foreign key dependencies
"""

import duckdb
import logging
import os

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def complete_cleanup():
    """Remove remaining unused tables with proper dependency handling"""
    logger.info("Starting complete cleanup with dependency handling...")

    conn = duckdb.connect(DB_PATH)

    # Step 1: Remove foreign key references by setting to NULL
    logger.info("Step 1: Removing foreign key references...")

    try:
        # Remove kb_person_id reference from kb_Artist
        conn.execute(
            "UPDATE kb_Artist SET kb_person_id = NULL WHERE kb_person_id IS NOT NULL")
        logger.info("  ✅ Cleared kb_person_id references from kb_Artist")
    except Exception as e:
        logger.warning(f"  ⚠️  Failed to clear kb_person_id: {e}")

    # Step 2: Drop remaining unused relationship tables first
    logger.info("\nStep 2: Dropping remaining relationship tables...")

    remaining_rel_tables = [
        'rel_Artist_Person_Role_Played_Role',
        'rel_Artist_Plays_Instrument',
        'rel_Entity_Has_URL',
        'rel_Song_Based_On_Work'
    ]

    for table in remaining_rel_tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"  ✅ Dropped {table} ({count:,} rows)")
        except Exception as e:
            logger.info(f"  ⏭️  {table} (already removed or doesn't exist)")

    # Step 3: Drop KB tables in dependency order
    logger.info("\nStep 3: Dropping unused KB tables...")

    # Drop in order: dependent tables first, then referenced tables
    kb_tables_to_drop = [
        'kb_Artist_Person_Role',  # References kb_Person and kb_Role
        'kb_Event',               # References kb_Venue
        'kb_Venue',               # Can be dropped after kb_Event
        'kb_URL',                 # No dependencies left
        'kb_Work',                # No dependencies left
        'kb_Instrument',          # No dependencies left after rel table dropped
        'kb_Role',                # No dependencies left after rel table dropped
        'kb_Person'               # No dependencies left after kb_Artist cleared
    ]

    for table in kb_tables_to_drop:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"  ✅ Dropped {table} ({count:,} rows)")
        except Exception as e:
            logger.warning(f"  ❌ Failed to drop {table}: {e}")

    # Step 4: Final validation
    logger.info("\nStep 4: Final validation...")

    remaining_tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """).fetchdf()

    # Check for any leftover unused tables
    unused_tables = remaining_tables[
        remaining_tables['table_name'].str.contains(
            'stage_|extract_|rel_.*(?<!kb_Relationship)')
    ]

    if len(unused_tables) > 0:
        logger.warning(
            f"  ⚠️  Found {len(unused_tables)} potentially unused tables still remaining")
        for table in unused_tables['table_name']:
            logger.warning(f"    - {table}")

    # Summary of core tables
    core_tables = remaining_tables[
        remaining_tables['table_name'].str.contains(
            'kb_|fact_plays|dim_|mb_|bridge_kb_')
    ]

    logger.info(f"\n🎉 Cleanup complete!")
    logger.info(f"   Core tables remaining: {len(core_tables)}")
    logger.info(f"   Total tables: {len(remaining_tables)}")

    # Show final KB state
    logger.info(f"\nFinal KB Core Entity Tables:")
    kb_core_entities = ['kb_Artist', 'kb_Song', 'kb_Album',
                        'kb_Release', 'kb_Genre', 'kb_Location', 'kb_RecordLabel']

    for entity in kb_core_entities:
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {entity}").fetchone()[0]
            logger.info(f"  ✅ {entity}: {count:,} entities")
        except:
            logger.warning(f"  ❌ {entity}: NOT FOUND")

    # Check relationships
    try:
        rel_count = conn.execute(
            "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
        logger.info(f"  ✅ kb_Relationship: {rel_count:,} relationships")
    except:
        logger.warning(f"  ❌ kb_Relationship: NOT FOUND")

    conn.close()


if __name__ == "__main__":
    complete_cleanup()
