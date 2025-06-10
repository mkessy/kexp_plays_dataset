#!/usr/bin/env python3
"""
Create Release-Label relationships to complete the KB relationship graph
"""

import duckdb
import hashlib
import logging
from datetime import datetime
import os

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_triple_id(subject_id: str, predicate: str, object_id: str) -> str:
    """Generate consistent triple ID"""
    content = f"{subject_id}|{predicate}|{object_id}"
    return hashlib.md5(content.encode()).hexdigest()


def create_release_label_relationships():
    """Create release-label relationships through play data connections"""
    logger.info("Creating release-label relationships...")

    conn = duckdb.connect(DB_PATH)

    # Step 1: Create the relationship data with small batches to avoid expensive operations
    logger.info("Step 1: Building release-label connections...")

    # First, get count to monitor progress
    count_query = """
    WITH play_to_release AS (
        SELECT 
            p.play_id,
            r.kb_id as release_kb_id
        FROM fact_plays p
        JOIN bridge_kb_song_to_kexp bks ON p.track_id_internal = bks.kexp_track_id_internal
        JOIN kb_Song s ON bks.kb_song_id = s.kb_id
        JOIN kb_Album a ON LOWER(TRIM(p.original_album_text)) = LOWER(TRIM(a.title))
        JOIN kb_Release r ON a.kb_id = r.album_id
        WHERE p.original_album_text IS NOT NULL 
          AND p.original_album_text != ''
          AND LENGTH(p.original_album_text) > 3
    ),
    play_to_label AS (
        SELECT 
            p.play_id,
            krl.kb_id as label_kb_id
        FROM fact_plays p
        JOIN bridge_play_to_label bpl ON p.play_id = bpl.play_id
        JOIN dim_labels_master dlm ON bpl.label_id_internal = dlm.label_id_internal
        JOIN kb_RecordLabel krl ON dlm.mb_id = krl.mb_label_id
    )
    SELECT COUNT(DISTINCT ptr.release_kb_id || '|' || ptl.label_kb_id) as unique_relationships
    FROM play_to_release ptr
    JOIN play_to_label ptl ON ptr.play_id = ptl.play_id
    """

    expected_relationships = conn.execute(count_query).fetchone()[0]
    logger.info(
        f"Expected to create {expected_relationships:,} unique release-label relationships")

    # Step 2: Create the relationships
    logger.info("Step 2: Creating relationship triples...")

    relationship_query = """
    WITH play_to_release AS (
        SELECT 
            p.play_id,
            r.kb_id as release_kb_id,
            r.title as release_title
        FROM fact_plays p
        JOIN bridge_kb_song_to_kexp bks ON p.track_id_internal = bks.kexp_track_id_internal
        JOIN kb_Song s ON bks.kb_song_id = s.kb_id
        JOIN kb_Album a ON LOWER(TRIM(p.original_album_text)) = LOWER(TRIM(a.title))
        JOIN kb_Release r ON a.kb_id = r.album_id
        WHERE p.original_album_text IS NOT NULL 
          AND p.original_album_text != ''
          AND LENGTH(p.original_album_text) > 3
    ),
    play_to_label AS (
        SELECT 
            p.play_id,
            krl.kb_id as label_kb_id,
            krl.name as label_name
        FROM fact_plays p
        JOIN bridge_play_to_label bpl ON p.play_id = bpl.play_id
        JOIN dim_labels_master dlm ON bpl.label_id_internal = dlm.label_id_internal
        JOIN kb_RecordLabel krl ON dlm.mb_id = krl.mb_label_id
    ),
    release_label_relationships AS (
        SELECT 
            ptr.release_kb_id,
            ptr.release_title,
            ptl.label_kb_id,
            ptl.label_name,
            COUNT(*) as play_count
        FROM play_to_release ptr
        JOIN play_to_label ptl ON ptr.play_id = ptl.play_id
        GROUP BY ptr.release_kb_id, ptr.release_title, ptl.label_kb_id, ptl.label_name
    )
    SELECT 
        ptr.release_kb_id as subject_id,
        'kb_Release' as subject_type,
        'released_by_label' as predicate,
        'kb_RecordLabel' as object_type,
        ptr.label_kb_id as object_id,
        ptr.release_title as source_name,
        ptr.label_name as target_name,
        'release_label' as mb_relation_type,
        'label' as mb_target_type
    FROM release_label_relationships ptr
    """

    relationships_df = conn.execute(relationship_query).fetchdf()
    actual_relationships = len(relationships_df)

    logger.info(f"Generated {actual_relationships:,} relationship records")

    # Step 3: Create triple IDs and insert into kb_Relationship
    logger.info("Step 3: Inserting relationships into kb_Relationship...")

    # Add triple IDs
    relationships_df['triple_id'] = relationships_df.apply(
        lambda row: generate_triple_id(
            row['subject_id'], row['predicate'], row['object_id']),
        axis=1
    )

    # Add timestamp
    relationships_df['created_at'] = datetime.now()

    # Insert into kb_Relationship table
    before_count = conn.execute(
        "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]

    try:
        conn.execute("""
            INSERT OR IGNORE INTO kb_Relationship 
            (triple_id, subject_type, subject_id, predicate, object_type, object_id, 
             source_name, target_name, mb_relation_type, mb_target_type, created_at)
            SELECT triple_id, subject_type, subject_id, predicate, object_type, object_id,
                   source_name, target_name, mb_relation_type, mb_target_type, created_at
            FROM relationships_df
        """)

        after_count = conn.execute(
            "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
        inserted_count = after_count - before_count

        logger.info(
            f"Successfully inserted {inserted_count:,} new relationships")
        logger.info(f"Total kb_Relationship count: {after_count:,}")

    except Exception as e:
        logger.error(f"Failed to insert relationships: {e}")
        raise

    # Step 4: Validation
    logger.info("Step 4: Validating release-label relationships...")

    validation_query = """
    SELECT 
        predicate,
        subject_type,
        object_type,
        COUNT(*) as count
    FROM kb_Relationship 
    WHERE object_type = 'kb_RecordLabel'
    GROUP BY predicate, subject_type, object_type
    ORDER BY count DESC
    """

    validation_results = conn.execute(validation_query).fetchdf()

    if len(validation_results) > 0:
        logger.info("✅ Release-label relationships created successfully:")
        for _, row in validation_results.iterrows():
            logger.info(
                f"   {row['predicate']}: {row['subject_type']} → {row['object_type']} ({row['count']:,})")
    else:
        logger.warning("❌ No release-label relationships found in validation")

    # Final KB state check
    logger.info("\nFinal KB Relationship State:")

    entity_participation = conn.execute("""
        SELECT DISTINCT object_type as entity_type, 'object' as role FROM kb_Relationship
        UNION 
        SELECT DISTINCT subject_type as entity_type, 'subject' as role FROM kb_Relationship
    """).fetchdf()

    participating_entities = set(entity_participation['entity_type'])
    core_entities = ['kb_Artist', 'kb_Song', 'kb_Album',
                     'kb_Release', 'kb_Genre', 'kb_Location', 'kb_RecordLabel']

    for entity in core_entities:
        status = "✅ In relationships" if entity in participating_entities else "❌ No relationships"
        logger.info(f"   {entity}: {status}")

    total_relationships = conn.execute(
        "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
    unique_predicates = conn.execute(
        "SELECT COUNT(DISTINCT predicate) FROM kb_Relationship").fetchone()[0]

    logger.info(f"\n🎉 KB Relationship completion summary:")
    logger.info(f"   Total relationships: {total_relationships:,}")
    logger.info(f"   Unique predicates: {unique_predicates}")
    logger.info(
        f"   All 7 core entity types: {'✅ Participating' if len(participating_entities.intersection(core_entities)) == 7 else '❌ Missing some'}")

    conn.close()


def main():
    """Main execution"""
    logger.info("Starting release-label relationship creation...")

    try:
        create_release_label_relationships()
        logger.info(
            "✅ Release-label relationship creation completed successfully")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
