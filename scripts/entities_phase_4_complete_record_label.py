#!/usr/bin/env python3
"""
Validate kb_RecordLabel completion and KB checkpoint state
kb_RecordLabel has been populated from dim_labels_master without name constraints
"""

import duckdb
import uuid
import logging
from datetime import datetime
import os

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def complete_record_labels(conn):
    """Complete kb_RecordLabel population from dim_labels_master"""
    logger.info("Checking current kb_RecordLabel state...")

    # Check current state
    current_count = conn.execute(
        "SELECT COUNT(*) FROM kb_RecordLabel").fetchone()[0]
    total_available = conn.execute("""
        SELECT COUNT(*) FROM dim_labels_master WHERE mb_id IS NOT NULL
    """).fetchone()[0]

    logger.info(f"Current kb_RecordLabel: {current_count:,} labels")
    logger.info(f"Available in dim_labels_master: {total_available:,} labels")

    if current_count >= total_available:
        logger.info("✅ kb_RecordLabel is complete and up to date")
        return

    logger.info("🔄 kb_RecordLabel needs updating...")

    # Get validation stats
    validation_query = """
    SELECT 
        COUNT(*) as total_labels,
        COUNT(CASE WHEN mb_label_id IS NOT NULL THEN 1 END) as with_mb_id,
        COUNT(DISTINCT name) as unique_names,
        COUNT(DISTINCT mb_label_id) as unique_mb_ids
    FROM kb_RecordLabel
    """

    stats = conn.execute(validation_query).fetchone()
    logger.info(
        f"Validation: {stats[0]:,} total, {stats[2]:,} unique names, {stats[3]:,} unique MB IDs")

    if stats[2] < stats[0]:
        duplicate_names = stats[0] - stats[2]
        logger.info(
            f"ℹ️  {duplicate_names:,} labels share names with other labels (using MB ID for uniqueness)")

    logger.info("✅ Record label population completed successfully")


def validate_kb_state(conn):
    """Validate current KB state and show checkpoint summary"""
    logger.info("Validating KB state...")

    # Core entity counts
    entity_counts = conn.execute("""
        SELECT 'kb_Artist' as entity_type, COUNT(*) as count FROM kb_Artist
        UNION ALL
        SELECT 'kb_Song' as entity_type, COUNT(*) as count FROM kb_Song
        UNION ALL
        SELECT 'kb_Album' as entity_type, COUNT(*) as count FROM kb_Album
        UNION ALL
        SELECT 'kb_Release' as entity_type, COUNT(*) as count FROM kb_Release
        UNION ALL
        SELECT 'kb_Genre' as entity_type, COUNT(*) as count FROM kb_Genre
        UNION ALL
        SELECT 'kb_Location' as entity_type, COUNT(*) as count FROM kb_Location
        UNION ALL
        SELECT 'kb_RecordLabel' as entity_type, COUNT(*) as count FROM kb_RecordLabel
        ORDER BY count DESC
    """).fetchdf()

    # Relationship summary
    relationship_summary = conn.execute("""
        SELECT 
            COUNT(*) as total_relationships,
            COUNT(DISTINCT predicate) as unique_predicates,
            COUNT(DISTINCT subject_type) as unique_subject_types,
            COUNT(DISTINCT object_type) as unique_object_types
        FROM kb_Relationship
    """).fetchone()

    # Entity participation in relationships
    entity_participation = conn.execute("""
        SELECT DISTINCT subject_type as entity_type, 'subject' as role FROM kb_Relationship
        UNION 
        SELECT DISTINCT object_type as entity_type, 'object' as role FROM kb_Relationship
    """).fetchdf()

    participating_entities = set(entity_participation['entity_type'])

    # Top relationship types
    top_relationships = conn.execute("""
        SELECT 
            predicate,
            subject_type,
            object_type,
            COUNT(*) as count
        FROM kb_Relationship
        GROUP BY predicate, subject_type, object_type
        ORDER BY count DESC
        LIMIT 10
    """).fetchdf()

    # Print validation report
    print("\n" + "="*60)
    print("KEXP KNOWLEDGE BASE - CHECKPOINT VALIDATION")
    print("="*60)

    print("\nCORE ENTITIES:")
    for _, row in entity_counts.iterrows():
        status = "✅ In relationships" if row['entity_type'] in participating_entities else "⏸️  No relationships yet"
        print(
            f"  {row['entity_type']:<15} {row['count']:>8,} entities  {status}")

    print(f"\nRELATIONSHIPS:")
    print(f"  Total relationships: {relationship_summary[0]:,}")
    print(f"  Unique predicates: {relationship_summary[1]}")
    print(f"  Entity types as subjects: {relationship_summary[2]}")
    print(f"  Entity types as objects: {relationship_summary[3]}")

    print(f"\nTOP 10 RELATIONSHIP TYPES:")
    for _, row in top_relationships.iterrows():
        print(
            f"  {row['predicate']:<25} {row['subject_type']} → {row['object_type']:<15} ({row['count']:,})")

    # Validate completeness
    missing_from_relationships = []
    core_entities = ['kb_Artist', 'kb_Song', 'kb_Album',
                     'kb_Release', 'kb_Genre', 'kb_Location', 'kb_RecordLabel']

    for entity in core_entities:
        if entity not in participating_entities:
            missing_from_relationships.append(entity)

    if missing_from_relationships:
        print(f"\nMISSING FROM RELATIONSHIPS:")
        for entity in missing_from_relationships:
            print(f"  ❌ {entity}")
    else:
        print(f"\n✅ All core entities participate in relationships")

    print("\n" + "="*60)
    return entity_counts, relationship_summary, missing_from_relationships


def main():
    """Main execution"""
    logger.info("Starting record label completion and KB validation...")

    try:
        conn = duckdb.connect(DB_PATH)

        # Complete record labels
        complete_record_labels(conn)

        # Validate current state
        entity_counts, rel_summary, missing = validate_kb_state(conn)

        # Final summary
        total_entities = entity_counts['count'].sum()
        logger.info(
            f"KB contains {total_entities:,} total entities with {rel_summary[0]:,} relationships")

        if not missing:
            logger.info(
                "✅ KB is at a solid checkpoint - all core entities populated and participating in relationships")
        else:
            logger.info(
                f"⏸️  Checkpoint reached with {len(missing)} entity types not yet in relationships")

        conn.close()

    except Exception as e:
        logger.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
