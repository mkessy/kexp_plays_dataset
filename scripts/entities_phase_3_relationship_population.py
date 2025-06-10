#!/usr/bin/env python3
"""
Script to populate knowledge base relationship tables from MusicBrainz data.
This script uses the staging tables created by entities_phase_3_relationship_analysis.py 
to populate the KB relationship tables.
"""

import duckdb
import logging
import argparse
import uuid
from pathlib import Path
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Connect to the database with auto commit disabled for transaction support
conn = duckdb.connect('kexp_data.db', read_only=False)
conn.execute("PRAGMA enable_progress_bar;")
conn.execute("SET memory_limit='8GB';")


def populate_artist_member_of_artist():
    """Populate the rel_Artist_Member_Of_Artist table from the staging table."""
    logger.info("Populating rel_Artist_Member_Of_Artist...")

    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_member_of_band'").fetchdf().shape[0]:
        logger.error(
            "Staging table stage_member_of_band does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False

    # Count records in staging table
    count = conn.execute(
        "SELECT COUNT(*) FROM stage_member_of_band").fetchone()[0]
    logger.info(f"Found {count} records in staging table")

    # Count records with both group and member KB IDs
    valid_count = conn.execute(
        "SELECT COUNT(*) FROM stage_member_of_band WHERE group_kb_id IS NOT NULL AND member_kb_id IS NOT NULL").fetchone()[0]
    logger.info(
        f"Found {valid_count} records with valid KB IDs for both group and member")

    # Count records with missing KB IDs
    missing_count = conn.execute("""
    SELECT COUNT(*) FROM stage_member_of_band 
    WHERE group_kb_id IS NULL OR member_kb_id IS NULL
    """).fetchone()[0]
    logger.info(f"Found {missing_count} records with missing KB IDs")

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION;")

        # Insert into KB relationship table
        conn.execute("""
        INSERT INTO rel_Artist_Member_Of_Artist (kb_group_artist_id, kb_member_artist_id, start_date, end_date)
        SELECT 
            group_kb_id, 
            member_kb_id, 
            TRY_CAST(start_date AS DATE), 
            TRY_CAST(end_date AS DATE)
        FROM stage_member_of_band
        WHERE group_kb_id IS NOT NULL 
          AND member_kb_id IS NOT NULL
          AND group_kb_id != member_kb_id
        ON CONFLICT DO NOTHING;
        """)

        # Commit the transaction
        conn.execute("COMMIT;")

        # Count inserted records
        inserted = conn.execute(
            "SELECT COUNT(*) FROM rel_Artist_Member_Of_Artist").fetchone()[0]
        logger.info(
            f"Inserted {inserted} records into rel_Artist_Member_Of_Artist")

        # Sample some records with missing KB IDs to help diagnose mapping issues
        if missing_count > 0:
            logger.info("Sampling records with missing KB IDs:")
            sample_missing = conn.execute("""
            SELECT group_mb_id, group_name, member_mb_id, member_name, group_kb_id, member_kb_id
            FROM stage_member_of_band
            WHERE group_kb_id IS NULL OR member_kb_id IS NULL
            LIMIT 5
            """).fetchdf()
            logger.info(f"Sample of missing KB ID records:\n{sample_missing}")

        return True

    except Exception as e:
        conn.execute("ROLLBACK;")
        logger.error(f"Failed to populate rel_Artist_Member_Of_Artist: {e}")
        return False


def populate_artist_plays_instrument():
    """Populate the rel_Artist_Plays_Instrument table from the staging table."""
    logger.info("Populating rel_Artist_Plays_Instrument...")

    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_artist_instrument'").fetchdf().shape[0]:
        logger.error(
            "Staging table stage_artist_instrument does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False

    # Count records in staging table
    count = conn.execute(
        "SELECT COUNT(*) FROM stage_artist_instrument").fetchone()[0]
    logger.info(f"Found {count} records in staging table")

    # Count records with artist and song IDs (we'll handle instrument mapping separately)
    valid_artist_song_count = conn.execute("""
    SELECT COUNT(*) FROM stage_artist_instrument 
    WHERE kb_artist_id IS NOT NULL 
      AND kb_song_id IS NOT NULL
    """).fetchone()[0]
    logger.info(
        f"Found {valid_artist_song_count} records with valid KB IDs for artist and song")

    # Create a mapping for instrument names to our KB instrument categories
    conn.execute("""
    CREATE OR REPLACE TEMP TABLE instrument_mapping AS
    WITH instrument_categories AS (
        SELECT 
            '8990e8aa-a9b0-4084-90b7-cb01fa322eef' AS kb_id, -- Vocals
            'vocals|vocal|singer|singing|vox|voice' AS pattern
        UNION ALL SELECT 
            '9680efa6-10f3-4721-ad7f-a55b61e25a6b' AS kb_id, -- Keys
            'piano|keyboard|keys|organ|synthesizer|synth|electric piano|wurlitzer|rhodes|harpsichord|clavinet|accordion' AS pattern
        UNION ALL SELECT 
            '542566a6-3252-4bbb-97b0-3b6cfa93214a' AS kb_id, -- Strings
            'guitar|bass|cello|double bass|viola|acoustic guitar|electric guitar|bass guitar|banjo|mandolin|ukulele' AS pattern
        UNION ALL SELECT 
            'bb1a900a-5d58-4d1c-92a9-9071ae499761' AS kb_id, -- Percussion
            'drum|drums|percussion|bongo|conga|tambourine|cajon|cymbal|timpani|marimba|vibraphone|xylophone' AS pattern
        UNION ALL SELECT 
            '9fbf1c1e-1b46-4444-9d5e-c116be334219' AS kb_id, -- Woodwind
            'saxophone|sax|clarinet|flute|oboe|bassoon|recorder' AS pattern
        UNION ALL SELECT 
            'f67ffc37-9009-410f-81cd-bd462bcb2f9c' AS kb_id, -- Brass
            'trumpet|trombone|horn|tuba|french horn|cornet|bugle' AS pattern
        UNION ALL SELECT 
            '90a48b2a-21ae-475b-8b4a-536d238be03b' AS kb_id, -- Orchestra Strings
            'violin|viola|cello|double bass|string section|string orchestra|fiddle' AS pattern
        UNION ALL SELECT 
            '573f68e1-c1ab-4dc3-b6c3-65396eeee7aa' AS kb_id, -- Other
            'theremin|beatbox|turntable|dj|effects|sampling|programming|sequencer|electronic|machine|producer' AS pattern
    )
    SELECT DISTINCT
        s.instrument_name,
        CASE 
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'vocal|vox|voice|singing|singer') THEN '8990e8aa-a9b0-4084-90b7-cb01fa322eef'
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'piano|keyboard|keys|organ|synthesizer|synth|electric piano|wurlitzer|rhodes|harpsichord|clavinet|accordion') THEN '9680efa6-10f3-4721-ad7f-a55b61e25a6b'
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'guitar|bass|banjo|mandolin|ukulele') THEN '542566a6-3252-4bbb-97b0-3b6cfa93214a'
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'drum|percussion|bongo|conga|tambourine|cajon|cymbal|timpani|marimba|vibraphone|xylophone') THEN 'bb1a900a-5d58-4d1c-92a9-9071ae499761'
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'saxophone|sax|clarinet|flute|oboe|bassoon|recorder') THEN '9fbf1c1e-1b46-4444-9d5e-c116be334219'
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'trumpet|trombone|horn|tuba|cornet|bugle') THEN 'f67ffc37-9009-410f-81cd-bd462bcb2f9c'
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'violin|viola|cello|double bass|fiddle') THEN '90a48b2a-21ae-475b-8b4a-536d238be03b'
            ELSE '573f68e1-c1ab-4dc3-b6c3-65396eeee7aa' -- Other
        END AS kb_instrument_id
    FROM stage_artist_instrument s;
    """)

    # Apply the mapping to update the staging table
    conn.execute("""
    UPDATE stage_artist_instrument s
    SET kb_instrument_id = m.kb_instrument_id
    FROM instrument_mapping m
    WHERE s.instrument_name = m.instrument_name
      AND s.kb_instrument_id IS NULL;
    """)

    # Count records with all required KB IDs after mapping
    valid_count = conn.execute("""
    SELECT COUNT(*) FROM stage_artist_instrument 
    WHERE kb_artist_id IS NOT NULL 
      AND kb_song_id IS NOT NULL 
      AND kb_instrument_id IS NOT NULL
    """).fetchone()[0]
    logger.info(
        f"After mapping: found {valid_count} records with valid KB IDs for artist, song, and instrument")

    # Count records with missing KB IDs
    missing_count = conn.execute("""
    SELECT COUNT(*) FROM stage_artist_instrument 
    WHERE kb_artist_id IS NULL OR kb_song_id IS NULL OR kb_instrument_id IS NULL
    """).fetchone()[0]
    logger.info(f"After mapping: found {missing_count} records with missing KB IDs")

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION;")

        # Insert into KB relationship table
        conn.execute("""
        INSERT INTO rel_Artist_Plays_Instrument (kb_artist_id, kb_song_id, kb_instrument_id)
        SELECT kb_artist_id, kb_song_id, kb_instrument_id
        FROM stage_artist_instrument
        WHERE kb_artist_id IS NOT NULL 
          AND kb_song_id IS NOT NULL 
          AND kb_instrument_id IS NOT NULL
        ON CONFLICT DO NOTHING;
        """)

        # Commit the transaction
        conn.execute("COMMIT;")

        # Count inserted records
        inserted = conn.execute(
            "SELECT COUNT(*) FROM rel_Artist_Plays_Instrument").fetchone()[0]
        logger.info(
            f"Inserted {inserted} records into rel_Artist_Plays_Instrument")

        # Sample some records with missing KB IDs to help diagnose mapping issues
        if missing_count > 0:
            logger.info("Sampling records with missing KB IDs:")
            sample_missing = conn.execute("""
            SELECT artist_mb_id, artist_name, recording_mb_id, recording_title, instrument_name,
                   kb_artist_id, kb_song_id, kb_instrument_id
            FROM stage_artist_instrument
            WHERE kb_artist_id IS NULL OR kb_song_id IS NULL OR kb_instrument_id IS NULL
            LIMIT 5
            """).fetchdf()
            logger.info(f"Sample of missing KB ID records:\n{sample_missing}")

        return True

    except Exception as e:
        conn.execute("ROLLBACK;")
        logger.error(f"Failed to populate rel_Artist_Plays_Instrument: {e}")
        return False


def populate_artist_performed_song():
    """Populate the rel_Artist_Performed_Song table from the staging table and KEXP plays."""
    logger.info("Populating rel_Artist_Performed_Song...")

    # 1. From MusicBrainz data
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_artist_performs_song'").fetchdf().shape[0]:
        logger.error(
            "Staging table stage_artist_performs_song does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False

    mb_count = conn.execute(
        "SELECT COUNT(*) FROM stage_artist_performs_song").fetchone()[0]
    mb_valid_count = conn.execute(
        "SELECT COUNT(*) FROM stage_artist_performs_song WHERE kb_artist_id IS NOT NULL AND kb_song_id IS NOT NULL").fetchone()[0]
    mb_missing_count = mb_count - mb_valid_count

    logger.info(
        f"Found {mb_count} performer records in MusicBrainz data, {mb_valid_count} with valid KB IDs, {mb_missing_count} with missing IDs")

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION;")

        # Insert from MusicBrainz data
        conn.execute("""
        INSERT INTO rel_Artist_Performed_Song (kb_artist_id, kb_song_id)
        SELECT kb_artist_id, kb_song_id
        FROM stage_artist_performs_song
        WHERE kb_artist_id IS NOT NULL 
          AND kb_song_id IS NOT NULL
        ON CONFLICT DO NOTHING;
        """)

        # 2. From KEXP plays data
        logger.info("Adding performer relationships from KEXP play data...")

        # Create a temporary staging table from KEXP data
        conn.execute("""
        CREATE OR REPLACE TEMP TABLE stage_kexp_performances AS
        SELECT DISTINCT
            bridge_a.kb_artist_id,
            bridge_s.kb_song_id
        FROM fact_plays p
        JOIN bridge_play_to_artist pa ON p.play_id = pa.play_id
        JOIN bridge_kb_artist_to_kexp bridge_a ON pa.artist_id_internal = bridge_a.kexp_artist_id_internal
        JOIN bridge_kb_song_to_kexp bridge_s ON p.track_id_internal = bridge_s.kexp_track_id_internal
        WHERE bridge_a.kb_artist_id IS NOT NULL
          AND bridge_s.kb_song_id IS NOT NULL;
        """)

        kexp_count = conn.execute(
            "SELECT COUNT(*) FROM stage_kexp_performances").fetchone()[0]
        logger.info(
            f"Found {kexp_count} performer relationships in KEXP play data")

        # Insert from KEXP data
        conn.execute("""
        INSERT INTO rel_Artist_Performed_Song (kb_artist_id, kb_song_id)
        SELECT kb_artist_id, kb_song_id
        FROM stage_kexp_performances
        ON CONFLICT DO NOTHING;
        """)

        # Commit the transaction
        conn.execute("COMMIT;")

        # Count total records after both inserts
        total = conn.execute(
            "SELECT COUNT(*) FROM rel_Artist_Performed_Song").fetchone()[0]
        logger.info(
            f"Total of {total} records now in rel_Artist_Performed_Song")

        # Sample some records with missing KB IDs from MusicBrainz data
        if mb_missing_count > 0:
            logger.info("Sampling MusicBrainz records with missing KB IDs:")
            sample_missing = conn.execute("""
            SELECT artist_mb_id, artist_name, recording_mb_id, recording_title, kb_artist_id, kb_song_id
            FROM stage_artist_performs_song
            WHERE kb_artist_id IS NULL OR kb_song_id IS NULL
            LIMIT 5
            """).fetchdf()
            logger.info(
                f"Sample of missing KB ID records from MusicBrainz:\n{sample_missing}")

        return True

    except Exception as e:
        conn.execute("ROLLBACK;")
        logger.error(f"Failed to populate rel_Artist_Performed_Song: {e}")
        return False


def populate_production_credits():
    """Populate the rel_Artist_Person_Role_Played_Role table from the staging table."""
    logger.info("Populating production credits (Artist_Person_Role)...")

    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_production_credits'").fetchdf().shape[0]:
        logger.error(
            "Staging table stage_production_credits does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False

    # Count records in staging table
    count = conn.execute(
        "SELECT COUNT(*) FROM stage_production_credits").fetchone()[0]
    logger.info(f"Found {count} records in staging table")

    # Count records with person and role IDs (we don't require target ID as we'll handle those separately)
    valid_person_role_count = conn.execute("""
    SELECT COUNT(*) FROM stage_production_credits 
    WHERE kb_person_id IS NOT NULL 
      AND kb_role_id IS NOT NULL
    """).fetchone()[0]
    logger.info(
        f"Found {valid_person_role_count} records with valid KB IDs for person and role")

    # Count records with all required KB IDs (including target)
    valid_with_target_count = conn.execute("""
    SELECT COUNT(*) FROM stage_production_credits 
    WHERE kb_person_id IS NOT NULL 
      AND kb_target_id IS NOT NULL 
      AND kb_role_id IS NOT NULL
    """).fetchone()[0]
    logger.info(
        f"Found {valid_with_target_count} records with valid KB IDs for person, target, and role")

    # Count records with missing KB IDs
    missing_count = conn.execute("""
    SELECT COUNT(*) FROM stage_production_credits 
    WHERE kb_person_id IS NULL OR kb_role_id IS NULL
    """).fetchone()[0]
    logger.info(f"Found {missing_count} records with missing KB IDs for person or role")

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION;")

        # First, create the contribution instances in kb_Artist_Person_Role for all valid person+role combinations
        conn.execute("""
        CREATE OR REPLACE TEMP TABLE temp_contribution_instances AS
        WITH source_data AS (
            SELECT 
                kb_person_id, 
                kb_role_id,
                target_type,
                kb_target_id,
                artist_mb_id,
                artist_name,
                role_name,
                target_entity_id,
                target_title
            FROM stage_production_credits
            WHERE kb_person_id IS NOT NULL 
              AND kb_role_id IS NOT NULL
        )
        SELECT 
            src.*,
            COALESCE(existing.kb_id, uuid()) AS contribution_kb_id,
            CASE WHEN existing.kb_id IS NULL THEN 'new' ELSE 'existing' END AS status
        FROM source_data src
        LEFT JOIN kb_Artist_Person_Role existing ON 
            src.kb_person_id = existing.kb_person_id AND 
            src.kb_role_id = existing.kb_role_id;
        """)

        # Insert new contribution instances
        conn.execute("""
        INSERT INTO kb_Artist_Person_Role (kb_id, kb_person_id, kb_role_id)
        SELECT DISTINCT
            contribution_kb_id,
            kb_person_id,
            kb_role_id
        FROM temp_contribution_instances
        WHERE status = 'new'
        ON CONFLICT DO NOTHING;
        """)

        # Now link these contribution instances to their targets, but only when target ID is not null
        conn.execute("""
        INSERT INTO rel_Artist_Person_Role_Played_Role 
            (kb_artist_person_role_id, kb_target_entity_kb_id, target_entity_type)
        SELECT 
            contribution_kb_id,
            kb_target_id,
            CASE 
                WHEN target_type = 'recording' THEN 'SONG' 
                WHEN target_type = 'release' THEN 'RELEASE'
                WHEN target_type = 'work' THEN 'SONG'
                ELSE 'UNKNOWN'
            END
        FROM temp_contribution_instances
        WHERE kb_target_id IS NOT NULL
        ON CONFLICT DO NOTHING;
        """)

        # Commit the transaction
        conn.execute("COMMIT;")

        # Count inserted records
        contributions = conn.execute(
            "SELECT COUNT(*) FROM kb_Artist_Person_Role").fetchone()[0]
        links = conn.execute(
            "SELECT COUNT(*) FROM rel_Artist_Person_Role_Played_Role").fetchone()[0]

        logger.info(
            f"Total of {contributions} contribution instances in kb_Artist_Person_Role")
        logger.info(
            f"Total of {links} links in rel_Artist_Person_Role_Played_Role")

        # Sample some records with missing KB IDs to help diagnose mapping issues
        if missing_count > 0:
            logger.info("Sampling records with missing KB IDs for person or role:")
            sample_missing = conn.execute("""
            SELECT artist_mb_id, artist_name, role_name, target_type, target_title,
                   kb_person_id, kb_target_id, kb_role_id
            FROM stage_production_credits
            WHERE kb_person_id IS NULL OR kb_role_id IS NULL
            LIMIT 5
            """).fetchdf()
            logger.info(f"Sample of missing KB ID records:\n{sample_missing}")

        # Sample some records with missing target IDs
        missing_target_count = conn.execute("""
        SELECT COUNT(*) FROM stage_production_credits 
        WHERE kb_person_id IS NOT NULL AND kb_role_id IS NOT NULL AND kb_target_id IS NULL
        """).fetchone()[0]
        
        if missing_target_count > 0:
            logger.info(f"Found {missing_target_count} records with missing target IDs but valid person and role IDs")
            logger.info("Sampling records with missing target IDs:")
            sample_missing_target = conn.execute("""
            SELECT artist_mb_id, artist_name, role_name, target_type, target_title,
                   kb_person_id, kb_role_id, target_entity_id
            FROM stage_production_credits
            WHERE kb_person_id IS NOT NULL AND kb_role_id IS NOT NULL AND kb_target_id IS NULL
            LIMIT 5
            """).fetchdf()
            logger.info(f"Sample of records with missing target IDs:\n{sample_missing_target}")

        return True

    except Exception as e:
        conn.execute("ROLLBACK;")
        logger.error(f"Failed to populate production credits: {e}")
        return False


def populate_entity_has_url():
    """Populate the rel_Entity_Has_URL table and kb_URL from the staging table."""
    logger.info("Populating kb_URL and rel_Entity_Has_URL...")

    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_external_links'").fetchdf().shape[0]:
        logger.error(
            "Staging table stage_external_links does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False

    # Count records in staging table
    count = conn.execute(
        "SELECT COUNT(*) FROM stage_external_links").fetchone()[0]
    logger.info(f"Found {count} external links in staging table")

    # Count records with required fields
    valid_count = conn.execute(
        "SELECT COUNT(*) FROM stage_external_links WHERE kb_entity_id IS NOT NULL AND url IS NOT NULL").fetchone()[0]
    logger.info(f"Found {valid_count} links with valid entity IDs and URLs")

    # Count records with missing IDs or URLs
    missing_count = count - valid_count
    logger.info(f"Found {missing_count} links with missing entity IDs or URLs")

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION;")

        # Determine link types and create mapping
        conn.execute("""
        CREATE OR REPLACE TEMP TABLE link_type_mapping AS
        SELECT DISTINCT 
            link_type,
            CASE 
                WHEN link_type LIKE '%official%' THEN 'OFFICIAL_WEBSITE'
                WHEN link_type IN ('bandcamp', 'soundcloud') THEN 'BANDCAMP'
                WHEN link_type = 'discogs' THEN 'DISCOGS'
                WHEN link_type IN ('youtube', 'video', 'vimeo') THEN 'MUSIC_VIDEO'
                WHEN link_type IN ('twitter', 'facebook', 'instagram', 'social network') THEN 'SOCIAL_MEDIA'
                WHEN link_type = 'wikipedia' THEN 'WIKIPEDIA'
                WHEN link_type LIKE '%event%' THEN 'EVENT_PAGE'
                ELSE 'OTHER'
            END AS kb_link_type
        FROM stage_external_links;
        """)

        # First, create URLs in kb_URL with basic URL validation
        conn.execute("""
        CREATE OR REPLACE TEMP TABLE url_mapping AS
        WITH source_data AS (
            SELECT DISTINCT
                url,
                m.kb_link_type
            FROM stage_external_links e
            JOIN link_type_mapping m ON e.link_type = m.link_type
            WHERE e.url IS NOT NULL
            AND e.url LIKE 'http%' -- Basic URL validation
        )
        SELECT 
            src.*,
            COALESCE(existing.kb_id, uuid()) AS kb_url_id,
            CASE WHEN existing.kb_id IS NULL THEN 'new' ELSE 'existing' END AS status
        FROM source_data src
        LEFT JOIN kb_URL existing ON src.url = existing.address;
        """)

        # Insert new URLs
        conn.execute("""
        INSERT INTO kb_URL (kb_id, address, link_type)
        SELECT 
            kb_url_id,
            url,
            kb_link_type
        FROM url_mapping
        WHERE status = 'new'
        ON CONFLICT DO NOTHING;
        """)

        # Now link entities to URLs
        conn.execute("""
        INSERT INTO rel_Entity_Has_URL (kb_entity_id, kb_url_id, kb_entity_type)
        SELECT DISTINCT
            e.kb_entity_id,
            u.kb_url_id,
            e.entity_type
        FROM stage_external_links e
        JOIN url_mapping u ON e.url = u.url
        WHERE e.kb_entity_id IS NOT NULL
        ON CONFLICT DO NOTHING;
        """)

        # Commit the transaction
        conn.execute("COMMIT;")

        # Count inserted records
        urls = conn.execute("SELECT COUNT(*) FROM kb_URL").fetchone()[0]
        links = conn.execute(
            "SELECT COUNT(*) FROM rel_Entity_Has_URL").fetchone()[0]

        logger.info(f"Total of {urls} URLs in kb_URL")
        logger.info(f"Total of {links} links in rel_Entity_Has_URL")

        # Sample some records with missing KB IDs to help diagnose mapping issues
        if missing_count > 0:
            logger.info("Sampling records with missing KB entity IDs or URLs:")
            sample_missing = conn.execute("""
            SELECT artist_mb_id, artist_name, link_type, url, kb_entity_id, entity_type
            FROM stage_external_links
            WHERE kb_entity_id IS NULL OR url IS NULL
            LIMIT 5
            """).fetchdf()
            logger.info(
                f"Sample of missing entity IDs or URLs:\n{sample_missing}")

        return True

    except Exception as e:
        conn.execute("ROLLBACK;")
        logger.error(f"Failed to populate URL relationships: {e}")
        return False


def generate_relationship_stats():
    """Generate statistics about the relationship population to help identify potential issues."""
    logger.info("Generating relationship statistics...")

    stats = {}

    # Check for KB entities without any relationships
    stats['artists_without_relationships'] = conn.execute("""
    SELECT COUNT(*) FROM kb_Artist a
    WHERE NOT EXISTS (
        SELECT 1 FROM rel_Artist_Member_Of_Artist m WHERE m.kb_group_artist_id = a.kb_id OR m.kb_member_artist_id = a.kb_id
    ) AND NOT EXISTS (
        SELECT 1 FROM rel_Artist_Plays_Instrument i WHERE i.kb_artist_id = a.kb_id
    ) AND NOT EXISTS (
        SELECT 1 FROM rel_Artist_Performed_Song p WHERE p.kb_artist_id = a.kb_id
    ) AND NOT EXISTS (
        SELECT 1 FROM rel_Entity_Has_URL u WHERE u.kb_entity_id = a.kb_id AND u.kb_entity_type = 'ARTIST'
    )
    """).fetchone()[0]

    # Check for songs without any relationships
    stats['songs_without_relationships'] = conn.execute("""
    SELECT COUNT(*) FROM kb_Song s
    WHERE NOT EXISTS (
        SELECT 1 FROM rel_Artist_Plays_Instrument i WHERE i.kb_song_id = s.kb_id
    ) AND NOT EXISTS (
        SELECT 1 FROM rel_Artist_Performed_Song p WHERE p.kb_song_id = s.kb_id
    ) AND NOT EXISTS (
        SELECT 1 FROM rel_Entity_Has_URL u WHERE u.kb_entity_id = s.kb_id AND u.kb_entity_type = 'SONG'
    )
    """).fetchone()[0]

    # Check for relationship counts
    stats['member_of_band_count'] = conn.execute(
        "SELECT COUNT(*) FROM rel_Artist_Member_Of_Artist").fetchone()[0]
    stats['plays_instrument_count'] = conn.execute(
        "SELECT COUNT(*) FROM rel_Artist_Plays_Instrument").fetchone()[0]
    stats['performed_song_count'] = conn.execute(
        "SELECT COUNT(*) FROM rel_Artist_Performed_Song").fetchone()[0]
    stats['production_credits_count'] = conn.execute(
        "SELECT COUNT(*) FROM rel_Artist_Person_Role_Played_Role").fetchone()[0]
    stats['url_links_count'] = conn.execute(
        "SELECT COUNT(*) FROM rel_Entity_Has_URL").fetchone()[0]

    return stats


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Populate KB relationship tables.')
    parser.add_argument('--all', action='store_true',
                        help='Populate all relationship tables')
    parser.add_argument('--band-members', action='store_true',
                        help='Populate band membership relationships')
    parser.add_argument('--instruments', action='store_true',
                        help='Populate instrument relationships')
    parser.add_argument('--performances', action='store_true',
                        help='Populate performance relationships')
    parser.add_argument('--credits', action='store_true',
                        help='Populate production and creative credits')
    parser.add_argument('--urls', action='store_true',
                        help='Populate URL relationships')
    parser.add_argument('--stats', action='store_true',
                        help='Generate relationship statistics')

    args = parser.parse_args()

    # If no specific arguments are provided, show help
    if not any(vars(args).values()):
        parser.print_help()
        return

    start_time = time.time()

    success_count = 0
    total_tasks = 0

    # Populate tables based on arguments
    if args.all or args.band_members:
        total_tasks += 1
        if populate_artist_member_of_artist():
            success_count += 1

    if args.all or args.instruments:
        total_tasks += 1
        if populate_artist_plays_instrument():
            success_count += 1

    if args.all or args.performances:
        total_tasks += 1
        if populate_artist_performed_song():
            success_count += 1

    if args.all or args.credits:
        total_tasks += 1
        if populate_production_credits():
            success_count += 1

    if args.all or args.urls:
        total_tasks += 1
        if populate_entity_has_url():
            success_count += 1

    # Generate relationship statistics
    if args.all or args.stats:
        stats = generate_relationship_stats()
        logger.info("Relationship Statistics:")
        for key, value in stats.items():
            logger.info(f"  - {key}: {value}")

    end_time = time.time()
    duration = end_time - start_time

    logger.info(
        f"Knowledge base relationship population complete! {success_count}/{total_tasks} tasks succeeded")
    logger.info(f"Total execution time: {duration:.2f} seconds")


if __name__ == "__main__":
    main()
