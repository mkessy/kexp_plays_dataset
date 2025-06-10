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

    # Step 1: Create a bridge table to map MusicBrainz recording IDs to kb_Song IDs
    # This is critical for handling cases where we have the artist but not the song
    logger.info("Creating recording_to_song bridge table...")

    # Check if bridge tables exist
    bridge_kb_song_to_mb_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'bridge_kb_song_to_mb'").fetchdf().shape[0] > 0
    mb_recording_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'mb_recording'").fetchdf().shape[0] > 0
    bridge_kb_song_to_kexp_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'bridge_kb_song_to_kexp'").fetchdf().shape[0] > 0

    # Create the SQL query based on what tables exist
    bridge_query = """
    CREATE OR REPLACE TEMP TABLE bridge_mb_recording_to_kb_song AS
    WITH all_possible_mappings AS (
        -- Direct mappings from our staging table
        SELECT DISTINCT
            recording_mb_id,
            kb_song_id
        FROM stage_artist_instrument
        WHERE recording_mb_id IS NOT NULL
          AND kb_song_id IS NOT NULL
    """

    # Add mappings from existing bridge table if it exists
    if bridge_kb_song_to_mb_exists:
        bridge_query += """
        UNION ALL

        -- Mappings from our existing bridge table
        SELECT
            mb_recording_id AS recording_mb_id,
            kb_song_id
        FROM bridge_kb_song_to_mb
        WHERE mb_recording_id IS NOT NULL
          AND kb_song_id IS NOT NULL
        """

    # Add mappings through KEXP data if the tables exist
    if mb_recording_exists and bridge_kb_song_to_kexp_exists:
        bridge_query += """
        UNION ALL

        -- Try to find mappings through the KEXP data
        SELECT
            r.mb_recording_id AS recording_mb_id,
            bs.kb_song_id
        FROM mb_recording r
        JOIN bridge_kb_song_to_kexp bs ON r.kexp_track_id_internal = bs.kexp_track_id_internal
        WHERE r.mb_recording_id IS NOT NULL
          AND bs.kb_song_id IS NOT NULL
        """

    # Close the query
    bridge_query += """
    )
    SELECT
        recording_mb_id,
        kb_song_id
    FROM all_possible_mappings
    GROUP BY recording_mb_id, kb_song_id
    HAVING recording_mb_id IS NOT NULL AND kb_song_id IS NOT NULL;
    """

    # Execute the query
    conn.execute(bridge_query)

    # Count how many recordings we were able to map
    bridge_count = conn.execute(
        "SELECT COUNT(DISTINCT recording_mb_id) FROM bridge_mb_recording_to_kb_song").fetchone()[0]
    logger.info(
        f"Created bridge table with {bridge_count} unique MB recording IDs mapped to KB songs")

    # Step 2: Update the stage_artist_instrument table with the newly mapped song IDs
    conn.execute("""
    UPDATE stage_artist_instrument s
    SET kb_song_id = b.kb_song_id
    FROM bridge_mb_recording_to_kb_song b
    WHERE s.recording_mb_id = b.recording_mb_id
      AND s.kb_song_id IS NULL;
    """)

    # Count records with artist and song IDs after bridge mapping
    valid_artist_song_count = conn.execute("""
    SELECT COUNT(*) FROM stage_artist_instrument
    WHERE kb_artist_id IS NOT NULL
      AND kb_song_id IS NOT NULL
    """).fetchone()[0]
    logger.info(
        f"After bridge mapping: found {valid_artist_song_count} records with valid KB IDs for artist and song")

    # Step 3: Create a mapping for instrument names to our KB instrument categories using actual IDs from kb_Instrument
    conn.execute("""
    -- First, fetch the actual IDs from the kb_Instrument table
    CREATE OR REPLACE TEMP TABLE kb_instrument_ids AS
    SELECT
        kb_id,
        name
    FROM kb_Instrument;
    """)

    # Now use these IDs in our mapping
    conn.execute("""
    CREATE OR REPLACE TEMP TABLE instrument_mapping AS
    WITH instrument_categories AS (
        SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Vocals') AS kb_id, -- Vocals
            'vocals|vocal|singer|singing|vox|voice' AS pattern
        UNION ALL SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Keys') AS kb_id, -- Keys
            'piano|keyboard|keys|organ|synthesizer|synth|electric piano|wurlitzer|rhodes|harpsichord|clavinet|accordion' AS pattern
        UNION ALL SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Strings') AS kb_id, -- Strings
            'guitar|bass|cello|double bass|viola|acoustic guitar|electric guitar|bass guitar|banjo|mandolin|ukulele' AS pattern
        UNION ALL SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Percussion') AS kb_id, -- Percussion
            'drum|drums|percussion|bongo|conga|tambourine|cajon|cymbal|timpani|marimba|vibraphone|xylophone' AS pattern
        UNION ALL SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Woodwind') AS kb_id, -- Woodwind
            'saxophone|sax|clarinet|flute|oboe|bassoon|recorder' AS pattern
        UNION ALL SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Brass') AS kb_id, -- Brass
            'trumpet|trombone|horn|tuba|french horn|cornet|bugle' AS pattern
        UNION ALL SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Orchestra Strings') AS kb_id, -- Orchestra Strings
            'violin|viola|cello|double bass|string section|string orchestra|fiddle' AS pattern
        UNION ALL SELECT
            (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Other') AS kb_id, -- Other
            'theremin|beatbox|turntable|dj|effects|sampling|programming|sequencer|electronic|machine|producer' AS pattern
    )
    SELECT DISTINCT
        s.instrument_name,
        CASE
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'vocal|vox|voice|singing|singer') THEN (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Vocals')
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'piano|keyboard|keys|organ|synthesizer|synth|electric piano|wurlitzer|rhodes|harpsichord|clavinet|accordion') THEN (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Keys')
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'guitar|bass|banjo|mandolin|ukulele') THEN (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Strings')
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'drum|percussion|bongo|conga|tambourine|cajon|cymbal|timpani|marimba|vibraphone|xylophone') THEN (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Percussion')
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'saxophone|sax|clarinet|flute|oboe|bassoon|recorder') THEN (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Woodwind')
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'trumpet|trombone|horn|tuba|cornet|bugle') THEN (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Brass')
            WHEN REGEXP_MATCHES(LOWER(s.instrument_name), 'violin|viola|cello|double bass|fiddle') THEN (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Orchestra Strings')
            ELSE (SELECT kb_id FROM kb_instrument_ids WHERE name = 'Other')
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
    logger.info(
        f"After mapping: found {missing_count} records with missing KB IDs")

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION;")

        # Insert into KB relationship table
        conn.execute(r"""
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

    # Step 1: Create bridge tables to map MusicBrainz entity IDs to KB entity IDs
    logger.info("Creating target entity bridge tables...")

    # Check if bridge tables exist
    bridge_kb_song_to_mb_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'bridge_kb_song_to_mb'").fetchdf().shape[0] > 0
    mb_recording_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'mb_recording'").fetchdf().shape[0] > 0
    bridge_kb_song_to_kexp_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'bridge_kb_song_to_kexp'").fetchdf().shape[0] > 0

    # Create bridge table for recording IDs
    logger.info("Creating recording bridge table...")

    # Create the SQL query for recordings based on what tables exist
    recording_bridge_query = """
    CREATE OR REPLACE TEMP TABLE bridge_mb_recording_to_kb_song AS
    WITH all_possible_mappings AS (
        -- Direct mappings from our staging table
        SELECT DISTINCT
            target_entity_id AS recording_mb_id,
            kb_target_id AS kb_song_id
        FROM stage_production_credits
        WHERE target_entity_id IS NOT NULL
          AND kb_target_id IS NOT NULL
          AND target_type = 'recording'
    """

    # Add mappings from existing bridge table if it exists
    if bridge_kb_song_to_mb_exists:
        recording_bridge_query += """
        UNION ALL

        -- Mappings from our existing bridge table
        SELECT
            mb_recording_id AS recording_mb_id,
            kb_song_id
        FROM bridge_kb_song_to_mb
        WHERE mb_recording_id IS NOT NULL
          AND kb_song_id IS NOT NULL
        """

    # Add mappings through KEXP data if the tables exist
    if mb_recording_exists and bridge_kb_song_to_kexp_exists:
        recording_bridge_query += """
        UNION ALL

        -- Try to find mappings through the KEXP data
        SELECT
            r.mb_recording_id AS recording_mb_id,
            bs.kb_song_id
        FROM mb_recording r
        JOIN bridge_kb_song_to_kexp bs ON r.kexp_track_id_internal = bs.kexp_track_id_internal
        WHERE r.mb_recording_id IS NOT NULL
          AND bs.kb_song_id IS NOT NULL
        """

    # Close the query
    recording_bridge_query += """
    )
    SELECT
        recording_mb_id,
        kb_song_id
    FROM all_possible_mappings
    GROUP BY recording_mb_id, kb_song_id
    HAVING recording_mb_id IS NOT NULL AND kb_song_id IS NOT NULL;
    """

    # Execute the query
    conn.execute(recording_bridge_query)

    # Create bridge table for release IDs
    logger.info("Creating release bridge table...")

    # Check if kb_Release table exists
    kb_release_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'kb_Release'").fetchdf().shape[0] > 0

    release_bridge_query = """
    CREATE OR REPLACE TEMP TABLE bridge_mb_release_to_kb_release AS
    WITH all_possible_mappings AS (
        -- Direct mappings from our staging table
        SELECT DISTINCT
            target_entity_id AS release_mb_id,
            kb_target_id AS kb_release_id
        FROM stage_production_credits
        WHERE target_entity_id IS NOT NULL
          AND kb_target_id IS NOT NULL
          AND target_type = 'release'
    """

    # Add mappings from kb_Release if it exists
    if kb_release_exists:
        release_bridge_query += """
        UNION ALL

        -- Mappings from other sources if available
        SELECT
            r.mb_release_id AS release_mb_id,
            r.kb_id AS kb_release_id
        FROM kb_Release r
        WHERE r.mb_release_id IS NOT NULL
          AND r.kb_id IS NOT NULL
        """

    # Close the query
    release_bridge_query += """
    )
    SELECT
        release_mb_id,
        kb_release_id
    FROM all_possible_mappings
    GROUP BY release_mb_id, kb_release_id
    HAVING release_mb_id IS NOT NULL AND kb_release_id IS NOT NULL;
    """

    # Execute the query
    conn.execute(release_bridge_query)

    # Create bridge table for work IDs
    logger.info("Creating work bridge table...")

    # Check if kb_Work table exists
    kb_work_exists = conn.execute(
        "SELECT * FROM information_schema.tables WHERE table_name = 'kb_Work'").fetchdf().shape[0] > 0

    work_bridge_query = """
    CREATE OR REPLACE TEMP TABLE bridge_mb_work_to_kb_work AS
    WITH all_possible_mappings AS (
        -- Direct mappings from our staging table
        SELECT DISTINCT
            target_entity_id AS work_mb_id,
            kb_target_id AS kb_work_id
        FROM stage_production_credits
        WHERE target_entity_id IS NOT NULL
          AND kb_target_id IS NOT NULL
          AND target_type = 'work'
    """

    # Add mappings from kb_Work if it exists
    if kb_work_exists:
        work_bridge_query += """
        UNION ALL

        -- Mappings from other sources if available
        SELECT
            w.mb_work_id AS work_mb_id,
            w.kb_id AS kb_work_id
        FROM kb_Work w
        WHERE w.mb_work_id IS NOT NULL
          AND w.kb_id IS NOT NULL
        """

    # Close the query
    work_bridge_query += """
    )
    SELECT
        work_mb_id,
        kb_work_id
    FROM all_possible_mappings
    GROUP BY work_mb_id, kb_work_id
    HAVING work_mb_id IS NOT NULL AND kb_work_id IS NOT NULL;
    """

    # Execute the query
    conn.execute(work_bridge_query)

    # Count how many entities we were able to map
    recording_bridge_count = conn.execute(
        "SELECT COUNT(DISTINCT recording_mb_id) FROM bridge_mb_recording_to_kb_song").fetchone()[0]
    release_bridge_count = conn.execute(
        "SELECT COUNT(DISTINCT release_mb_id) FROM bridge_mb_release_to_kb_release").fetchone()[0]
    work_bridge_count = conn.execute(
        "SELECT COUNT(DISTINCT work_mb_id) FROM bridge_mb_work_to_kb_work").fetchone()[0]

    logger.info(
        f"Created bridge tables with: {recording_bridge_count} recordings, {release_bridge_count} releases, and {work_bridge_count} works mapped to KB entities")

    # Step 2: Update the staging table with the newly mapped target IDs
    conn.execute("""
    UPDATE stage_production_credits pc
    SET kb_target_id = s.kb_song_id
    FROM bridge_mb_recording_to_kb_song s
    WHERE pc.target_entity_id = s.recording_mb_id
      AND pc.target_type = 'recording'
      AND pc.kb_target_id IS NULL;
    """)

    conn.execute("""
    UPDATE stage_production_credits pc
    SET kb_target_id = r.kb_release_id
    FROM bridge_mb_release_to_kb_release r
    WHERE pc.target_entity_id = r.release_mb_id
      AND pc.target_type = 'release'
      AND pc.kb_target_id IS NULL;
    """)

    conn.execute("""
    UPDATE stage_production_credits pc
    SET kb_target_id = w.kb_work_id
    FROM bridge_mb_work_to_kb_work w
    WHERE pc.target_entity_id = w.work_mb_id
      AND pc.target_type = 'work'
      AND pc.kb_target_id IS NULL;
    """)

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
    logger.info(
        f"Found {missing_count} records with missing KB IDs for person or role")

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION;")

        # First, check if the roles exist in kb_Role and update if needed
        conn.execute("""
        CREATE OR REPLACE TEMP TABLE role_mapping AS
        SELECT DISTINCT
            pc.role_name,
            r.kb_id AS kb_role_id
        FROM stage_production_credits pc
        JOIN kb_Role r ON LOWER(pc.role_name) = LOWER(r.name)
        WHERE pc.kb_role_id IS NULL;
        """)

        # Update missing role IDs in the staging table
        conn.execute("""
        UPDATE stage_production_credits pc
        SET kb_role_id = rm.kb_role_id
        FROM role_mapping rm
        WHERE LOWER(pc.role_name) = LOWER(rm.role_name)
          AND pc.kb_role_id IS NULL;
        """)

        # Step 3: Insert all valid, unique person/role pairs into kb_Artist_Person_Role
        conn.execute("""
            INSERT INTO kb_Artist_Person_Role (kb_id, kb_person_id, kb_role_id)
            SELECT
                uuid(),
                stage.kb_person_id,
                stage.kb_role_id
            FROM (
                SELECT DISTINCT kb_person_id, kb_role_id
                FROM stage_production_credits
                WHERE kb_person_id IS NOT NULL AND kb_role_id IS NOT NULL
            ) AS stage
            -- This join validates that the IDs exist in the parent tables
            JOIN kb_Person p ON stage.kb_person_id = p.kb_id
            JOIN kb_Role r ON stage.kb_role_id = r.kb_id
            ON CONFLICT (kb_person_id, kb_role_id) DO NOTHING;
        """)
        logger.info(
            "Upserted all valid, unique person/role pairs into kb_Artist_Person_Role.")

        # Step 4: Now that all pairs exist, join everything to insert into the final relationship table.
        conn.execute(r"""
            INSERT INTO rel_Artist_Person_Role_Played_Role
                (kb_artist_person_role_id, kb_target_entity_kb_id, target_entity_type)
            SELECT DISTINCT
                roles.kb_id,
                stage.kb_target_id,
                CAST(
                    CASE
                        WHEN stage.target_type = 'recording' THEN 'SONG'
                        WHEN stage.target_type = 'release' THEN 'RELEASE'
                        WHEN stage.target_type = 'work' THEN 'WORK'
                    END
                AS entity_type)
            FROM stage_production_credits AS stage
            -- Join to get the canonical ID for the person-role pair
            JOIN kb_Artist_Person_Role AS roles
              ON stage.kb_person_id = roles.kb_person_id
             AND stage.kb_role_id = roles.kb_role_id
            -- Ensure the target entity ID is valid and exists
            WHERE stage.kb_target_id IS NOT NULL
              AND EXISTS (
                    SELECT 1 FROM kb_Song WHERE kb_id = stage.kb_target_id
                    UNION ALL
                    SELECT 1 FROM kb_Release WHERE kb_id = stage.kb_target_id
                    UNION ALL
                    SELECT 1 FROM kb_Work WHERE kb_id = stage.kb_target_id
              )
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
            logger.info(
                "Sampling records with missing KB IDs for person or role:")
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
            logger.info(
                f"Found {missing_target_count} records with missing target IDs but valid person and role IDs")
            logger.info("Sampling records with missing target IDs:")
            sample_missing_target = conn.execute("""
            SELECT artist_mb_id, artist_name, role_name, target_type, target_title,
                   kb_person_id, kb_role_id, target_entity_id
            FROM stage_production_credits
            WHERE kb_person_id IS NOT NULL AND kb_role_id IS NOT NULL AND kb_target_id IS NULL
            LIMIT 5
            """).fetchdf()
            logger.info(
                f"Sample of records with missing target IDs:\n{sample_missing_target}")

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

        # First, deduplicate URLs in the staging table by choosing a preferred link_type
        conn.execute("""
        CREATE OR REPLACE TEMP TABLE deduplicated_urls AS
        WITH ranked_urls AS (
            SELECT 
                url,
                link_type,
                ROW_NUMBER() OVER (PARTITION BY url ORDER BY 
                    CASE 
                        WHEN link_type LIKE '%official%' THEN 1
                        WHEN link_type = 'bandcamp' THEN 2
                        WHEN link_type = 'discogs' THEN 3
                        WHEN link_type IN ('youtube', 'video', 'vimeo') THEN 4
                        WHEN link_type IN ('twitter', 'facebook', 'instagram', 'social network') THEN 5
                        WHEN link_type = 'wikipedia' THEN 6
                        WHEN link_type LIKE '%event%' THEN 7
                        ELSE 8
                    END
                ) as row_num
            FROM stage_external_links
            WHERE url IS NOT NULL AND url LIKE 'http%'
        )
        SELECT url, link_type
        FROM ranked_urls
        WHERE row_num = 1;
        """)

        # Step 1: Insert all unique, valid URLs into kb_URL first.
        # Using deduplicated URLs to avoid the duplicate key issue
        conn.execute(r"""
            INSERT INTO kb_URL (kb_id, address, kb_link_type)
            SELECT
                uuid(),
                url,
                CAST(
                    CASE
                        WHEN link_type LIKE '%official%' THEN 'OFFICIAL_WEBSITE'
                        WHEN link_type = 'bandcamp' THEN 'BANDCAMP'
                        WHEN link_type = 'discogs' THEN 'DISCOGS'
                        WHEN link_type = 'soundcloud' THEN 'SOCIAL_MEDIA'
                        WHEN link_type IN ('youtube', 'video', 'vimeo') THEN 'PERFORMANCE_VIDEO'
                        WHEN link_type IN ('twitter', 'facebook', 'instagram', 'social network') THEN 'SOCIAL_MEDIA'
                        WHEN link_type = 'wikipedia' THEN 'WIKIDATA' -- Corrected from WIKIPEDIA
                        WHEN link_type LIKE '%event%' THEN 'EVENT_PAGE'
                        ELSE 'OTHER'
                    END
                AS link_type)
            FROM deduplicated_urls
            ON CONFLICT (address) DO NOTHING;
        """)
        logger.info("Upserted all unique URLs into kb_URL.")

        # Step 2: Now create the links by joining the staging table with the now-populated kb_URL table.
        # We use the original stage_external_links to ensure all entity-URL pairs are considered
        conn.execute("""
            INSERT INTO rel_Entity_Has_URL (kb_entity_id, kb_url_id, kb_entity_type)
            SELECT DISTINCT
                stage.kb_entity_id,
                urls.kb_id,
                CAST(stage.entity_type AS entity_type)
            FROM stage_external_links AS stage
            JOIN kb_URL AS urls ON stage.url = urls.address
            WHERE stage.kb_entity_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM kb_Artist WHERE kb_id = stage.kb_entity_id
                  UNION ALL
                  SELECT 1 FROM kb_Song WHERE kb_id = stage.kb_entity_id
                  UNION ALL
                  SELECT 1 FROM kb_Release WHERE kb_id = stage.kb_entity_id
              )
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
