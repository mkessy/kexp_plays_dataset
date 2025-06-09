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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Connect to the database
conn = duckdb.connect('kexp_data.db')

def populate_artist_member_of_artist():
    """Populate the rel_Artist_Member_Of_Artist table from the staging table."""
    logger.info("Populating rel_Artist_Member_Of_Artist...")
    
    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_member_of_band'").fetchdf().shape[0]:
        logger.error("Staging table stage_member_of_band does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False
    
    # Count records in staging table
    count = conn.execute("SELECT COUNT(*) FROM stage_member_of_band").fetchone()[0]
    logger.info(f"Found {count} records in staging table")
    
    # Count records with both group and member KB IDs
    valid_count = conn.execute("SELECT COUNT(*) FROM stage_member_of_band WHERE group_kb_id IS NOT NULL AND member_kb_id IS NOT NULL").fetchone()[0]
    logger.info(f"Found {valid_count} records with valid KB IDs for both group and member")
    
    # Insert into KB relationship table
    conn.execute("""
    INSERT INTO rel_Artist_Member_Of_Artist (kb_group_artist_id, kb_member_artist_id, start_date, end_date)
    SELECT 
        group_kb_id, 
        member_kb_id, 
        CASE WHEN start_date = '' THEN NULL ELSE start_date END, 
        CASE WHEN end_date = '' THEN NULL ELSE end_date END
    FROM stage_member_of_band
    WHERE group_kb_id IS NOT NULL 
      AND member_kb_id IS NOT NULL
      AND group_kb_id != member_kb_id
    ON CONFLICT DO NOTHING;
    """)
    
    # Count inserted records
    inserted = conn.execute("SELECT COUNT(*) FROM rel_Artist_Member_Of_Artist").fetchone()[0]
    logger.info(f"Inserted {inserted} records into rel_Artist_Member_Of_Artist")
    
    return True

def populate_artist_plays_instrument():
    """Populate the rel_Artist_Plays_Instrument table from the staging table."""
    logger.info("Populating rel_Artist_Plays_Instrument...")
    
    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_artist_instrument'").fetchdf().shape[0]:
        logger.error("Staging table stage_artist_instrument does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False
    
    # Count records in staging table
    count = conn.execute("SELECT COUNT(*) FROM stage_artist_instrument").fetchone()[0]
    logger.info(f"Found {count} records in staging table")
    
    # Count records with all required KB IDs
    valid_count = conn.execute("""
    SELECT COUNT(*) FROM stage_artist_instrument 
    WHERE kb_artist_id IS NOT NULL 
      AND kb_song_id IS NOT NULL 
      AND kb_instrument_id IS NOT NULL
    """).fetchone()[0]
    logger.info(f"Found {valid_count} records with valid KB IDs for artist, song, and instrument")
    
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
    
    # Count inserted records
    inserted = conn.execute("SELECT COUNT(*) FROM rel_Artist_Plays_Instrument").fetchone()[0]
    logger.info(f"Inserted {inserted} records into rel_Artist_Plays_Instrument")
    
    return True

def populate_artist_performed_song():
    """Populate the rel_Artist_Performed_Song table from the staging table and KEXP plays."""
    logger.info("Populating rel_Artist_Performed_Song...")
    
    # 1. From MusicBrainz data
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_artist_performs_song'").fetchdf().shape[0]:
        logger.error("Staging table stage_artist_performs_song does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False
    
    mb_count = conn.execute("SELECT COUNT(*) FROM stage_artist_performs_song").fetchone()[0]
    mb_valid_count = conn.execute("SELECT COUNT(*) FROM stage_artist_performs_song WHERE kb_artist_id IS NOT NULL AND kb_song_id IS NOT NULL").fetchone()[0]
    
    logger.info(f"Found {mb_count} performer records in MusicBrainz data, {mb_valid_count} with valid KB IDs")
    
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
    
    kexp_count = conn.execute("SELECT COUNT(*) FROM stage_kexp_performances").fetchone()[0]
    logger.info(f"Found {kexp_count} performer relationships in KEXP play data")
    
    # Insert from KEXP data
    conn.execute("""
    INSERT INTO rel_Artist_Performed_Song (kb_artist_id, kb_song_id)
    SELECT kb_artist_id, kb_song_id
    FROM stage_kexp_performances
    ON CONFLICT DO NOTHING;
    """)
    
    # Count total records after both inserts
    total = conn.execute("SELECT COUNT(*) FROM rel_Artist_Performed_Song").fetchone()[0]
    logger.info(f"Total of {total} records now in rel_Artist_Performed_Song")
    
    return True

def populate_production_credits():
    """Populate the rel_Artist_Person_Role_Played_Role table from the staging table."""
    logger.info("Populating production credits (Artist_Person_Role)...")
    
    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_production_credits'").fetchdf().shape[0]:
        logger.error("Staging table stage_production_credits does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False
    
    # Count records in staging table
    count = conn.execute("SELECT COUNT(*) FROM stage_production_credits").fetchone()[0]
    logger.info(f"Found {count} records in staging table")
    
    # Count records with all required KB IDs
    valid_count = conn.execute("""
    SELECT COUNT(*) FROM stage_production_credits 
    WHERE kb_person_id IS NOT NULL 
      AND kb_target_id IS NOT NULL 
      AND kb_role_id IS NOT NULL
    """).fetchone()[0]
    logger.info(f"Found {valid_count} records with valid KB IDs for person, target, and role")
    
    # First, create the contribution instances in kb_Artist_Person_Role
    conn.execute("""
    CREATE OR REPLACE TEMP TABLE temp_contribution_instances AS
    WITH source_data AS (
        SELECT 
            kb_person_id, 
            kb_role_id,
            target_type,
            kb_target_id
        FROM stage_production_credits
        WHERE kb_person_id IS NOT NULL 
          AND kb_target_id IS NOT NULL 
          AND kb_role_id IS NOT NULL
    )
    SELECT 
        src.*,
        COALESCE(existing.kb_id, uuid_generate_v4()) AS contribution_kb_id,
        CASE WHEN existing.kb_id IS NULL THEN 'new' ELSE 'existing' END AS status
    FROM source_data src
    LEFT JOIN kb_Artist_Person_Role existing ON 
        src.kb_person_id = existing.kb_person_id AND 
        src.kb_role_id = existing.kb_role_id;
    """)
    
    # Insert new contribution instances
    conn.execute("""
    INSERT INTO kb_Artist_Person_Role (kb_id, kb_person_id, kb_role_id)
    SELECT 
        contribution_kb_id,
        kb_person_id,
        kb_role_id
    FROM temp_contribution_instances
    WHERE status = 'new'
    ON CONFLICT DO NOTHING;
    """)
    
    # Now link these contribution instances to their targets
    conn.execute("""
    INSERT INTO rel_Artist_Person_Role_Played_Role 
        (kb_artist_person_role_id, kb_target_entity_kb_id, target_entity_type)
    SELECT 
        contribution_kb_id,
        kb_target_id,
        CASE 
            WHEN target_type = 'recording' THEN 'SONG' 
            WHEN target_type = 'release' THEN 'RELEASE'
            ELSE 'UNKNOWN'
        END
    FROM temp_contribution_instances
    ON CONFLICT DO NOTHING;
    """)
    
    # Count inserted records
    contributions = conn.execute("SELECT COUNT(*) FROM kb_Artist_Person_Role").fetchone()[0]
    links = conn.execute("SELECT COUNT(*) FROM rel_Artist_Person_Role_Played_Role").fetchone()[0]
    
    logger.info(f"Total of {contributions} contribution instances in kb_Artist_Person_Role")
    logger.info(f"Total of {links} links in rel_Artist_Person_Role_Played_Role")
    
    return True

def populate_entity_has_url():
    """Populate the rel_Entity_Has_URL table and kb_URL from the staging table."""
    logger.info("Populating kb_URL and rel_Entity_Has_URL...")
    
    # Check if staging table exists
    if not conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'stage_external_links'").fetchdf().shape[0]:
        logger.error("Staging table stage_external_links does not exist. Run entities_phase_3_relationship_analysis.py first.")
        return False
    
    # Count records in staging table
    count = conn.execute("SELECT COUNT(*) FROM stage_external_links").fetchone()[0]
    logger.info(f"Found {count} external links in staging table")
    
    # Count records with required fields
    valid_count = conn.execute("SELECT COUNT(*) FROM stage_external_links WHERE kb_entity_id IS NOT NULL AND url IS NOT NULL").fetchone()[0]
    logger.info(f"Found {valid_count} links with valid entity IDs and URLs")
    
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
    
    # First, create URLs in kb_URL
    conn.execute("""
    CREATE OR REPLACE TEMP TABLE url_mapping AS
    WITH source_data AS (
        SELECT DISTINCT
            url,
            m.kb_link_type
        FROM stage_external_links e
        JOIN link_type_mapping m ON e.link_type = m.link_type
        WHERE e.url IS NOT NULL
    )
    SELECT 
        src.*,
        COALESCE(existing.kb_id, uuid_generate_v4()) AS kb_url_id,
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
    
    # Count inserted records
    urls = conn.execute("SELECT COUNT(*) FROM kb_URL").fetchone()[0]
    links = conn.execute("SELECT COUNT(*) FROM rel_Entity_Has_URL").fetchone()[0]
    
    logger.info(f"Total of {urls} URLs in kb_URL")
    logger.info(f"Total of {links} links in rel_Entity_Has_URL")
    
    return True

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Populate KB relationship tables.')
    parser.add_argument('--all', action='store_true', help='Populate all relationship tables')
    parser.add_argument('--band-members', action='store_true', help='Populate band membership relationships')
    parser.add_argument('--instruments', action='store_true', help='Populate instrument relationships')
    parser.add_argument('--performances', action='store_true', help='Populate performance relationships')
    parser.add_argument('--credits', action='store_true', help='Populate production and creative credits')
    parser.add_argument('--urls', action='store_true', help='Populate URL relationships')
    
    args = parser.parse_args()
    
    # If no specific arguments are provided, show help
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    # Make sure we have the UUID extension
    conn.execute("CREATE OR REPLACE FUNCTION uuid_generate_v4() AS 'uuid_uuid_v4';")
    
    # Populate tables based on arguments
    if args.all or args.band_members:
        populate_artist_member_of_artist()
    
    if args.all or args.instruments:
        populate_artist_plays_instrument()
    
    if args.all or args.performances:
        populate_artist_performed_song()
    
    if args.all or args.credits:
        populate_production_credits()
    
    if args.all or args.urls:
        populate_entity_has_url()
    
    logger.info("Knowledge base relationship population complete!")

if __name__ == "__main__":
    main()