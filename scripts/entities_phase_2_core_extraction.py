#!/usr/bin/env python3
"""
KEXP Knowledge Base - Phase 2 Core Entities Extraction
Implementation for populating kb_Song, kb_Artist, and kb_Person entities.
"""

import duckdb
import os
from typing import Optional
import sys

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


class Phase2CoreEntityExtractor:
    """Handles extraction of core entities from KEXP and MusicBrainz data."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to database with error handling."""
        try:
            self.conn = duckdb.connect(self.db_path)
            print(f"✅ Connected to database: {self.db_path}")
            return self.conn
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            raise

    def validate_prerequisites(self) -> bool:
        """Validate that required tables exist before extraction."""
        print("\n🔍 Validating prerequisites...")

        required_tables = [
            'mb_artists_raw',
            'dim_artists_master',
            'dim_tracks',
            'dim_releases_master',
            'kb_Artist',
            'kb_Person',
            'kb_Song',
            'kb_Album',
            'kb_Release',
            'bridge_kb_artist_to_kexp',
            'bridge_kb_song_to_kexp'
        ]

        for table in required_tables:
            try:
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"    ✅ {table}: {count:,} records")
            except Exception as e:
                print(f"    ❌ Missing table {table}: {e}")
                return False

        # Check KEXP-MB coverage for songs
        mb_song_coverage = self.conn.execute("""
            SELECT COUNT(DISTINCT mb_recording_id)
            FROM dim_tracks
            WHERE mb_recording_id IS NOT NULL
        """).fetchone()[0]

        print(f"    ✅ KEXP tracks with MB recording IDs: {mb_song_coverage:,}")

        # Check KEXP-MB coverage for artists
        mb_artist_coverage = self.conn.execute("""
            SELECT COUNT(DISTINCT mb_id)
            FROM dim_artists_master
            WHERE mb_id IS NOT NULL
        """).fetchone()[0]

        print(f"    ✅ KEXP artists with MB IDs: {mb_artist_coverage:,}")

        # Prepare mb_relations_enhanced if it doesn't exist
        self._prepare_relations_table()

        return True

    def _prepare_relations_table(self):
        """Creates the `mb_relations_enhanced` table for use by extraction methods."""
        try:
            # Check if table exists
            self.conn.execute(
                "SELECT COUNT(*) FROM mb_relations_enhanced").fetchone()[0]
            print("    ✅ mb_relations_enhanced table already exists")
        except:
            print("\n🤝 Preparing enhanced relations table...")
            self.conn.execute("DROP TABLE IF EXISTS mb_relations_enhanced")
            self.conn.execute("""
                CREATE TABLE mb_relations_enhanced AS
                SELECT
                    CAST(mb.id AS UUID) as artist_mb_id,
                    mb.name as artist_name,
                    r.type as relation_type,
                    r."target-type" as target_type,
                    r."target-credit" as target_credit,
                    r."source-credit" as source_credit,
                    r.direction as direction,
                    r."target-id" as target_entity_id,
                    r.attributes as attributes_raw,
                    CASE
                        WHEN r.attributes IS NULL THEN []::VARCHAR[]
                        WHEN typeof(r.attributes) = 'VARCHAR[][]' THEN r.attributes[1]
                        WHEN typeof(r.attributes) = 'VARCHAR[]' THEN r.attributes
                        ELSE []::VARCHAR[]
                    END as attributes_array,
                    r.begin as start_date,
                    r.end as end_date
                FROM mb_artists_raw mb, UNNEST(mb.relations) AS t(r)
                WHERE CAST(mb.id AS UUID) IN (
                    SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL
                )
                AND r.type IS NOT NULL
                AND r.type != ''
            """)
            relations_count = self.conn.execute(
                "SELECT COUNT(*) FROM mb_relations_enhanced").fetchone()[0]
            print(
                f"    ✅ Created enhanced relations table with {relations_count:,} relations")

    def create_staging_tables(self):
        """Create staging tables for extraction validation."""
        print("\n🏗️  Creating staging extraction tables...")

        # Drop existing staging tables
        staging_tables = [
            'stage_song_extraction',
            'stage_artist_extraction',
            'stage_person_extraction',
            'stage_album_extraction',
            'stage_release_extraction'
        ]

        for table in staging_tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")

        # Create staging tables with comprehensive metadata
        self.conn.execute("""
            CREATE TABLE stage_song_extraction (
                mb_recording_id UUID,
                title VARCHAR NOT NULL,
                kexp_track_id_internal UUID,
                mb_work_id UUID,
                artist_count INTEGER,
                sample_artists TEXT,
                play_count INTEGER
            )
        """)

        self.conn.execute("""
            CREATE TABLE stage_artist_extraction (
                mb_artist_id UUID,
                name VARCHAR NOT NULL,
                artist_type VARCHAR,
                kexp_artist_id_internal UUID,
                country_code VARCHAR(2),
                mb_begin_area_id UUID,
                mb_end_area_id UUID,
                mb_area_id UUID,
                begin_date_year INTEGER,
                end_date_year INTEGER,
                play_count INTEGER,
                is_person BOOLEAN
            )
        """)

        self.conn.execute("""
            CREATE TABLE stage_person_extraction (
                mb_person_id UUID NOT NULL,
                legal_name VARCHAR NULL,
                common_name VARCHAR NOT NULL,
                gender VARCHAR NULL,
                nationality VARCHAR NULL,
                disambiguation VARCHAR NULL,
                linked_artist_kb_id UUID NULL
            )
        """)

        self.conn.execute("""
            CREATE TABLE stage_album_extraction (
                mb_release_group_id UUID,
                title VARCHAR NOT NULL,
                release_count INTEGER,
                sample_releases TEXT,
                play_count INTEGER
            )
        """)

        self.conn.execute("""
            CREATE TABLE stage_release_extraction (
                mb_release_id UUID,
                title VARCHAR NOT NULL,
                mb_release_group_id UUID,
                release_date DATE,
                country_code VARCHAR(2),
                format VARCHAR,
                barcode VARCHAR,
                play_count INTEGER
            )
        """)

        print("✅ Staging tables created")

    def extract_songs_to_staging(self):
        """Extract song data to staging table."""
        print("\n🎵 Extracting songs to staging...")

        # Extract songs from dim_tracks with play counts
        self.conn.execute("""
            WITH play_counts AS (
                SELECT
                    track_id_internal,
                    COUNT(*) as play_count
                FROM fact_plays
                GROUP BY track_id_internal
            )
            INSERT INTO stage_song_extraction
            SELECT
                t.mb_recording_id,
                t.primary_song_title_observed as title,
                t.track_id_internal as kexp_track_id_internal,
                NULL as mb_work_id, -- Not available in current data
                1 as artist_count, -- Simplified since we can't easily join to artists here
                'Various Artists' as sample_artists, -- Placeholder
                COALESCE(pc.play_count, 0) as play_count
            FROM dim_tracks t
            LEFT JOIN play_counts pc ON t.track_id_internal = pc.track_id_internal
            WHERE t.mb_recording_id IS NOT NULL
        """)

        # Add songs without MB IDs - these are still valid KEXP songs
        self.conn.execute("""
            WITH play_counts AS (
                SELECT
                    track_id_internal,
                    COUNT(*) as play_count
                FROM fact_plays
                GROUP BY track_id_internal
            )
            INSERT INTO stage_song_extraction
            SELECT
                NULL as mb_recording_id,
                t.primary_song_title_observed as title,
                t.track_id_internal as kexp_track_id_internal,
                NULL as mb_work_id,
                1 as artist_count, -- Simplified
                'Various Artists' as sample_artists, -- Placeholder
                COALESCE(pc.play_count, 0) as play_count
            FROM dim_tracks t
            LEFT JOIN play_counts pc ON t.track_id_internal = pc.track_id_internal
            WHERE t.mb_recording_id IS NULL
        """)

        song_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_song_extraction").fetchone()[0]
        print(f"✅ Extracted {song_count:,} songs to staging")

        # Show distribution of songs with/without MB IDs
        mb_stats = self.conn.execute("""
            SELECT 
                CASE WHEN mb_recording_id IS NULL THEN 'No MB ID' ELSE 'Has MB ID' END as has_mb,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percent
            FROM stage_song_extraction
            GROUP BY has_mb
            ORDER BY has_mb
        """).fetchall()

        print("    Song MB ID coverage:")
        for status, count, percent in mb_stats:
            print(f"        {status}: {count:,} songs ({percent}%)")

    def extract_artists_to_staging(self):
        """Extract artist data to staging table."""
        print("\n👨‍🎤 Extracting artists to staging...")

        # Extract artists from dim_artists_master with type from mb_artists_raw
        self.conn.execute("""
            INSERT INTO stage_artist_extraction
            WITH artist_play_stats AS (
                SELECT
                    a.artist_id_internal,
                    a.primary_name_observed,
                    a.mb_id,
                    COUNT(*) as play_count
                FROM dim_artists_master a
                JOIN fact_plays p ON a.artist_id_internal = p.artist_id_internal
                GROUP BY a.artist_id_internal, a.primary_name_observed, a.mb_id
            ),
            artist_mb_details AS (
                SELECT
                    CAST(mb.id AS UUID) as mb_id,
                    mb.type as mb_type,
                    mb."iso-3166-1-codes"[1] as country_code,
                    CAST(mb."begin-area".id AS UUID) as begin_area_id,
                    CAST(mb.area.id AS UUID) as area_id,
                    CAST(mb."end-area".id AS UUID) as end_area_id,
                    CAST(mb."life-span".begin AS INTEGER) as begin_year,
                    CAST(mb."life-span".end AS INTEGER) as end_year
                FROM mb_artists_raw mb
                WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
            )
            SELECT
                a.mb_id as mb_artist_id,
                a.primary_name_observed as name,
                CASE 
                    WHEN mb.mb_type = 'Person' THEN 'PERSON'
                    WHEN mb.mb_type = 'Group' THEN 'GROUP'
                    WHEN mb.mb_type = 'Orchestra' THEN 'ORCHESTRA'
                    WHEN mb.mb_type = 'Character' THEN 'CHARACTER'
                    ELSE 'OTHER' 
                END as artist_type,
                a.artist_id_internal as kexp_artist_id_internal,
                mb.country_code,
                mb.begin_area_id as mb_begin_area_id,
                mb.end_area_id as mb_end_area_id,
                mb.area_id as mb_area_id,
                mb.begin_year as begin_date_year,
                mb.end_year as end_date_year,
                aps.play_count,
                mb.mb_type = 'Person' as is_person
            FROM dim_artists_master a
            LEFT JOIN artist_mb_details mb ON a.mb_id = mb.mb_id
            LEFT JOIN artist_play_stats aps ON a.artist_id_internal = aps.artist_id_internal
            WHERE a.mb_id IS NOT NULL
        """)

        # Add artists without MB IDs - we still want these in our KB
        self.conn.execute("""
            INSERT INTO stage_artist_extraction
            WITH artist_play_stats AS (
                SELECT
                    a.artist_id_internal,
                    a.primary_name_observed,
                    COUNT(*) as play_count
                FROM dim_artists_master a
                JOIN fact_plays p ON a.artist_id_internal = p.artist_id_internal
                WHERE a.mb_id IS NULL
                GROUP BY a.artist_id_internal, a.primary_name_observed
            )
            SELECT
                NULL as mb_artist_id,
                a.primary_name_observed as name,
                'OTHER' as artist_type, -- Default type for unknown artists
                a.artist_id_internal as kexp_artist_id_internal,
                NULL as country_code,
                NULL as mb_begin_area_id,
                NULL as mb_end_area_id,
                NULL as mb_area_id,
                NULL as begin_date_year,
                NULL as end_date_year,
                aps.play_count,
                FALSE as is_person -- Conservative default
            FROM dim_artists_master a
            LEFT JOIN artist_play_stats aps ON a.artist_id_internal = aps.artist_id_internal
            WHERE a.mb_id IS NULL
        """)

        artist_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_artist_extraction").fetchone()[0]
        print(f"✅ Extracted {artist_count:,} artists to staging")

        # Show distribution of artist types
        type_stats = self.conn.execute("""
            SELECT 
                artist_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percent
            FROM stage_artist_extraction
            GROUP BY artist_type
            ORDER BY count DESC
        """).fetchall()

        print("    Artist type distribution:")
        for artist_type, count, percent in type_stats:
            print(f"        {artist_type}: {count:,} artists ({percent}%)")

    def extract_persons_to_staging(self):
        """Extract person data to staging table."""
        print("\n👤 Extracting persons to staging...")

        # Extract persons from the artist data for artists of type 'PERSON'
        self.conn.execute("""
            INSERT INTO stage_person_extraction
            SELECT
                mb_artist_id as mb_person_id,
                NULL as legal_name,  -- Not available in current data
                name as common_name,
                NULL as gender,      -- Not available in current data 
                country_code as nationality,
                NULL as disambiguation,
                NULL as linked_artist_kb_id  -- Will be populated after kb_Artist is populated
            FROM stage_artist_extraction
            WHERE is_person = TRUE
            AND mb_artist_id IS NOT NULL
        """)

        person_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_person_extraction").fetchone()[0]
        print(f"✅ Extracted {person_count:,} persons to staging")

    def extract_albums_to_staging(self):
        """Extract album data to staging table."""
        print("\n💿 Extracting albums to staging...")

        # Extract albums (release groups) from dim_releases_master
        self.conn.execute("""
            WITH album_release_samples AS (
                SELECT
                    r.mb_release_group_id,
                    r.title_on_release as title,
                    r.mb_release_id,
                    COUNT(*) as play_count,
                    ROW_NUMBER() OVER (PARTITION BY r.mb_release_group_id ORDER BY COUNT(*) DESC) as rn
                FROM dim_releases_master r
                JOIN fact_plays p ON r.release_id_internal = p.release_id_internal
                WHERE r.mb_release_group_id IS NOT NULL
                GROUP BY r.mb_release_group_id, r.title_on_release, r.mb_release_id
            ),
            album_stats AS (
                SELECT
                    mb_release_group_id,
                    title,
                    COUNT(DISTINCT mb_release_id) as release_count,
                    STRING_AGG(mb_release_id::VARCHAR, '; ') FILTER (WHERE rn <= 3) as sample_releases,
                    SUM(play_count) as play_count
                FROM album_release_samples
                GROUP BY mb_release_group_id, title
            )
            INSERT INTO stage_album_extraction
            SELECT
                mb_release_group_id,
                title,
                release_count,
                sample_releases,
                play_count
            FROM album_stats
        """)

        # Add albums without MB IDs
        self.conn.execute("""
            INSERT INTO stage_album_extraction
            WITH album_stats AS (
                SELECT
                    NULL as mb_release_group_id,
                    r.title_on_release as title,
                    COUNT(DISTINCT r.mb_release_id) as release_count,
                    STRING_AGG(r.mb_release_id::VARCHAR, '; ') as sample_releases,
                    COUNT(*) as play_count
                FROM dim_releases_master r
                JOIN fact_plays p ON r.release_id_internal = p.release_id_internal
                WHERE r.mb_release_group_id IS NULL
                GROUP BY r.title_on_release
            )
            SELECT * FROM album_stats
        """)

        album_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_album_extraction").fetchone()[0]
        print(f"✅ Extracted {album_count:,} albums to staging")

        # Show distribution of albums with/without MB IDs
        mb_stats = self.conn.execute("""
            SELECT 
                CASE WHEN mb_release_group_id IS NULL THEN 'No MB ID' ELSE 'Has MB ID' END as has_mb,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percent
            FROM stage_album_extraction
            GROUP BY has_mb
            ORDER BY has_mb
        """).fetchall()

        print("    Album MB ID coverage:")
        for status, count, percent in mb_stats:
            print(f"        {status}: {count:,} albums ({percent}%)")

    def extract_releases_to_staging(self):
        """Extract release data to staging table."""
        print("\n📀 Extracting releases to staging...")

        # Extract releases from dim_releases_master
        self.conn.execute("""
            WITH release_play_stats AS (
                SELECT
                    r.mb_release_id,
                    r.title_on_release as title,
                    r.mb_release_group_id,
                    CAST(r.release_date AS DATE) as release_date,
                    r.release_country as country_code,
                    r.release_format as format,
                    r.barcode,
                    COUNT(*) as play_count
                FROM dim_releases_master r
                JOIN fact_plays p ON r.release_id_internal = p.release_id_internal
                WHERE r.mb_release_id IS NOT NULL
                GROUP BY r.mb_release_id, r.title_on_release, r.mb_release_group_id, 
                         r.release_date, r.release_country, r.release_format, r.barcode
            )
            INSERT INTO stage_release_extraction
            SELECT
                mb_release_id,
                title,
                mb_release_group_id,
                release_date,
                country_code,
                format,
                barcode,
                play_count
            FROM release_play_stats
        """)

        # Add releases without MB IDs
        self.conn.execute("""
            INSERT INTO stage_release_extraction
            WITH release_stats AS (
                SELECT
                    NULL as mb_release_id,
                    r.title_on_release as title,
                    NULL as mb_release_group_id,
                    CAST(r.release_date AS DATE) as release_date,
                    r.release_country as country_code,
                    r.release_format as format,
                    r.barcode,
                    COUNT(*) as play_count
                FROM dim_releases_master r
                JOIN fact_plays p ON r.release_id_internal = p.release_id_internal
                WHERE r.mb_release_id IS NULL
                GROUP BY r.title_on_release, r.release_date, r.release_country, r.release_format, r.barcode
            )
            SELECT * FROM release_stats
        """)

        release_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_release_extraction").fetchone()[0]
        print(f"✅ Extracted {release_count:,} releases to staging")

        # Show distribution of releases with/without MB IDs
        mb_stats = self.conn.execute("""
            SELECT 
                CASE WHEN mb_release_id IS NULL THEN 'No MB ID' ELSE 'Has MB ID' END as has_mb,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percent
            FROM stage_release_extraction
            GROUP BY has_mb
            ORDER BY has_mb
        """).fetchall()

        print("    Release MB ID coverage:")
        for status, count, percent in mb_stats:
            print(f"        {status}: {count:,} releases ({percent}%)")

    def validate_staging_data(self):
        """Validate extracted staging data before KB population."""
        print("\n✅ Validating staging data...")

        # Check for data quality issues
        validation_checks = [
            ("Songs with empty titles",
             "SELECT COUNT(*) FROM stage_song_extraction WHERE title IS NULL OR title = ''"),
            ("Artists with empty names",
             "SELECT COUNT(*) FROM stage_artist_extraction WHERE name IS NULL OR name = ''"),
            ("Persons with empty common names",
             "SELECT COUNT(*) FROM stage_person_extraction WHERE common_name IS NULL OR common_name = ''"),
            ("Albums with empty titles",
             "SELECT COUNT(*) FROM stage_album_extraction WHERE title IS NULL OR title = ''"),
            ("Releases with empty titles",
             "SELECT COUNT(*) FROM stage_release_extraction WHERE title IS NULL OR title = ''"),
            ("Songs with MB recording IDs",
             "SELECT COUNT(*) FROM stage_song_extraction WHERE mb_recording_id IS NOT NULL"),
            ("Artists with MB artist IDs",
             "SELECT COUNT(*) FROM stage_artist_extraction WHERE mb_artist_id IS NOT NULL"),
            ("Albums with MB release group IDs",
             "SELECT COUNT(*) FROM stage_album_extraction WHERE mb_release_group_id IS NOT NULL"),
            ("Releases with MB release IDs",
             "SELECT COUNT(*) FROM stage_release_extraction WHERE mb_release_id IS NOT NULL"),
            ("Person records ready for extraction",
             "SELECT COUNT(*) FROM stage_person_extraction")
        ]

        for check_name, query in validation_checks:
            count = self.conn.execute(query).fetchone()[0]
            status = "✅" if count > 0 or "empty" in check_name.lower() else "⚠️"
            print(f"    {status} {check_name}: {count:,}")

        # Check for duplicates
        dup_checks = [
            ("Duplicate MB recording IDs",
             "SELECT COUNT(*) - COUNT(DISTINCT mb_recording_id) FROM stage_song_extraction WHERE mb_recording_id IS NOT NULL"),
            ("Duplicate MB artist IDs",
             "SELECT COUNT(*) - COUNT(DISTINCT mb_artist_id) FROM stage_artist_extraction WHERE mb_artist_id IS NOT NULL"),
            ("Duplicate MB release group IDs",
             "SELECT COUNT(*) - COUNT(DISTINCT mb_release_group_id) FROM stage_album_extraction WHERE mb_release_group_id IS NOT NULL"),
            ("Duplicate MB release IDs",
             "SELECT COUNT(*) - COUNT(DISTINCT mb_release_id) FROM stage_release_extraction WHERE mb_release_id IS NOT NULL"),
            ("Duplicate KEXP track IDs",
             "SELECT COUNT(*) - COUNT(DISTINCT kexp_track_id_internal) FROM stage_song_extraction WHERE kexp_track_id_internal IS NOT NULL"),
            ("Duplicate KEXP artist IDs",
             "SELECT COUNT(*) - COUNT(DISTINCT kexp_artist_id_internal) FROM stage_artist_extraction WHERE kexp_artist_id_internal IS NOT NULL")
        ]

        for check_name, query in dup_checks:
            dup_count = self.conn.execute(query).fetchone()[0]
            status = "✅" if dup_count == 0 else "⚠️"
            print(f"    {status} {check_name}: {dup_count:,}")

    def populate_kb_tables(self):
        """Populate actual KB tables from validated staging data."""
        print("\n📝 Populating KB tables from staging data...")

        # 1. Populate kb_Song
        print("    Populating kb_Song...")
        self.conn.execute("""
            INSERT INTO kb_Song (kb_id, title, mb_recording_id, type, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                title,
                mb_recording_id,
                'SONG'::work_of_art_type as type,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_song_extraction
            WHERE mb_recording_id IS NOT NULL
            ON CONFLICT (mb_recording_id) DO UPDATE SET
                title = EXCLUDED.title,
                updated_at = EXCLUDED.updated_at
        """)

        # Add songs without MB IDs
        self.conn.execute("""
            INSERT INTO kb_Song (kb_id, title, type, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                title,
                'SONG'::work_of_art_type as type,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_song_extraction
            WHERE mb_recording_id IS NULL
        """)

        # Now populate the bridge table for kb_Song to KEXP tracks
        self.conn.execute("""
            INSERT INTO bridge_kb_song_to_kexp (kb_song_id, kexp_track_id_internal)
            SELECT
                s.kb_id,
                sse.kexp_track_id_internal
            FROM kb_Song s
            JOIN stage_song_extraction sse ON 
                (s.mb_recording_id = sse.mb_recording_id) OR 
                (s.mb_recording_id IS NULL AND sse.mb_recording_id IS NULL AND s.title = sse.title)
            WHERE sse.kexp_track_id_internal IS NOT NULL
            ON CONFLICT DO NOTHING
        """)

        song_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Song").fetchone()[0]
        print(f"        ✅ {song_inserted:,} songs in kb_Song")

        # 2. Populate kb_Person first
        print("    Populating kb_Person...")
        self.conn.execute("""
            INSERT INTO kb_Person (kb_id, common_name, legal_name, mb_person_id, gender, nationality, disambiguation, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                common_name,
                legal_name,
                mb_person_id,
                gender,
                nationality,
                disambiguation,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_person_extraction
            ON CONFLICT (mb_person_id) DO UPDATE SET
                common_name = EXCLUDED.common_name,
                legal_name = EXCLUDED.legal_name,
                gender = EXCLUDED.gender,
                nationality = EXCLUDED.nationality,
                disambiguation = EXCLUDED.disambiguation,
                updated_at = EXCLUDED.updated_at
        """)

        person_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Person").fetchone()[0]
        print(f"        ✅ {person_inserted:,} persons in kb_Person")

        # 3. Populate kb_Artist
        print("    Populating kb_Artist...")

        # First, artists who are not persons (groups, orchestras, etc.)
        self.conn.execute("""
            INSERT INTO kb_Artist (kb_id, name, mb_artist_id, kb_artist_type, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                name,
                mb_artist_id,
                artist_type::artist_type,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_artist_extraction
            WHERE is_person = FALSE OR is_person IS NULL
            ON CONFLICT (mb_artist_id) DO UPDATE SET
                name = EXCLUDED.name,
                kb_artist_type = EXCLUDED.kb_artist_type,
                updated_at = EXCLUDED.updated_at
        """)

        # Now handle person artists - link them to kb_Person
        self.conn.execute("""
            INSERT INTO kb_Artist (kb_id, name, mb_artist_id, kb_artist_type, kb_person_id, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                a.name,
                a.mb_artist_id,
                a.artist_type::artist_type,
                p.kb_id as kb_person_id,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_artist_extraction a
            JOIN kb_Person p ON a.mb_artist_id = p.mb_person_id
            WHERE a.is_person = TRUE
            ON CONFLICT (mb_artist_id) DO UPDATE SET
                name = EXCLUDED.name,
                kb_artist_type = EXCLUDED.kb_artist_type,
                kb_person_id = EXCLUDED.kb_person_id,
                updated_at = EXCLUDED.updated_at
        """)

        # Now populate the bridge table for kb_Artist to KEXP artists
        self.conn.execute("""
            INSERT INTO bridge_kb_artist_to_kexp (kb_artist_id, kexp_artist_id_internal)
            SELECT
                a.kb_id,
                sae.kexp_artist_id_internal
            FROM kb_Artist a
            JOIN stage_artist_extraction sae ON 
                (a.mb_artist_id = sae.mb_artist_id) OR 
                (a.mb_artist_id IS NULL AND sae.mb_artist_id IS NULL AND a.name = sae.name)
            WHERE sae.kexp_artist_id_internal IS NOT NULL
            ON CONFLICT DO NOTHING
        """)

        artist_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Artist").fetchone()[0]
        print(f"        ✅ {artist_inserted:,} artists in kb_Artist")

        # Record person-artist mappings for reference
        linked_count = self.conn.execute("""
            SELECT COUNT(*) 
            FROM kb_Artist 
            WHERE kb_person_id IS NOT NULL
        """).fetchone()[0]
        print(f"        ✅ {linked_count:,} artists linked to persons")

        # 4. Populate kb_Album
        print("    Populating kb_Album...")
        self.conn.execute("""
            INSERT INTO kb_Album (kb_id, title, mb_release_group_id, type, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                title,
                mb_release_group_id,
                'ALBUM'::work_of_art_type as type,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_album_extraction
            WHERE mb_release_group_id IS NOT NULL
            ON CONFLICT (mb_release_group_id) DO UPDATE SET
                title = EXCLUDED.title,
                updated_at = EXCLUDED.updated_at
        """)

        # Add albums without MB IDs
        self.conn.execute("""
            INSERT INTO kb_Album (kb_id, title, type, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                title,
                'ALBUM'::work_of_art_type as type,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_album_extraction
            WHERE mb_release_group_id IS NULL
        """)

        album_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Album").fetchone()[0]
        print(f"        ✅ {album_inserted:,} albums in kb_Album")

        # 5. Populate kb_Release
        print("    Populating kb_Release...")

        # First, releases with MB IDs linked to albums with MB IDs
        self.conn.execute("""
            INSERT INTO kb_Release (kb_id, title, mb_release_id, album_id, release_date, format, barcode, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                r.title,
                r.mb_release_id,
                a.kb_id as album_id,
                r.release_date,
                r.format,
                r.barcode,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_release_extraction r
            JOIN kb_Album a ON r.mb_release_group_id = a.mb_release_group_id
            WHERE r.mb_release_id IS NOT NULL
            AND r.mb_release_group_id IS NOT NULL
            ON CONFLICT (mb_release_id) DO UPDATE SET
                title = EXCLUDED.title,
                album_id = EXCLUDED.album_id,
                release_date = EXCLUDED.release_date,
                format = EXCLUDED.format,
                barcode = EXCLUDED.barcode,
                updated_at = EXCLUDED.updated_at
        """)

        # Add releases without MB IDs or without album linkage
        self.conn.execute("""
            INSERT INTO kb_Release (kb_id, title, release_date, format, barcode, created_at, updated_at)
            SELECT
                uuid() as kb_id,
                title,
                release_date,
                format,
                barcode,
                CURRENT_TIMESTAMP as created_at,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_release_extraction
            WHERE mb_release_id IS NULL
            OR mb_release_group_id IS NULL
        """)

        release_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Release").fetchone()[0]
        print(f"        ✅ {release_inserted:,} releases in kb_Release")

        # Relationships will be populated in Phase 3
        print("    Skipping relationship population (will be done in Phase 3)")
        relationship_count = 0

        print(f"\n✅ Phase 2 core entities populated successfully!")

        # Summary report
        print(f"\n📊 PHASE 2 COMPLETION SUMMARY")
        print(f"{'='*50}")
        print(f"    Songs extracted: {song_inserted:,}")
        print(f"    Artists extracted: {artist_inserted:,}")
        print(f"    Persons extracted: {person_inserted:,}")
        print(f"    Albums extracted: {album_inserted:,}")
        print(f"    Releases extracted: {release_inserted:,}")
        print(
            f"    Total core entities: {song_inserted + artist_inserted + person_inserted + album_inserted + release_inserted:,}")

    def cleanup_staging_tables(self, keep_staging: bool = True):
        """Clean up staging tables (optional)."""
        if not keep_staging:
            print("\n🧹 Cleaning up staging tables...")
            staging_tables = [
                'stage_song_extraction',
                'stage_artist_extraction',
                'stage_person_extraction',
                'stage_album_extraction',
                'stage_release_extraction'
            ]

            for table in staging_tables:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")

            print("✅ Staging tables cleaned up")
        else:
            print(f"\n📋 Staging tables preserved for inspection:")
            print(f"    - stage_song_extraction")
            print(f"    - stage_artist_extraction")
            print(f"    - stage_person_extraction")
            print(f"    - stage_album_extraction")
            print(f"    - stage_release_extraction")

    def run_full_extraction(self, cleanup: bool = False):
        """Run the complete Phase 2 extraction process."""
        print("🎵 KEXP Knowledge Base - Phase 2 Core Entity Extraction")
        print("=" * 60)

        try:
            # Connect and validate
            self.connect()
            if not self.validate_prerequisites():
                print("❌ Prerequisites validation failed. Aborting.")
                return False

            # Create staging and extract
            self.create_staging_tables()
            self.extract_songs_to_staging()
            self.extract_artists_to_staging()
            self.extract_persons_to_staging()
            self.extract_albums_to_staging()
            self.extract_releases_to_staging()

            # Validate and populate
            self.validate_staging_data()
            # self.populate_kb_tables()

            # Cleanup
            self.cleanup_staging_tables(keep_staging=not cleanup)

            print(f"\n🎉 Phase 2 extraction completed successfully!")
            return True

        except Exception as e:
            print(f"\n❌ Error during Phase 2 extraction: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if self.conn:
                self.conn.close()
                print(f"\n🔐 Database connection closed.")


def main():
    """Main execution function."""
    extractor = Phase2CoreEntityExtractor()

    # Parse command line arguments
    cleanup_staging = "--cleanup" in sys.argv

    success = extractor.run_full_extraction(cleanup=cleanup_staging)

    if success:
        print(f"\n✅ Ready for Phase 3: Relationship Enrichment")
        print(f"📋 Next steps:")
        print(f"    1. Run Phase 3 to populate relationship tables")
        print(f"    2. Review core entities in KB tables")
        print(f"    3. Validate entity and relationship quality")
    else:
        print(f"\n❌ Phase 2 extraction failed. Check logs and data.")
        sys.exit(1)


if __name__ == "__main__":
    main()
