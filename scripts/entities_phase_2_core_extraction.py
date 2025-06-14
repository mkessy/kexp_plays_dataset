#!/usr/bin/env python3
"""
KEXP Knowledge Base - Phase 2 Core Entities Extraction
Implementation for populating kb_Song, kb_Album, kb_Release, kb_Artist
aligned with simplified RDF schema (no kb_Person/kb_Instrument).
"""

import duckdb
import os
import sys
import traceback
from typing import Optional

# --- Configuration ---
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


class Phase2CoreEntityExtractor:
    """Handles extraction of core entities from KEXP and MusicBrainz data."""

    def __init__(self, db_path: str = DB_PATH):
        """Initializes the extractor with the database path."""
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connects to the DuckDB database."""
        try:
            self.conn = duckdb.connect(self.db_path)
            print(f"✅ Connected to database: {self.db_path}")
            return self.conn
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            raise

    def validate_prerequisites(self) -> bool:
        """Validates that all required source and destination tables exist."""
        print("\n🔍 Validating prerequisites...")
        if not self.conn:
            print("❌ Database connection is not available.")
            return False

        required_tables = [
            'mb_artists_raw', 'dim_artists_master', 'dim_tracks', 'dim_releases_master', 'fact_plays',
            'kb_Artist', 'kb_Song', 'kb_Album', 'kb_Release',  # Removed kb_Person
            'bridge_kb_artist_to_kexp', 'bridge_kb_song_to_kexp'
        ]
        all_exist = True
        for table in required_tables:
            try:
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  - ✅ Table '{table}' exists with {count:,} records.")
            except duckdb.Error as e:
                print(f"  - ❌ Missing required table '{table}': {e}")
                all_exist = False

        if not all_exist:
            print(
                "\nError: One or more required tables are missing. Please ensure all previous scripts have run.")
            return False

        return True

    def create_staging_tables(self):
        """Creates fresh staging tables for this extraction phase."""
        print("\n🏗️  Creating or replacing staging tables...")
        staging_tables = [
            'stage_song_extraction', 'stage_artist_extraction',
            'stage_album_extraction', 'stage_release_extraction'
        ]
        for table in staging_tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")

        # Staging table for Songs (Recordings)
        self.conn.execute("""
            CREATE TABLE stage_song_extraction (
                kexp_track_id_internal UUID PRIMARY KEY,
                title VARCHAR NOT NULL,
                mb_recording_id UUID,
                length_ms INTEGER
            )
        """)

        # Staging table for Artists (simplified - no person handling)
        self.conn.execute("""
            CREATE TABLE stage_artist_extraction (
                kexp_artist_id_internal UUID PRIMARY KEY,
                name VARCHAR NOT NULL,
                mb_artist_id UUID,
                artist_type VARCHAR,
                begin_date DATE,
                end_date DATE,
                disambiguation VARCHAR
            )
        """)

        # Staging table for Albums (Release Groups)
        self.conn.execute("""
            CREATE TABLE stage_album_extraction (
                mb_release_group_id UUID PRIMARY KEY,
                title VARCHAR NOT NULL,
                primary_type VARCHAR,
                secondary_types VARCHAR,
                first_release_date DATE
            )
        """)

        # Staging table for Releases
        self.conn.execute("""
            CREATE TABLE stage_release_extraction (
                kexp_release_id_internal UUID PRIMARY KEY,
                title VARCHAR NOT NULL,
                mb_release_id UUID,
                mb_release_group_id UUID,
                release_date DATE,
                country VARCHAR,
                barcode VARCHAR
            )
        """)
        print("✅ Staging tables created successfully.")

    def extract_songs_to_staging(self):
        """Extracts song (recording) data from dim_tracks into a staging table."""
        print("\n🎵 Extracting songs to staging...")
        self.conn.execute("""
            INSERT INTO stage_song_extraction(kexp_track_id_internal, title, mb_recording_id)
            SELECT
                track_id_internal,
                primary_song_title_observed,
                mb_recording_id,
            FROM dim_tracks;
        """)
        count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_song_extraction").fetchone()[0]
        print(f"  - ✅ Extracted {count:,} total songs to staging.")

    def extract_artists_to_staging(self):
        """Extracts artist data from KEXP and MusicBrainz into a staging table."""
        print("\n👨‍🎤 Extracting artists to staging...")

        # Simplified artist extraction - no person/group distinction
        self.conn.execute("""
            INSERT INTO stage_artist_extraction
            SELECT
                kexp.artist_id_internal as kexp_artist_id_internal,
                kexp.primary_name_observed as name,
                kexp.mb_id as mb_artist_id,
                CASE 
                    WHEN mb.type = 'Person' THEN 'PERSON'
                    WHEN mb.type = 'Group' THEN 'GROUP'
                    WHEN mb.type = 'Orchestra' THEN 'ORCHESTRA'
                    WHEN mb.type = 'Character' THEN 'CHARACTER'
                    ELSE 'OTHER' 
                END as artist_type,
                -- Parse date strings to proper DATE format
                CASE 
                    WHEN regexp_extract(mb."life-span".begin, '(\\d{4}-\\d{2}-\\d{2})', 1) IS NOT NULL 
                    THEN try_cast(regexp_extract(mb."life-span".begin, '(\\d{4}-\\d{2}-\\d{2})', 1) AS DATE)
                    WHEN regexp_extract(mb."life-span".begin, '(\\d{4})', 1) IS NOT NULL
                    THEN try_cast(regexp_extract(mb."life-span".begin, '(\\d{4})', 1) || '-01-01' AS DATE)
                    ELSE NULL
                END as begin_date,
                CASE 
                    WHEN regexp_extract(mb."life-span".end, '(\\d{4}-\\d{2}-\\d{2})', 1) IS NOT NULL 
                    THEN try_cast(regexp_extract(mb."life-span".end, '(\\d{4}-\\d{2}-\\d{2})', 1) AS DATE)
                    WHEN regexp_extract(mb."life-span".end, '(\\d{4})', 1) IS NOT NULL
                    THEN try_cast(regexp_extract(mb."life-span".end, '(\\d{4})', 1) || '-01-01' AS DATE)
                    ELSE NULL
                END as end_date,
                mb.disambiguation
            FROM dim_artists_master AS kexp
            LEFT JOIN mb_artists_raw AS mb ON kexp.mb_id = CAST(mb.id AS UUID)
            WHERE kexp.mb_id IS NOT NULL AND kexp.mb_id != 'None'
        """)
        count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_artist_extraction").fetchone()[0]
        print(f"  - ✅ Extracted {count:,} total artists to staging.")

    def extract_albums_releases_to_staging(self):
        """Extracts Album (Release Group) and Release data into staging tables."""
        print("\n💿 Extracting albums and releases to staging...")

        # Extract unique release groups with the most frequently used title
        self.conn.execute("""
            INSERT INTO stage_album_extraction(mb_release_group_id, title, first_release_date)
            SELECT
                mb_release_group_id,
                arg_max(primary_album_name_observed, play_count) AS title,
                MIN(release_date_iso) as first_release_date
            FROM (
                SELECT
                    r.mb_release_group_id,
                    r.primary_album_name_observed,
                    r.release_date_iso,
                    count(p.play_id) as play_count
                FROM dim_releases_master r
                JOIN dim_tracks t ON r.release_id_internal = t.release_id_internal_on_track
                JOIN fact_plays p ON t.track_id_internal = p.track_id_internal
                WHERE r.mb_release_group_id IS NOT NULL
                GROUP BY r.mb_release_group_id, r.primary_album_name_observed, r.release_date_iso
            ) AS release_group_titles
            GROUP BY mb_release_group_id;
        """)
        album_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_album_extraction").fetchone()[0]
        print(
            f"  - ✅ Extracted {album_count:,} unique albums (release groups) to staging.")

        # Populate Release staging table
        self.conn.execute("""
            INSERT INTO stage_release_extraction(kexp_release_id_internal, title, mb_release_id, mb_release_group_id, release_date)
            SELECT
                release_id_internal,
                primary_album_name_observed,
                mb_release_id,
                mb_release_group_id,
                release_date_iso
            FROM dim_releases_master;
        """)
        release_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_release_extraction").fetchone()[0]
        print(f"  - ✅ Extracted {release_count:,} total releases to staging.")

    def populate_kb_tables(self):
        """Populates the actual Knowledge Base tables from the staged data."""
        print("\n📝 Populating final KB tables from staged data...")

        # --- Populate Entity Tables ---
        # Populate kb_Artist (simplified - no person linking)
        self.conn.execute("""
            INSERT INTO kb_Artist(kb_id, name, sort_name, type, mb_artist_id, begin_date, end_date, disambiguation, created_at, updated_at)
            SELECT
                uuid(),
                name,
                name as sort_name,  -- Use name as sort_name for now
                artist_type::artist_type,
                mb_artist_id,
                begin_date,
                end_date,
                disambiguation,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM stage_artist_extraction
            WHERE mb_artist_id IS NOT NULL
            ON CONFLICT (mb_artist_id) DO NOTHING;
        """)

        # Insert artists without MB ID (avoid duplicates by name)
        self.conn.execute("""
            INSERT INTO kb_Artist(kb_id, name, sort_name, type, created_at, updated_at)
            SELECT
                uuid(),
                name,
                name as sort_name,
                'OTHER'::artist_type,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM stage_artist_extraction sa
            WHERE sa.mb_artist_id IS NULL 
              AND NOT EXISTS (
                SELECT 1 FROM kb_Artist ka 
                WHERE ka.name = sa.name AND ka.mb_artist_id IS NULL
            );
        """)
        artist_count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Artist").fetchone()[0]
        print(f"  - Populated kb_Artist: {artist_count:,} entities")

        # Populate kb_Album
        self.conn.execute("""
            INSERT INTO kb_Album(kb_id, title, mb_release_group_id, primary_type, first_release_date, created_at, updated_at)
            SELECT 
                uuid(), 
                title, 
                mb_release_group_id, 
                primary_type,
                first_release_date,
                CURRENT_TIMESTAMP, 
                CURRENT_TIMESTAMP
            FROM stage_album_extraction
            ON CONFLICT (mb_release_group_id) DO NOTHING;
        """)
        album_count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Album").fetchone()[0]
        print(f"  - Populated kb_Album: {album_count:,} entities")

        # Populate kb_Release (link to albums where possible)
        self.conn.execute("""
            INSERT INTO kb_Release(kb_id, title, mb_release_id, album_id, release_date, created_at, updated_at)
            SELECT
                uuid(),
                sr.title,
                sr.mb_release_id,
                ka.kb_id,
                sr.release_date,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM stage_release_extraction sr
            LEFT JOIN kb_Album ka ON sr.mb_release_group_id = ka.mb_release_group_id
            WHERE sr.mb_release_id IS NOT NULL
            ON CONFLICT (mb_release_id) DO NOTHING;
        """)
        release_count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Release").fetchone()[0]
        print(f"  - Populated kb_Release: {release_count:,} entities")

        # Populate kb_Song
        self.conn.execute("""
            INSERT INTO kb_Song(kb_id, title, length_ms, mb_recording_id, created_at, updated_at)
            SELECT 
                uuid(), 
                title, 
                length_ms,
                mb_recording_id, 
                CURRENT_TIMESTAMP, 
                CURRENT_TIMESTAMP
            FROM stage_song_extraction
            WHERE mb_recording_id IS NOT NULL
            ON CONFLICT (mb_recording_id) DO NOTHING;
        """)

        # Insert songs without MB ID (careful with duplicates)
        self.conn.execute("""
            INSERT INTO kb_Song(kb_id, title, length_ms, created_at, updated_at)
            SELECT 
                uuid(), 
                title, 
                length_ms,
                CURRENT_TIMESTAMP, 
                CURRENT_TIMESTAMP
            FROM stage_song_extraction
            WHERE mb_recording_id IS NULL
              AND NOT EXISTS (
                SELECT 1 FROM kb_Song ks 
                WHERE ks.title = stage_song_extraction.title AND ks.mb_recording_id IS NULL
            );
        """)
        song_count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Song").fetchone()[0]
        print(f"  - Populated kb_Song: {song_count:,} entities")

        # --- Populate Bridge Tables ---
        print("\n🔗 Populating bridge tables for traceability...")

        # Bridge artists with MB IDs
        self.conn.execute("""
            INSERT INTO bridge_kb_artist_to_kexp (kb_artist_id, kexp_artist_id_internal)
            SELECT
                ka.kb_id,
                sa.kexp_artist_id_internal
            FROM stage_artist_extraction sa
            JOIN kb_Artist ka ON sa.mb_artist_id = ka.mb_artist_id
            WHERE sa.mb_artist_id IS NOT NULL
            ON CONFLICT DO NOTHING;
        """)

        # Bridge artists without MB IDs (match by name)
        self.conn.execute("""
            INSERT INTO bridge_kb_artist_to_kexp (kb_artist_id, kexp_artist_id_internal)
            SELECT
                ka.kb_id,
                sa.kexp_artist_id_internal
            FROM stage_artist_extraction sa
            JOIN kb_Artist ka ON sa.name = ka.name AND ka.mb_artist_id IS NULL
            WHERE sa.mb_artist_id IS NULL
            ON CONFLICT DO NOTHING;
        """)

        # Bridge songs with MB IDs
        self.conn.execute("""
            INSERT INTO bridge_kb_song_to_kexp (kb_song_id, kexp_track_id_internal)
            SELECT
                ks.kb_id,
                ss.kexp_track_id_internal
            FROM stage_song_extraction ss
            JOIN kb_Song ks ON ss.mb_recording_id = ks.mb_recording_id
            WHERE ss.mb_recording_id IS NOT NULL
            ON CONFLICT DO NOTHING;
        """)

        # Skip songs without MB IDs to avoid cartesian product issue
        # This commented section was causing the 7 million entity problem
        # self.conn.execute("""
        #     INSERT INTO bridge_kb_song_to_kexp (kb_song_id, kexp_track_id_internal)
        #     SELECT
        #         MIN(ks.kb_id) as kb_song_id,
        #         ss.kexp_track_id_internal
        #     FROM stage_song_extraction ss
        #     JOIN kb_Song ks ON ss.title = ks.title AND ks.mb_recording_id IS NULL
        #     WHERE ss.mb_recording_id IS NULL
        #     GROUP BY ss.kexp_track_id_internal
        #     ON CONFLICT DO NOTHING;
        # """)
        print("  - ⚠️ Skipping non-MusicBrainz ID songs to avoid cartesian product issues")

        # Get bridge table counts
        artist_bridge_count = self.conn.execute(
            "SELECT COUNT(*) FROM bridge_kb_artist_to_kexp").fetchone()[0]
        song_bridge_count = self.conn.execute(
            "SELECT COUNT(*) FROM bridge_kb_song_to_kexp").fetchone()[0]

        print(f"  - ✅ Bridge tables populated:")
        print(f"    - Artist mappings: {artist_bridge_count:,}")
        print(f"    - Song mappings: {song_bridge_count:,}")

        print("\n📊 PHASE 2 COMPLETION SUMMARY")
        print(f"{'='*50}")
        for table_name, count in [
            ('kb_Artist', artist_count),
            ('kb_Song', song_count),
            ('kb_Album', album_count),
            ('kb_Release', release_count),
            ('bridge_kb_artist_to_kexp', artist_bridge_count),
            ('bridge_kb_song_to_kexp', song_bridge_count)
        ]:
            print(f"  {table_name:<25} {count:>10,} entities")

    def cleanup_staging_tables(self, keep_staging: bool = True):
        """Optionally cleans up staging tables."""
        if not keep_staging:
            print("\n🧹 Cleaning up staging tables...")
            staging_tables = [
                'stage_song_extraction', 'stage_artist_extraction',
                'stage_album_extraction', 'stage_release_extraction'
            ]
            for table in staging_tables:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")
            print("  - ✅ Staging tables cleaned up.")
        else:
            print("\n📋 Staging tables preserved for inspection.")

    def run_full_extraction(self, cleanup: bool = False):
        """Runs the complete Phase 2 extraction and population process."""
        print("🎵 KEXP Knowledge Base - Phase 2 Core Entity Extraction")
        print("=" * 60)
        try:
            self.connect()
            if not self.validate_prerequisites():
                return False

            self.create_staging_tables()
            self.extract_songs_to_staging()
            self.extract_artists_to_staging()
            self.extract_albums_releases_to_staging()

            self.populate_kb_tables()
            self.cleanup_staging_tables(keep_staging=not cleanup)

            print(f"\n🎉 Phase 2 extraction completed successfully!")
            return True

        except Exception as e:
            print(f"\n❌ Error during Phase 2 extraction: {e}")
            traceback.print_exc()
            return False
        finally:
            if self.conn:
                self.conn.close()
                print(f"\n🔐 Database connection closed.")


def main():
    """Main execution function."""
    extractor = Phase2CoreEntityExtractor()
    cleanup_staging = "--cleanup" in sys.argv
    success = extractor.run_full_extraction(cleanup=cleanup_staging)

    if success:
        print(f"\n✅ Ready for Phase 3: Relationship Enrichment")
    else:
        print(f"\n❌ Phase 2 extraction failed. Check logs and data.")
        sys.exit(1)


if __name__ == "__main__":
    main()
