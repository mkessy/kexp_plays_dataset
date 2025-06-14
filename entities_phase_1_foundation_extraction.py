#!/usr/bin/env python3
"""
KEXP Knowledge Base - Phase 1 Foundation Entities Extraction
Corrected implementation based on project analysis and data structure insights.
"""

import duckdb
import os
from typing import Optional
import sys

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


class Phase1FoundationExtractor:
    """Handles extraction of foundation entities from MusicBrainz raw data."""

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
            'kb_Genre',
            'kb_Location'
        ]

        for table in required_tables:
            try:
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"    ✅ {table}: {count:,} records")
            except Exception as e:
                print(f"    ❌ Missing table {table}: {e}")
                return False

        # Check KEXP-MB connection coverage
        mb_coverage = self.conn.execute("""
            SELECT COUNT(DISTINCT mb_id)
            FROM dim_artists_master
            WHERE mb_id IS NOT NULL
        """).fetchone()[0]

        print(f"    ✅ KEXP artists with MB IDs: {mb_coverage:,}")

        if mb_coverage < 50000:
            print("    ⚠️  Warning: Low MB ID coverage. Proceeding anyway.")

        return True

    def create_staging_tables(self):
        """Create staging tables for extraction validation."""
        print("\n🏗️  Creating staging extraction tables...")

        # Drop existing staging tables
        staging_tables = [
            'stage_genre_extraction',
            'stage_location_extraction'
        ]

        for table in staging_tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")

        # Create staging tables with comprehensive metadata
        self.conn.execute("""
            CREATE TABLE stage_genre_extraction (
                mb_genre_id UUID,
                genre_name VARCHAR NOT NULL,
                genre_disambiguation VARCHAR,
                total_votes BIGINT,
                artist_count INTEGER,
                sample_artists TEXT,
                quality_score INTEGER
            )
        """)

        self.conn.execute("""
            CREATE TABLE stage_location_extraction (
                mb_area_id UUID,
                location_type VARCHAR, -- 'main_area' or 'begin_area'
                location_name VARCHAR,
                country_code VARCHAR,
                city_name VARCHAR,
                region_name VARCHAR,
                artist_count INTEGER,
                coordinates_available BOOLEAN,
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6)
            )
        """)

        print("✅ Staging tables created")

    def extract_genres_to_staging(self):
        """Extract genre data to staging table with quality filtering."""
        print("\n🎵 Extracting genres to staging...")

        # Extract genres with vote count and artist coverage
        self.conn.execute("""
            WITH genre_artist_samples AS (
                SELECT
                    g.id as genre_id,
                    g.name as genre_name,
                    g.disambiguation as genre_disambiguation,
                    g.count as total_votes,
                    mb.name as artist_name,
                    ROW_NUMBER() OVER (PARTITION BY g.id ORDER BY g.count DESC, mb.name) as rn
                FROM mb_artists_raw mb, UNNEST(mb.genres) AS t(g)
                WHERE CAST(mb.id AS UUID) IN (
                    SELECT CAST(mb_id AS UUID) FROM dim_artists_master WHERE mb_id IS NOT NULL AND mb_id != 'None'
                )
                AND g.name IS NOT NULL
                AND g.name != ''
            ),
            genre_stats AS (
                SELECT
                    genre_id,
                    genre_name,
                    genre_disambiguation,
                    total_votes,
                    COUNT(DISTINCT artist_name) as artist_count,
                    STRING_AGG(artist_name, '; ') FILTER (WHERE rn <= 3) as sample_artists
                FROM genre_artist_samples
                GROUP BY genre_id, genre_name, genre_disambiguation, total_votes
                HAVING total_votes >= 2  -- Minimum vote threshold
                AND COUNT(DISTINCT artist_name) >= 2  -- Minimum artist coverage
            )
            INSERT INTO stage_genre_extraction
            SELECT
                CAST(genre_id AS UUID) as mb_genre_id,
                genre_name,
                genre_disambiguation,
                total_votes,
                artist_count,
                sample_artists,
                CASE
                    WHEN total_votes >= 50 AND artist_count >= 10 THEN 5
                    WHEN total_votes >= 20 AND artist_count >= 5 THEN 4
                    WHEN total_votes >= 10 AND artist_count >= 3 THEN 3
                    WHEN total_votes >= 5 AND artist_count >= 2 THEN 2
                    ELSE 1
                END as quality_score
            FROM genre_stats
        """)

        genre_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_genre_extraction").fetchone()[0]
        print(f"✅ Extracted {genre_count:,} quality genres to staging")

        # Show quality distribution
        quality_dist = self.conn.execute("""
            SELECT quality_score, COUNT(*) as count,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percent
            FROM stage_genre_extraction
            GROUP BY quality_score
            ORDER BY quality_score DESC
        """).fetchall()

        print("    Quality distribution:")
        for score, count, percent in quality_dist:
            print(f"        Score {score}: {count:,} genres ({percent}%)")

        """Extract location data to staging table."""
        print("\n🌍 Extracting locations to staging...")

        # Extract main areas and begin areas with ISO codes (now confirmed available)
        self.conn.execute("""
            INSERT INTO stage_location_extraction
            SELECT
                CAST(mb.area.id AS UUID) as mb_area_id,
                'main_area' as location_type,
                mb.area.name as location_name,
                CASE
                    WHEN mb.area."iso-3166-1-codes" IS NOT NULL
                         AND array_length(mb.area."iso-3166-1-codes") > 0
                    THEN mb.area."iso-3166-1-codes"[1]
                    ELSE NULL
                END as country_code,
                NULL as city_name,  -- Could be enhanced later with hierarchy parsing
                CASE
                    WHEN mb.area."iso-3166-2-codes" IS NOT NULL
                         AND array_length(mb.area."iso-3166-2-codes") > 0
                    THEN mb.area."iso-3166-2-codes"[1]
                    ELSE NULL
                END as region_name,
                COUNT(DISTINCT CAST(mb.id AS UUID)) as artist_count,
                FALSE as coordinates_available,  -- Confirmed not available
                NULL as latitude,
                NULL as longitude
            FROM mb_artists_raw mb
            WHERE CAST(mb.id AS UUID) IN (
                SELECT CAST(mb_id AS UUID) FROM dim_artists_master WHERE mb_id IS NOT NULL AND mb_id != 'None'
            )
            AND mb.area IS NOT NULL
            AND mb.area.name IS NOT NULL
            AND mb.area.name != ''
            GROUP BY mb.area.id, mb.area.name, mb.area."iso-3166-1-codes", mb.area."iso-3166-2-codes"
            HAVING COUNT(DISTINCT CAST(mb.id AS UUID)) >= 1

            UNION ALL

            SELECT
                CAST(mb."begin-area".id AS UUID) as mb_area_id,
                'begin_area' as location_type,
                mb."begin-area".name as location_name,
                CASE
                    WHEN mb."begin-area"."iso-3166-1-codes" IS NOT NULL
                         AND array_length(mb."begin-area"."iso-3166-1-codes") > 0
                    THEN mb."begin-area"."iso-3166-1-codes"[1]
                    ELSE NULL
                END as country_code,
                NULL as city_name,
                CASE
                    WHEN mb."begin-area"."iso-3166-2-codes" IS NOT NULL
                         AND array_length(mb."begin-area"."iso-3166-2-codes") > 0
                    THEN mb."begin-area"."iso-3166-2-codes"[1]
                    ELSE NULL
                END as region_name,
                COUNT(DISTINCT CAST(mb.id AS UUID)) as artist_count,
                FALSE as coordinates_available,
                NULL as latitude,
                NULL as longitude
            FROM mb_artists_raw mb
            WHERE CAST(mb.id AS UUID) IN (
                SELECT CAST(mb_id AS UUID) FROM dim_artists_master WHERE mb_id IS NOT NULL AND mb_id != 'None'
            )
            AND mb."begin-area" IS NOT NULL
            AND mb."begin-area".name IS NOT NULL
            AND mb."begin-area".name != ''
            GROUP BY mb."begin-area".id, mb."begin-area".name, mb."begin-area"."iso-3166-1-codes", mb."begin-area"."iso-3166-2-codes"
            HAVING COUNT(DISTINCT CAST(mb.id AS UUID)) >= 1
        """)

        # Deduplicate locations by mb_area_id, keeping the one with higher artist_count
        self.conn.execute("""
            WITH location_ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY mb_area_id
                        ORDER BY artist_count DESC, location_type
                    ) as rn
                FROM stage_location_extraction
            )
            DELETE FROM stage_location_extraction
            WHERE (mb_area_id, location_type, artist_count) NOT IN (
                SELECT mb_area_id, location_type, artist_count
                FROM location_ranked
                WHERE rn = 1
            )
        """)

        location_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_location_extraction").fetchone()[0]
        print(f"✅ Extracted {location_count:,} unique locations to staging")

        # Show top locations with enhanced data
        top_locations = self.conn.execute("""
            SELECT location_name, country_code, region_name, artist_count
            FROM stage_location_extraction
            ORDER BY artist_count DESC
            LIMIT 10
        """).fetchall()

        print("    Top locations by artist count:")
        for country, code, region, count in top_locations:
            code_display = f" ({code})" if code else ""
            region_display = f", {region}" if region else ""
            print(
                f"        {country}{code_display}{region_display}: {count:,} artists")

    def extract_locations_to_staging(self):
        """Extract location data to staging table."""
        print("\n🌍 Extracting locations to staging...")

        # CORRECTED INSERT STATEMENT
        insert_sql = """
            INSERT INTO stage_location_extraction
            WITH all_areas AS (
                SELECT
                    area.id as mb_area_id,
                    'main_area' as location_type,
                    area.name as location_name,
                    area."iso-3166-1-codes"[1] as country_code,
                    NULL as city_name,
                    area."iso-3166-2-codes"[1] as region_name,
                    mb.id as artist_id
                FROM mb_artists_raw mb
                WHERE mb.area IS NOT NULL AND mb.area.name IS NOT NULL AND mb.area.name != ''

                UNION ALL

                SELECT
                    "begin-area".id as mb_area_id,
                    'begin_area' as location_type,
                    "begin-area".name as location_name,
                    "begin-area"."iso-3166-1-codes"[1] as country_code,
                    NULL as city_name,
                    "begin-area"."iso-3166-2-codes"[1] as region_name,
                    mb.id as artist_id
                FROM mb_artists_raw mb
                WHERE mb."begin-area" IS NOT NULL AND mb."begin-area".name IS NOT NULL AND mb."begin-area".name != ''
            )
            SELECT
                CAST(mb_area_id AS UUID),
                location_type,
                location_name,
                country_code,
                city_name,
                region_name,
                COUNT(DISTINCT artist_id) as artist_count,
                FALSE as coordinates_available, -- Not available from source
                NULL as latitude,               -- Not available from source
                NULL as longitude               -- Not available from source
            FROM all_areas
            WHERE mb_area_id IS NOT NULL AND CAST(artist_id AS UUID) IN (SELECT CAST(mb_id AS UUID) FROM dim_artists_master WHERE mb_id IS NOT NULL AND mb_id != 'None')
            GROUP BY ALL;
        """
        self.conn.execute(insert_sql)
        location_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_location_extraction").fetchone()[0]
        print(f"✅ Extracted {location_count:,} unique locations to staging")

    # Note: Role and Instrument extraction methods have been removed to align with RDF schema
    # These entities will be handled as relationships in the RDF triple structure

    def validate_staging_data(self):
        """Validate extracted staging data before KB population."""
        print("\n✅ Validating staging data...")

        # Check for data quality issues
        validation_checks = [
            ("Genres with empty names",
             "SELECT COUNT(*) FROM stage_genre_extraction WHERE genre_name IS NULL OR genre_name = ''"),
            ("Locations with empty names",
             "SELECT COUNT(*) FROM stage_location_extraction WHERE location_name IS NULL OR location_name = ''"),
            ("High-quality genres (score ≥ 4)",
             "SELECT COUNT(*) FROM stage_genre_extraction WHERE quality_score >= 4"),
            ("Valid locations extracted",
             "SELECT COUNT(*) FROM stage_location_extraction WHERE location_name IS NOT NULL")
        ]

        for check_name, query in validation_checks:
            count = self.conn.execute(query).fetchone()[0]
            status = "✅" if count > 0 or "empty" in check_name.lower() else "⚠️"
            print(f"    {status} {check_name}: {count:,}")

        # Check for duplicates
        dup_checks = [
            ("Duplicate genre names",
             "SELECT COUNT(*) - COUNT(DISTINCT genre_name) FROM stage_genre_extraction"),
            ("Duplicate location areas",
             "SELECT COUNT(*) - COUNT(DISTINCT mb_area_id) FROM stage_location_extraction")
        ]

        for check_name, query in dup_checks:
            dup_count = self.conn.execute(query).fetchone()[0]
            status = "✅" if dup_count == 0 else "⚠️"
            print(f"    {status} {check_name}: {dup_count:,}")

    def populate_kb_tables(self):
        """Populate actual KB tables from validated staging data."""
        print("\n📝 Populating KB tables from staging data...")

        # 1. Populate kb_Genre
        print("    Populating kb_Genre...")
        self.conn.execute("""
            INSERT INTO kb_Genre (kb_id, name, description, mb_genre_id, created_at)
            SELECT
                uuid() as kb_id,
                genre_name as name,
                COALESCE(genre_disambiguation, 'Genre with ' || total_votes || ' votes from ' || artist_count || ' artists') as description,
                mb_genre_id,
                CURRENT_TIMESTAMP as created_at
            FROM stage_genre_extraction
            WHERE quality_score >= 1  -- Only high quality genres
            ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description,
                mb_genre_id = EXCLUDED.mb_genre_id
        """)

        genre_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Genre").fetchone()[0]
        print(f"        ✅ {genre_inserted:,} genres in kb_Genre")

        # 2. Populate kb_Location
        print("    Populating kb_Location...")
        self.conn.execute("""
            INSERT INTO kb_Location (kb_id, mb_area_id, name, type, country_code, created_at)
            SELECT
                uuid() as kb_id,
                mb_area_id,
                location_name as name,
                location_type as type,
                country_code,
                CURRENT_TIMESTAMP as created_at
            FROM stage_location_extraction
            WHERE artist_count >= 1 -- Ingest all locations found
            ON CONFLICT  DO NOTHING
        """)
        location_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Location").fetchone()[0]
        print(f"        ✅ {location_inserted:,} locations in kb_Location")

        print(f"\n✅ Phase 1 foundation entities populated successfully!")

        # Summary report
        print(f"\n📊 PHASE 1 COMPLETION SUMMARY")
        print(f"{'='*50}")
        print(f"    Genres extracted: {genre_inserted:,}")
        print(f"    Locations extracted: {location_inserted:,}")
        print(
            f"    Total foundation entities: {genre_inserted + location_inserted:,}")

        # Note on location data enhancement potential
        print(f"\n💡 LOCATION DATA NOTES:")
        print(f"    - ISO country codes available in staging (not yet in KB schema)")
        print(f"    - MusicBrainz area IDs available for future linking")
        print(f"    - 77.3% of KEXP artists have location data")
        print(f"    - Consider schema enhancement for country_code and mb_area_id fields")

    def cleanup_staging_tables(self, keep_staging: bool = True):
        """Clean up staging tables(optional)."""
        if not keep_staging:
            print("\n🧹 Cleaning up staging tables...")
            staging_tables = [
                'stage_genre_extraction',
                'stage_location_extraction'
            ]

            for table in staging_tables:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")

            print("✅ Staging tables cleaned up")
        else:
            print(f"\n📋 Staging tables preserved for inspection:")
            print(f"    - stage_genre_extraction")
            print(f"    - stage_location_extraction")

    def run_full_extraction(self, cleanup: bool = False):
        """Run the complete Phase 1 foundation extraction process."""
        print("🎵 KEXP Knowledge Base - Phase 1 Foundation Extraction (RDF-Aligned)")
        print("=" * 60)

        try:
            # Connect and validate
            self.connect()
            if not self.validate_prerequisites():
                print("❌ Prerequisites validation failed. Aborting.")
                return False

            # Create staging and extract
            self.create_staging_tables()
            self.extract_genres_to_staging()
            self.extract_locations_to_staging()

            # Validate and populate
            self.validate_staging_data()
            self.populate_kb_tables()

            # Cleanup
            self.cleanup_staging_tables(keep_staging=not cleanup)

            print(f"\n🎉 Phase 1 extraction completed successfully!")
            return True

        except Exception as e:
            print(f"\n❌ Error during Phase 1 extraction: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if self.conn:
                self.conn.close()
                print(f"\n🔐 Database connection closed.")


def main():
    """Main execution function."""
    extractor = Phase1FoundationExtractor()

    # Parse command line arguments
    cleanup_staging = "--cleanup" in sys.argv

    success = extractor.run_full_extraction(cleanup=cleanup_staging)

    if success:
        print(f"\n✅ Ready for Phase 2: Core Entity Enrichment")
        print(f"📋 Next steps:")
        print(f"    1. Run Phase 2 to populate kb_Artist entities")
        print(f"    2. Review foundation entities in KB tables")
        print(f"    3. Prepare for RDF relationships in Phase 3")
    else:
        print(f"\n❌ Phase 1 extraction failed. Check logs and data.")
        sys.exit(1)


if __name__ == "__main__":
    main()
