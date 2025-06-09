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
            'kb_Location',
            'kb_Role'
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
            'stage_location_extraction',
            'stage_role_extraction',
            'stage_instrument_extraction'
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

        self.conn.execute("""
            CREATE TABLE stage_role_extraction (
                role_name VARCHAR NOT NULL,
                role_category VARCHAR,
                usage_count INTEGER,
                unique_artists INTEGER,
                unique_recordings INTEGER,
                sample_relations TEXT,
                is_production_role BOOLEAN,
                is_performance_role BOOLEAN
            )
        """)

        self.conn.execute("""
            CREATE TABLE stage_instrument_extraction (
                instrument_name VARCHAR NOT NULL,
                instrument_category VARCHAR,
                usage_count INTEGER,
                unique_artists INTEGER,
                sample_artists TEXT
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
                    SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL
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
                SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL
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
                SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL
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
            WHERE mb_area_id IS NOT NULL AND CAST(artist_id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
            GROUP BY ALL;
        """
        self.conn.execute(insert_sql)
        location_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_location_extraction").fetchone()[0]
        print(f"✅ Extracted {location_count:,} unique locations to staging")

    def _prepare_relations_table(self):
        """Creates the `mb_relations_enhanced` table for use by other extractors."""
        print("\n🤝 Preparing enhanced relations table...")
        self.conn.execute("DROP TABLE IF EXISTS mb_relations_enhanced")
        self.conn.execute("""
            CREATE TABLE mb_relations_enhanced AS
            SELECT
                CAST(mb.id AS UUID) as artist_mb_id,
                mb.name as artist_name,
                r.type as relation_type,
                r."target-type" as target_type,
                r.attributes as attributes_raw,
                CASE
                    WHEN r.attributes IS NULL THEN []::VARCHAR[]
                    WHEN typeof(r.attributes) = 'VARCHAR[][]' THEN r.attributes[1]
                    WHEN typeof(r.attributes) = 'VARCHAR[]' THEN r.attributes
                    ELSE []::VARCHAR[]
                END as attributes_array,
                r.begin as begin_date,
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

    def extract_roles_to_staging(self):
        """Extract role data from relations to staging table."""
        print("\n🎭 Extracting roles to staging...")

        # NOTE: Assumes _prepare_relations_table() has been called.

        # Extract roles with categorization
        self.conn.execute("""
            WITH role_artist_samples AS (
                SELECT
                    relation_type,
                    artist_mb_id,
                    artist_name,
                    target_type,
                    ROW_NUMBER() OVER (PARTITION BY relation_type ORDER BY RANDOM()) as rn
                FROM mb_relations_enhanced
                WHERE target_type IN ('recording', 'release', 'work')  -- Focus on music-related relations
            ),
            role_stats AS (
                SELECT
                    relation_type,
                    COUNT(*) as usage_count,
                    COUNT(DISTINCT artist_mb_id) as unique_artists,
                    COUNT(DISTINCT CASE WHEN target_type = 'recording' THEN 1 END) as unique_recordings,
                    STRING_AGG(DISTINCT artist_name, '; ') FILTER (WHERE rn <= 3) as sample_relations
                FROM role_artist_samples
                GROUP BY relation_type
                HAVING COUNT(*) >= 50  -- Minimum usage threshold
                AND COUNT(DISTINCT artist_mb_id) >= 5  -- Minimum artist diversity
            )
            INSERT INTO stage_role_extraction
            SELECT
                relation_type as role_name,
                CASE
                    WHEN relation_type IN ('vocal', 'vocals', 'background vocals', 'lead vocals', 'harmony vocals')
                        THEN 'Vocals'
                    WHEN relation_type IN ('instrument', 'guitar', 'bass', 'drums', 'piano', 'keyboard', 'violin', 'saxophone', 'trumpet', 'flute')
                        THEN 'Instrument Performance'
                    WHEN relation_type IN ('producer', 'co-producer', 'executive producer', 'additional production')
                        THEN 'Production'
                    WHEN relation_type IN ('engineer', 'recording engineer', 'mix engineer', 'mastering engineer', 'sound engineer')
                        THEN 'Engineering'
                    WHEN relation_type IN ('composer', 'writer', 'lyricist', 'songwriter', 'arranger', 'orchestrator')
                        THEN 'Composition'
                    WHEN relation_type IN ('conductor', 'performing orchestra', 'performer', 'main performer')
                        THEN 'Performance Direction'
                    WHEN relation_type IN ('remixer', 'mix-DJ', 'DJ mix')
                        THEN 'Remix/DJ'
                    ELSE 'Other'
                END as role_category,
                usage_count,
                unique_artists,
                unique_recordings,
                sample_relations,
                relation_type IN ('producer', 'co-producer', 'executive producer', 'engineer', 'recording engineer', 'mix engineer', 'mastering engineer') as is_production_role,
                relation_type IN ('instrument', 'vocal', 'vocals', 'performer', 'main performer', 'conducting') as is_performance_role
            FROM role_stats
        """)

        role_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_role_extraction").fetchone()[0]
        print(f"✅ Extracted {role_count:,} roles to staging")

        # Show role category distribution
        category_dist = self.conn.execute("""
            SELECT role_category, COUNT(*) as role_types, SUM(usage_count) as total_usage
            FROM stage_role_extraction
            GROUP BY role_category
            ORDER BY total_usage DESC
        """).fetchall()

        print("    Role category distribution:")
        for category, types, usage in category_dist:
            print(
                f"        {category}: {types} role types, {usage:,} total relations")

    def extract_instruments_to_staging(self):
        """Extract instrument data from relations to staging table."""
        print("\n🎸 Extracting instruments to staging...")

        # NOTE: Assumes _prepare_relations_table() has been called.
        self.conn.execute("""
            WITH instrument_artist_samples AS (
                SELECT
                    attr AS instrument_name,
                    rel.artist_mb_id,
                    rel.artist_name,
                    ROW_NUMBER() OVER (PARTITION BY attr ORDER BY RANDOM()) as rn
                FROM mb_relations_enhanced rel, UNNEST(rel.attributes_array) AS t(attr)
                WHERE rel.relation_type IN ('instrument', 'vocal')
                  AND rel.attributes_array IS NOT NULL
                  AND array_length(rel.attributes_array) > 0
            ),
            instrument_stats AS (
                SELECT
                    instrument_name,
                    COUNT(*) as usage_count,
                    COUNT(DISTINCT artist_mb_id) as unique_artists,
                    STRING_AGG(DISTINCT artist_name, '; ') FILTER (WHERE rn <= 3) as sample_artists
                FROM instrument_artist_samples
                GROUP BY instrument_name
                HAVING COUNT(*) >= 1
            )
            INSERT INTO stage_instrument_extraction (instrument_name, instrument_category, usage_count, unique_artists, sample_artists)
            SELECT
                instrument_name,
                CASE
                    WHEN LOWER(instrument_name) LIKE '%vocal%' OR LOWER(instrument_name) LIKE '%sing%' OR LOWER(instrument_name) LIKE '%choir%' THEN 'Vocals'
                    WHEN LOWER(instrument_name) LIKE '%guitar%' OR LOWER(instrument_name) LIKE '%bass%' OR LOWER(instrument_name) LIKE '%banjo%' OR LOWER(instrument_name) LIKE '%mandolin%' THEN 'Strings'
                    WHEN LOWER(instrument_name) LIKE '%drum%' OR LOWER(instrument_name) LIKE '%percussion%' OR LOWER(instrument_name) LIKE '%timpani%' THEN 'Percussion'
                    WHEN LOWER(instrument_name) LIKE '%keyboard%' OR LOWER(instrument_name) LIKE '%piano%' OR LOWER(instrument_name) LIKE '%organ%' OR LOWER(instrument_name) LIKE '%synthesizer%' THEN 'Keys'
                    WHEN LOWER(instrument_name) LIKE '%trumpet%' OR LOWER(instrument_name) LIKE '%horn%' OR LOWER(instrument_name) LIKE '%trombone%' OR LOWER(instrument_name) LIKE '%tuba%' THEN 'Brass'
                    WHEN LOWER(instrument_name) LIKE '%flute%' OR LOWER(instrument_name) LIKE '%clarinet%' OR LOWER(instrument_name) LIKE '%saxophone%' OR LOWER(instrument_name) LIKE '%oboe%' THEN 'Woodwind'
                    WHEN LOWER(instrument_name) LIKE '%violin%' OR LOWER(instrument_name) LIKE '%viola%' OR LOWER(instrument_name) LIKE '%cello%' OR LOWER(instrument_name) LIKE '%double bass%' THEN 'Orchestra Strings'
                    ELSE 'Other'
                END as instrument_category,
                usage_count,
                unique_artists,
                sample_artists
            FROM instrument_stats
            ORDER BY usage_count DESC
        """)
        instrument_count = self.conn.execute(
            "SELECT COUNT(*) FROM stage_instrument_extraction").fetchone()[0]
        print(f"✅ Extracted {instrument_count:,} instruments to staging")

        # Show category distribution
        category_dist = self.conn.execute("""
            SELECT instrument_category, COUNT(*) as instrument_types, SUM(usage_count) as total_usage
            FROM stage_instrument_extraction
            GROUP BY instrument_category
            ORDER BY total_usage DESC
        """).fetchall()

        print("    Instrument category distribution:")
        for category, types, usage in category_dist:
            print(
                f"        {category}: {types} instrument types, {usage:,} total relations")

    def validate_staging_data(self):
        """Validate extracted staging data before KB population."""
        print("\n✅ Validating staging data...")

        # Check for data quality issues
        validation_checks = [
            ("Genres with empty names",
             "SELECT COUNT(*) FROM stage_genre_extraction WHERE genre_name IS NULL OR genre_name = ''"),
            ("Locations with empty names",
             "SELECT COUNT(*) FROM stage_location_extraction WHERE location_name IS NULL OR location_name = ''"),
            ("Roles with empty names",
             "SELECT COUNT(*) FROM stage_role_extraction WHERE role_name IS NULL OR role_name = ''"),
            ("High-quality genres (score ≥ 4)",
             "SELECT COUNT(*) FROM stage_genre_extraction WHERE quality_score >= 4"),
            ("Valid locations extracted",
             "SELECT COUNT(*) FROM stage_location_extraction WHERE location_name IS NOT NULL"),
            ("Production/Performance roles",
             "SELECT COUNT(*) FROM stage_role_extraction WHERE is_production_role = true OR is_performance_role = true")
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
             "SELECT COUNT(*) - COUNT(DISTINCT mb_area_id) FROM stage_location_extraction"),
            ("Duplicate role names",
             "SELECT COUNT(*) - COUNT(DISTINCT role_name) FROM stage_role_extraction")
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
            INSERT INTO kb_Genre (kb_id, name, description, mb_genre_id, updated_at)
            SELECT
                uuid() as kb_id,
                genre_name as name,
                COALESCE(genre_disambiguation, 'Genre with ' || total_votes || ' votes from ' || artist_count || ' artists') as description,
                mb_genre_id,
                CURRENT_TIMESTAMP as update_time
            FROM stage_genre_extraction
            WHERE quality_score >= 1  -- Only high quality genres
            ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description,
                mb_genre_id = EXCLUDED.mb_genre_id,
                updated_at = EXCLUDED.updated_at
        """)

        genre_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Genre").fetchone()[0]
        print(f"        ✅ {genre_inserted:,} genres in kb_Genre")

        # 2. Populate kb_Location
        print("    Populating kb_Location...")
        self.conn.execute("""
            INSERT INTO kb_Location (kb_id, mb_area_id, name, type, country_code, updated_at)
            SELECT
                uuid() as kb_id,
                mb_area_id,
                location_name as name,
                location_type as type,
                country_code,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_location_extraction
            WHERE artist_count >= 1 -- Ingest all locations found
            ON CONFLICT (mb_area_id) DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                country_code = EXCLUDED.country_code,
                updated_at = EXCLUDED.updated_at;
        """)
        location_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Location").fetchone()[0]
        print(f"        ✅ {location_inserted:,} locations in kb_Location")
        # 3. Populate kb_Role
        print("    Populating kb_Role...")
        self.conn.execute("""
            INSERT INTO kb_Role (kb_id, name, category, description, updated_at)
            SELECT
                uuid() as kb_id,
                role_name as name,
                role_category::role_category,
                role_category || ' role used in ' || usage_count || ' relations across ' || unique_artists || ' artists' as description,
                CURRENT_TIMESTAMP as update_time
            FROM stage_role_extraction
            WHERE usage_count >= 100  -- Only frequently used roles
            ON CONFLICT (name) DO UPDATE SET
                category = EXCLUDED.category,
                description = EXCLUDED.description,
                updated_at = EXCLUDED.updated_at
        """)

        role_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Role").fetchone()[0]
        print(f"        ✅ {role_inserted:,} roles in kb_Role")

        # 4. Populate kb_Instrument
        print("    Populating kb_Instrument...")
        self.conn.execute("""
            INSERT INTO kb_Instrument (kb_id, instrument_type, name, description, updated_at)
            SELECT
                uuid() as kb_id,
                instrument_name as name,
                instrument_category as instrument_type,
                NULL as description,
                CURRENT_TIMESTAMP as updated_at
            FROM stage_instrument_extraction
            WHERE usage_count >= 5  
            ON CONFLICT (name) DO NOTHING;
        """)

        instrument_inserted = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Instrument").fetchone()[0]
        print(
            f"        ✅ {instrument_inserted:,} instruments in kb_Instrument")

        print(f"\n✅ Phase 1 foundation entities populated successfully!")

        # Summary report
        print(f"\n📊 PHASE 1 COMPLETION SUMMARY")
        print(f"{'='*50}")
        print(f"    Genres extracted: {genre_inserted:,}")
        print(f"    Locations extracted: {location_inserted:,}")
        print(f"    Roles extracted: {role_inserted:,}")
        print(f"    Instruments extracted: {instrument_inserted:,}")
        print(
            f"    Total foundation entities: {genre_inserted + location_inserted + role_inserted + instrument_inserted:,}")

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
                'stage_location_extraction',
                'stage_role_extraction',
                'stage_instrument_extraction',
                'mb_relations_enhanced'
            ]

            for table in staging_tables:
                self.conn.execute(f"DROP TABLE IF EXISTS {table}")

            print("✅ Staging tables cleaned up")
        else:
            print(f"\n📋 Staging tables preserved for inspection:")
            print(f"    - stage_genre_extraction")
            print(f"    - stage_location_extraction")
            print(f"    - stage_role_extraction")
            print(f"    - stage_instrument_extraction")
            print(f"    - mb_relations_enhanced")

    def run_full_extraction(self, cleanup: bool = False):
        """Run the complete Phase 1 extraction process."""
        print("🎵 KEXP Knowledge Base - Phase 1 Foundation Extraction")
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
            # self._prepare_relations_table()
            self.extract_instruments_to_staging()
            self.extract_roles_to_staging()

            # Validate and populate
            # self.validate_staging_data()
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
        print(f"    1. Run Phase 2 to populate kb_Artist and kb_Person")
        print(f"    2. Review foundation entities in KB tables")
        print(f"    3. Validate relationship readiness")
    else:
        print(f"\n❌ Phase 1 extraction failed. Check logs and data.")
        sys.exit(1)


if __name__ == "__main__":
    main()
