#!/usr/bin/env python3
"""
Smart Relations Extraction - Separate Tables Approach
Create separate tables for different types of MB relationships to inspect and extract properly.
"""

import duckdb
import os

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


def connect_db() -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB database."""
    try:
        conn = duckdb.connect(DB_PATH)
        print(f"✅ Connected to database: {DB_PATH}")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        raise


def create_basic_relations_table(conn: duckdb.DuckDBPyConnection):
    """Create a basic flattened relations table first."""
    print("\n🔧 Creating basic relations table...")
    conn.execute("DROP TABLE IF EXISTS mb_relations_basic")
    conn.execute("""
        CREATE TABLE mb_relations_basic AS
        SELECT 
            CAST(mb.id AS UUID) as artist_mb_id,
            mb.name as artist_name,
            r.type as relation_type,
            r."target-type" as target_type,
            r.begin as begin_date,
            r.end as end_date,
            r."target-credit" as target_credit,
            r."source-credit" as source_credit
        FROM mb_artists_raw mb, UNNEST(mb.relations) AS t(r)
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM mb_relations_basic").fetchone()[0]
    print(f"✅ Created basic relations table with {count:,} records")

    # Show relation type distribution
    print("\n📊 Relation type distribution:")
    results = conn.execute("""
        SELECT relation_type, target_type, COUNT(*) as count
        FROM mb_relations_basic 
        GROUP BY relation_type, target_type 
        ORDER BY count DESC 
        LIMIT 10
    """).fetchall()

    for rel_type, target_type, count in results:
        print(f"   {rel_type} -> {target_type}: {count:,}")


def create_instrument_relations_table(conn: duckdb.DuckDBPyConnection):
    """Create dedicated table for instrument relationships."""
    print("\n🎸 Creating instrument relations table...")

    conn.execute("DROP TABLE IF EXISTS mb_instrument_relations")
    conn.execute("""
        CREATE TABLE mb_instrument_relations AS
        SELECT 
            CAST(mb.id AS UUID) as artist_mb_id,
            mb.name as artist_name,
            r.type as relation_type,
            r."target-type" as target_type,
            r.instrument as instrument_data
        FROM mb_artists_raw mb, UNNEST(mb.relations) AS t(r)
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
          AND r.type = 'instrument'
          AND r.instrument IS NOT NULL
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM mb_instrument_relations").fetchone()[0]
    print(f"✅ Created instrument relations table with {count:,} records")

    if count > 0:
        # Show sample instrument data
        print("\n🔍 Sample instrument data:")
        sample = conn.execute("""
            SELECT 
                artist_name,
                instrument_data.name as instrument_name,
                instrument_data.type as instrument_type
            FROM mb_instrument_relations 
            WHERE instrument_data.name IS NOT NULL
            LIMIT 5
        """).fetchall()

        for artist, instrument, inst_type in sample:
            print(f"   {artist} plays {instrument} ({inst_type})")


def create_genre_relations_table(conn: duckdb.DuckDBPyConnection):
    """Create dedicated table for genre data."""
    print("\n🎵 Creating genre relations table...")

    conn.execute("DROP TABLE IF EXISTS mb_genre_relations")
    conn.execute("""
        CREATE TABLE mb_genre_relations AS
        SELECT 
            CAST(mb.id AS UUID) as artist_mb_id,
            mb.name as artist_name,
            g as genre_data
        FROM mb_artists_raw mb, UNNEST(mb.genres) AS t(g)
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
          AND g.name IS NOT NULL
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM mb_genre_relations").fetchone()[0]
    print(f"✅ Created genre relations table with {count:,} records")

    if count > 0:
        # Show top genres
        print("\n🔍 Top genres:")
        top_genres = conn.execute("""
            SELECT 
                genre_data.name as genre_name,
                COUNT(DISTINCT artist_mb_id) as artist_count,
                SUM(genre_data.count) as total_votes
            FROM mb_genre_relations 
            GROUP BY genre_data.name, genre_data.id
            ORDER BY artist_count DESC 
            LIMIT 5
        """).fetchall()

        for genre, artists, votes in top_genres:
            print(f"   {genre}: {artists} artists ({votes} votes)")


def create_location_relations_table(conn: duckdb.DuckDBPyConnection):
    """Create dedicated table for location data."""
    print("\n🌍 Creating location relations table...")

    conn.execute("DROP TABLE IF EXISTS mb_location_relations")
    conn.execute("""
        CREATE TABLE mb_location_relations AS
        SELECT 
            CAST(mb.id AS UUID) as artist_mb_id,
            mb.name as artist_name,
            'main_area' as location_type,
            mb.area as location_data
        FROM mb_artists_raw mb
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
          AND mb.area IS NOT NULL
          AND mb.area.name IS NOT NULL
        
        UNION ALL
        
        SELECT 
            CAST(mb.id AS UUID) as artist_mb_id,
            mb.name as artist_name,
            'begin_area' as location_type,
            mb."begin-area" as location_data
        FROM mb_artists_raw mb
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
          AND mb."begin-area" IS NOT NULL
          AND mb."begin-area".name IS NOT NULL
    """)

    count = conn.execute(
        "SELECT COUNT(*) FROM mb_location_relations").fetchone()[0]
    print(f"✅ Created location relations table with {count:,} records")

    if count > 0:
        # Show top locations
        print("\n🔍 Top locations:")
        top_locations = conn.execute("""
            SELECT 
                location_data.name as location_name,
                COUNT(DISTINCT artist_mb_id) as artist_count
            FROM mb_location_relations 
            GROUP BY location_data.name, location_data.id
            ORDER BY artist_count DESC 
            LIMIT 5
        """).fetchall()

        for location, artists in top_locations:
            print(f"   {location}: {artists} artists")


def analyze_instrument_data(conn: duckdb.DuckDBPyConnection):
    """Analyze the instrument data structure."""
    print("\n🔍 ANALYZING INSTRUMENT DATA STRUCTURE")
    print("=" * 50)

    # Check if we have any instrument relations
    count = conn.execute(
        "SELECT COUNT(*) FROM mb_instrument_relations").fetchone()[0]
    if count == 0:
        print("❌ No instrument relations found!")

        # Let's check what relation types we DO have
        print("\n🔍 Available relation types:")
        rel_types = conn.execute("""
            SELECT relation_type, COUNT(*) as count
            FROM mb_relations_basic 
            GROUP BY relation_type 
            ORDER BY count DESC 
            LIMIT 20
        """).fetchall()

        for rel_type, count in rel_types:
            print(f"   {rel_type}: {count:,}")

        # Check if 'instrument' relations exist at all
        instrument_check = conn.execute("""
            SELECT COUNT(*) 
            FROM mb_relations_basic 
            WHERE relation_type = 'instrument'
        """).fetchone()[0]

        print(f"\n🔍 Relations with type 'instrument': {instrument_check:,}")

        # Let's see what the actual instrument data looks like
        print("\n🔍 Sample relations with instrument in name:")
        samples = conn.execute("""
            SELECT relation_type, target_type, COUNT(*) as count
            FROM mb_relations_basic 
            WHERE relation_type LIKE '%instrument%' 
               OR relation_type LIKE '%guitar%'
               OR relation_type LIKE '%piano%'
               OR relation_type LIKE '%drum%'
            GROUP BY relation_type, target_type
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        for rel_type, target_type, count in samples:
            print(f"   {rel_type} -> {target_type}: {count:,}")

    else:
        print(f"✅ Found {count:,} instrument relations")

        # Analyze instrument data structure
        print("\n🔍 Instrument data structure:")
        sample_data = conn.execute("""
            SELECT 
                instrument_data.name,
                instrument_data.type,
                instrument_data.description,
                COUNT(*) as usage_count
            FROM mb_instrument_relations 
            WHERE instrument_data.name IS NOT NULL
            GROUP BY 
                instrument_data.name,
                instrument_data.type,
                instrument_data.description
            ORDER BY usage_count DESC
            LIMIT 10
        """).fetchall()

        for name, inst_type, desc, count in sample_data:
            print(f"   {name} ({inst_type}): {count:,} usages")
            if desc:
                print(f"     Description: {desc}")


def create_final_extraction_tables(conn: duckdb.DuckDBPyConnection):
    """Create the final extraction tables based on our analysis."""
    print("\n🏗️  Creating final extraction tables...")

    # Drop existing extraction tables
    tables = ['extract_instruments', 'extract_roles',
              'extract_genres', 'extract_locations']
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    # Create extraction tables
    conn.execute("""
        CREATE TABLE extract_instruments (
            extract_id INTEGER,
            instrument_name VARCHAR,
            mb_instrument_id UUID,
            instrument_type VARCHAR,
            description VARCHAR,
            usage_count INTEGER,
            sample_artists VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE extract_roles (
            extract_id INTEGER,
            role_name VARCHAR,
            role_category VARCHAR,
            usage_count INTEGER,
            sample_relations VARCHAR,
            description VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE extract_genres (
            extract_id INTEGER,
            genre_name VARCHAR,
            mb_genre_id UUID,
            vote_count BIGINT,
            artist_count INTEGER,
            sample_artists VARCHAR,
            disambiguation VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE extract_locations (
            extract_id INTEGER,
            country_name VARCHAR,
            country_code VARCHAR(10),
            city_name VARCHAR,
            region_name VARCHAR,
            mb_area_id UUID,
            usage_count INTEGER,
            sample_artists VARCHAR,
            has_coordinates BOOLEAN,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6)
        )
    """)

    print("✅ Extraction tables created")


def extract_from_specialized_tables(conn: duckdb.DuckDBPyConnection):
    """Extract entities from the specialized tables."""
    print("\n📊 EXTRACTING FROM SPECIALIZED TABLES")
    print("=" * 50)

    # Extract instruments (if any exist)
    instrument_count = conn.execute(
        "SELECT COUNT(*) FROM mb_instrument_relations").fetchone()[0]
    if instrument_count > 0:
        print("\n🎸 Extracting instruments...")
        conn.execute("""
            INSERT INTO extract_instruments
            SELECT 
                row_number() OVER (ORDER BY usage_count DESC) as extract_id,
                instrument_name,
                mb_instrument_id,
                instrument_type,
                description,
                usage_count,
                sample_artists
            FROM (
                SELECT 
                    instrument_data.name as instrument_name,
                    instrument_data.id as mb_instrument_id,
                    instrument_data.type as instrument_type,
                    instrument_data.description as description,
                    COUNT(*) as usage_count,
                    string_agg(DISTINCT artist_name, ', ') as sample_artists
                FROM mb_instrument_relations 
                WHERE instrument_data.name IS NOT NULL
                GROUP BY 
                    instrument_data.name,
                    instrument_data.id,
                    instrument_data.type,
                    instrument_data.description
                HAVING COUNT(*) >= 3
            ) instruments
            ORDER BY usage_count DESC
        """)

        extracted = conn.execute(
            "SELECT COUNT(*) FROM extract_instruments").fetchone()[0]
        print(f"✅ Extracted {extracted:,} unique instruments")
    else:
        print("⚠️  No instrument data found to extract")

    # Extract roles
    print("\n🎭 Extracting roles...")
    conn.execute("""
        INSERT INTO extract_roles
        SELECT 
            row_number() OVER (ORDER BY usage_count DESC) as extract_id,
            relation_type as role_name,
            CASE 
                WHEN relation_type IN ('vocal', 'lead vocals', 'background vocals', 'choir vocals') 
                    THEN 'Vocals'
                WHEN relation_type LIKE '%guitar%' OR relation_type LIKE '%bass%' 
                    THEN 'Strings'
                WHEN relation_type LIKE '%drum%' OR relation_type LIKE '%percussion%' 
                    THEN 'Percussion'
                WHEN relation_type LIKE '%keyboard%' OR relation_type LIKE '%piano%' OR relation_type LIKE '%organ%' 
                    THEN 'Keys'
                WHEN relation_type IN ('producer', 'co-producer', 'executive producer') 
                    THEN 'Production'
                WHEN relation_type IN ('engineer', 'recording', 'mix', 'mastering', 'sound') 
                    THEN 'Engineering'
                WHEN relation_type IN ('composer', 'writer', 'lyricist', 'arranger', 'orchestrator') 
                    THEN 'Composition'
                WHEN relation_type = 'member of band' 
                    THEN 'Membership'
                WHEN relation_type IN ('conductor', 'performing orchestra', 'performer', 'main performer') 
                    THEN 'Performance'
                ELSE 'Other'
            END as role_category,
            usage_count,
            sample_relations,
            'Extracted from MusicBrainz relationship: ' || relation_type as description
        FROM (
            SELECT 
                relation_type,
                COUNT(*) as usage_count,
                string_agg(DISTINCT target_type, ', ') as sample_relations
            FROM mb_relations_basic
            WHERE target_type != 'url'
            GROUP BY relation_type
            HAVING COUNT(*) >= 5
        ) roles
        ORDER BY usage_count DESC
    """)

    role_count = conn.execute(
        "SELECT COUNT(*) FROM extract_roles").fetchone()[0]
    print(f"✅ Extracted {role_count:,} unique roles")

    # Extract genres
    print("\n🎵 Extracting genres...")
    conn.execute("""
        INSERT INTO extract_genres
        SELECT 
            row_number() OVER (ORDER BY artist_count DESC, vote_count DESC) as extract_id,
            genre_name,
            mb_genre_id,
            vote_count,
            artist_count,
            sample_artists,
            disambiguation
        FROM (
            SELECT 
                genre_data.name as genre_name,
                genre_data.id as mb_genre_id,
                SUM(genre_data.count) as vote_count,
                COUNT(DISTINCT artist_mb_id) as artist_count,
                string_agg(DISTINCT artist_name, ', ') as sample_artists,
                genre_data.disambiguation as disambiguation
            FROM mb_genre_relations 
            GROUP BY 
                genre_data.name,
                genre_data.id,
                genre_data.disambiguation
            HAVING COUNT(DISTINCT artist_mb_id) >= 3
        ) genres
        ORDER BY artist_count DESC, vote_count DESC
    """)

    genre_count = conn.execute(
        "SELECT COUNT(*) FROM extract_genres").fetchone()[0]
    print(f"✅ Extracted {genre_count:,} unique genres")

    # Extract locations
    print("\n🌍 Extracting locations...")
    conn.execute("""
        INSERT INTO extract_locations
        SELECT 
            row_number() OVER (ORDER BY usage_count DESC) as extract_id,
            location_name as country_name,
            country_code,
            NULL as city_name,
            NULL as region_name,
            mb_area_id,
            usage_count,
            sample_artists,
            FALSE as has_coordinates,
            NULL as latitude,
            NULL as longitude
        FROM (
            SELECT 
                location_data.name as location_name,
                location_data.id as mb_area_id,
                CASE 
                    WHEN array_length(location_data."iso-3166-1-codes") > 0 
                    THEN location_data."iso-3166-1-codes"[1]
                    WHEN location_data.name = 'United States' THEN 'US'
                    WHEN location_data.name = 'United Kingdom' THEN 'GB'
                    WHEN location_data.name = 'Germany' THEN 'DE'
                    ELSE NULL
                END as country_code,
                COUNT(DISTINCT artist_mb_id) as usage_count,
                string_agg(DISTINCT artist_name, ', ') as sample_artists
            FROM mb_location_relations 
            GROUP BY 
                location_data.name,
                location_data.id,
                location_data."iso-3166-1-codes"
            HAVING COUNT(DISTINCT artist_mb_id) >= 2
        ) locations
        ORDER BY usage_count DESC
    """)

    location_count = conn.execute(
        "SELECT COUNT(*) FROM extract_locations").fetchone()[0]
    print(f"✅ Extracted {location_count:,} unique locations")


def main():
    """Main execution function."""
    print("🎵 KEXP Knowledge Base - Smart Relations Extraction")
    print("=" * 60)

    conn = connect_db()

    try:
        # Step 1: Create basic relations table
        # create_basic_relations_table(conn)

        # Step 2: Create specialized tables for different data types
        create_instrument_relations_table(conn)
        # create_genre_relations_table(conn)
        # create_location_relations_table(conn)

        # Step 3: Analyze what we found
        # analyze_instrument_data(conn)

        # Step 4: Create extraction tables
        # create_final_extraction_tables(conn)

        # Step 5: Extract entities from specialized tables
        # extract_from_specialized_tables(conn)

        print("\n✅ Smart relations extraction complete!")
        print("📋 Review data in: mb_relations_basic, mb_instrument_relations, mb_genre_relations, mb_location_relations")
        print("📋 Extracted entities in: extract_instruments, extract_roles, extract_genres, extract_locations")

    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        import traceback
        traceback.print_exc()

    finally:
        conn.close()
        print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
