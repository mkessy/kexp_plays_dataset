#!/usr/bin/env python3
"""
KEXP Knowledge Base - RDF-First Schema Creation
Streamlined schema: Core entities + Genre/Location from MB + single kb_Relationship table
"""

import duckdb
import os
from typing import Optional

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


def create_rdf_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create complete RDF-focused schema"""

    # Drop everything first
    drop_all_kb_objects(conn)

    # Create ENUM types
    create_enum_types(conn)

    # Create core entity tables
    create_entity_tables(conn)

    # Create single RDF relationship table
    create_relationship_table(conn)

    # Create bridge tables for KEXP mapping
    create_bridge_tables(conn)


def drop_all_kb_objects(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop all KB objects for clean slate"""
    print("🔥 Dropping all existing Knowledge Base objects...")

    # Drop bridge tables
    for table in ['bridge_kb_song_to_kexp', 'bridge_kb_artist_to_kexp', 'bridge_kb_song_to_mb']:
        conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    # Drop relationship table
    conn.execute("DROP TABLE IF EXISTS kb_Relationship CASCADE")

    # Drop all legacy rel_* tables
    legacy_rel_tables = [
        'rel_Artist_Person_Role_Played_Role', 'rel_Entity_Has_URL',
        'rel_Artist_Originates_From_Location', 'rel_Album_Has_Genre',
        'rel_Song_Has_Genre', 'rel_Artist_Has_Genre', 'rel_Artist_Plays_Instrument',
        'rel_Artist_Performed_At_Event', 'rel_Release_By_Label',
        'rel_Song_Appears_On_Release', 'rel_Song_Based_On_Work',
        'rel_Artist_Member_Of_Artist', 'rel_Artist_Performed_Song'
    ]

    for table in legacy_rel_tables:
        conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    # Drop all KB entity tables (including ones we're removing)
    entity_tables = [
        'kb_KexpComment', 'kb_Play', 'kb_Show', 'kb_Program', 'kb_Host',
        'kb_Artist_Person_Role', 'kb_Event', 'kb_Venue', 'kb_Role', 'kb_URL',
        'kb_Date_Entity', 'kb_Release', 'kb_Album', 'kb_Song', 'kb_Work',
        'kb_Instrument', 'kb_Genre', 'kb_RecordLabel', 'kb_Artist', 'kb_Person', 'kb_Location'
    ]

    for table in entity_tables:
        conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    # Drop ENUM types
    enum_types = ['artist_type', 'event_type', 'link_type',
                  'work_of_art_type', 'entity_type', 'role_category']
    for enum_type in enum_types:
        conn.execute(f"DROP TYPE IF EXISTS {enum_type} CASCADE")

    print("✅ All KB objects dropped successfully")


def create_enum_types(conn: duckdb.DuckDBPyConnection) -> None:
    """Create required ENUM types"""
    print("🏗️  Creating ENUM types...")

    conn.execute("""
        CREATE TYPE artist_type AS ENUM (
            'PERSON', 'GROUP', 'ORCHESTRA', 'CHOIR', 'CHARACTER', 'OTHER'
        )
    """)

    print("✅ ENUM types created")


def create_entity_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all KB entity tables"""
    print("🏗️  Creating KB entity tables...")

    # Core music entities
    conn.execute("""
        CREATE TABLE kb_Artist (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            name VARCHAR NOT NULL,
            sort_name VARCHAR,
            type artist_type,
            mb_artist_id UUID UNIQUE,
            begin_date DATE,
            end_date DATE,
            disambiguation VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_Song (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            title VARCHAR NOT NULL,
            length_ms INTEGER,
            mb_recording_id UUID UNIQUE,
            disambiguation VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_Album (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            title VARCHAR NOT NULL,
            mb_release_group_id UUID UNIQUE,
            primary_type VARCHAR,
            secondary_types VARCHAR,
            first_release_date DATE,
            disambiguation VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_Release (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            title VARCHAR NOT NULL,
            mb_release_id UUID UNIQUE,
            album_id UUID,
            release_date DATE,
            country VARCHAR,
            barcode VARCHAR,
            disambiguation VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Supporting entities - ONLY Genre and Location from MB data
    conn.execute("""
        CREATE TABLE kb_Genre (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            name VARCHAR NOT NULL UNIQUE,
            mb_genre_id UUID,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_Location (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            name VARCHAR NOT NULL,
            mb_area_id UUID UNIQUE,
            type VARCHAR,
            country_code VARCHAR,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            
        )
    """)

    conn.execute("""
        CREATE TABLE kb_RecordLabel (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            name VARCHAR NOT NULL,
            mb_label_id UUID,
            country VARCHAR,
            label_code INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # KEXP broadcast entities
    conn.execute("""
        CREATE TABLE kb_Host (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            name VARCHAR NOT NULL,
            kexp_host_id BIGINT UNIQUE,
            host_uri VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_Program (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            name VARCHAR NOT NULL,
            kexp_program_id BIGINT UNIQUE,
            program_uri VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_Show (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            name VARCHAR,
            kexp_show_id BIGINT UNIQUE,
            program_id UUID,
            host_id UUID,
            show_date DATE,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_Play (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            kexp_play_id BIGINT UNIQUE,
            song_id UUID,
            artist_id UUID,
            show_id UUID,
            play_timestamp TIMESTAMP,
            comment_id UUID,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE kb_KexpComment (
            kb_id UUID PRIMARY KEY DEFAULT uuid(),
            comment_text VARCHAR,
            kexp_comment_id BIGINT UNIQUE,
            play_id UUID,
            comment_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    print("✅ KB entity tables created")


def create_relationship_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create single RDF relationship table"""
    print("🏗️  Creating RDF relationship table...")

    conn.execute("""
        CREATE TABLE kb_Relationship (
            triple_id VARCHAR PRIMARY KEY,
            subject_type VARCHAR NOT NULL,
            subject_id VARCHAR NOT NULL,
            predicate VARCHAR NOT NULL,
            object_type VARCHAR NOT NULL,
            object_id VARCHAR NOT NULL,
            source_name VARCHAR,
            target_name VARCHAR,
            mb_relation_type VARCHAR,
            mb_target_type VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for performance
    conn.execute(
        "CREATE INDEX idx_kb_rel_subject ON kb_Relationship(subject_id)")
    conn.execute(
        "CREATE INDEX idx_kb_rel_object ON kb_Relationship(object_id)")
    conn.execute(
        "CREATE INDEX idx_kb_rel_predicate ON kb_Relationship(predicate)")
    conn.execute(
        "CREATE INDEX idx_kb_rel_subject_type ON kb_Relationship(subject_type)")
    conn.execute(
        "CREATE INDEX idx_kb_rel_object_type ON kb_Relationship(object_type)")

    print("✅ RDF relationship table created with indexes")


def create_bridge_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create bridge tables for KEXP mapping"""
    print("🏗️  Creating bridge tables...")

    conn.execute("""
        CREATE TABLE bridge_kb_artist_to_kexp (
            kb_artist_id UUID,
            kexp_artist_id_internal UUID,
            PRIMARY KEY (kb_artist_id, kexp_artist_id_internal)
        )
    """)

    conn.execute("""
        CREATE TABLE bridge_kb_song_to_kexp (
            kb_song_id UUID,
            kexp_track_id_internal UUID,
            PRIMARY KEY (kb_song_id, kexp_track_id_internal)
        )
    """)

    print("✅ Bridge tables created")


def main():
    """Create RDF-focused schema"""
    print("=" * 60)
    print("KEXP Knowledge Base - RDF Schema Creation")
    print("=" * 60)

    conn = duckdb.connect(DB_PATH)
    try:
        create_rdf_schema(conn)
        print("\n🎉 RDF Schema created successfully!")
        print("📝 Note: All relationships use the single kb_Relationship table")
        print("📝 Only kb_Genre and kb_Location from MB data")
        print("📝 Person/Instrument data expressed as relationships")
    except Exception as e:
        print(f"❌ Schema creation failed: {e}")
        raise
    finally:
        conn.close()
        print("🔐 Database connection closed")


if __name__ == "__main__":
    main()
