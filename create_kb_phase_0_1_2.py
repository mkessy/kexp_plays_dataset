#!/usr/bin/env python3
"""
KEXP Knowledge Base - Phase 0 & 1: Schema and Foundation
This script drops and recreates the entire Knowledge Base schema, including
all tables and ENUM types, preparing it for data population.
"""
import duckdb
import os
import traceback
from typing import Optional

# --- Configuration ---
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")


# --- OBJECT LISTS FOR DROPPING ---

# List of all Knowledge Base tables to be dropped, in an order that respects dependencies.
KB_TABLES_TO_DROP = [
    # Bridge tables first
    "bridge_kb_song_to_kexp",
    "bridge_kb_artist_to_kexp",
    # Relationship tables
    "rel_Artist_Person_Role_Played_Role",
    "rel_Entity_Has_URL",
    "rel_Artist_Originates_From_Location",
    "rel_Album_Has_Genre",
    "rel_Song_Has_Genre",
    "rel_Artist_Has_Genre",
    "rel_Artist_Plays_Instrument",
    "rel_Artist_Performed_At_Event",
    "rel_Release_By_Label",
    "rel_Song_Appears_On_Release",
    "rel_Song_Based_On_Work",
    "rel_Artist_Member_Of_Artist",
    "rel_Artist_Performed_Song",
    # Core entity tables (those referenced by other tables dropped last)
    "kb_Artist_Person_Role",
    "kb_Event",
    "kb_Venue",
    "kb_Role",
    "kb_URL",
    "kb_Date_Entity",
    "kb_Release",
    "kb_Album",
    "kb_Song",
    "kb_Work",
    "kb_Instrument",
    "kb_Genre",
    "kb_RecordLabel",
    "kb_Artist",
    "kb_Person",
    "kb_Location"
]

# List of all ENUM types to be dropped
KB_ENUMS_TO_DROP = [
    "artist_type",
    "event_type",
    "link_type",
    "work_of_art_type",
    "entity_type",
    "role_category",
]


def drop_all_kb_objects(conn: duckdb.DuckDBPyConnection):
    """Drops all knowledge base tables and ENUM types for a clean slate."""
    print("\n🔥 Dropping all existing Knowledge Base objects...")
    for table in KB_TABLES_TO_DROP:
        conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    print("  - ✅ Dropped all KB tables.")

    for enum in KB_ENUMS_TO_DROP:
        conn.execute(f"DROP TYPE IF EXISTS {enum} CASCADE;")
    print("  - ✅ Dropped all KB ENUM types.")
    print("🔥 All KB objects dropped successfully.")


def create_enum_types(conn: duckdb.DuckDBPyConnection):
    """Creates all custom ENUM types required for the KB schema."""
    print("\n🏗️  Creating ENUM types...")
    enum_statements = [
        "CREATE TYPE artist_type AS ENUM ('PERSON', 'GROUP', 'CHARACTER', 'ORCHESTRA', 'OTHER');",
        "CREATE TYPE event_type AS ENUM ('SHOW', 'FESTIVAL', 'IN_STUDIO_SESSION', 'OTHER');",
        "CREATE TYPE link_type AS ENUM ('OFFICIAL_WEBSITE', 'BANDCAMP', 'ARTICLE', 'PERFORMANCE_VIDEO', 'SOCIAL_MEDIA', 'EVENT_PAGE', 'DISCOGS', 'ALLMUSIC', 'LASTFM', 'WIKIDATA', 'STREAMING', 'OTHER');",
        "CREATE TYPE work_of_art_type AS ENUM ('SONG', 'ALBUM');",
        "CREATE TYPE entity_type AS ENUM ('ARTIST', 'SONG', 'RELEASE', 'LABEL', 'EVENT', 'GENRE', 'LOCATION', 'PERSON', 'ROLE', 'INSTRUMENT', 'WORK');",
        "CREATE TYPE role_category AS ENUM ('Vocals', 'Instrument Performance', 'Production', 'Engineering', 'Composition', 'Performance Direction', 'Remix/DJ', 'Other');"
    ]
    for stmt in enum_statements:
        conn.execute(stmt)
    print("  - ✅ ENUM types created.")


def create_kb_tables(conn: duckdb.DuckDBPyConnection):
    """Creates all tables for the Knowledge Base with appropriate constraints."""
    print("\n🏗️  Creating KB tables...")
    table_statements = [
        # --- Entity Tables ---
        '''
        CREATE TABLE kb_Location (
            kb_id UUID PRIMARY KEY,
            mb_area_id UUID UNIQUE,
            name VARCHAR NOT NULL,
            type VARCHAR,
            country_code VARCHAR(3),
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Person (
            kb_id UUID PRIMARY KEY,
            legal_name TEXT NULL,
            common_name TEXT NOT NULL,
            mb_person_id UUID UNIQUE,
            gender VARCHAR(50) NULL,
            nationality VARCHAR(50) NULL,
            disambiguation TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Artist (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            mb_artist_id UUID UNIQUE,
            country_id UUID NULL REFERENCES kb_Location(kb_id),
            kb_artist_type artist_type,
            kb_person_id UUID NULL REFERENCES kb_Person(kb_id),
            disambiguation TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Work (
            kb_id UUID PRIMARY KEY,
            title VARCHAR NOT NULL,
            mb_work_id UUID UNIQUE,
            work_type VARCHAR,
            language VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Song (
            kb_id UUID PRIMARY KEY,
            title TEXT NOT NULL,
            type work_of_art_type DEFAULT 'SONG',
            mb_recording_id UUID UNIQUE,
            mb_work_id UUID NULL, -- This can be populated later
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Album (
            kb_id UUID PRIMARY KEY,
            title TEXT NOT NULL,
            type work_of_art_type DEFAULT 'ALBUM',
            mb_release_group_id UUID UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Release (
            kb_id UUID PRIMARY KEY,
            album_id UUID NULL REFERENCES kb_Album(kb_id),
            title TEXT NOT NULL,
            mb_release_id UUID UNIQUE,
            release_date DATE NULL,
            country_id UUID NULL REFERENCES kb_Location(kb_id),
            format VARCHAR(100) NULL,
            barcode TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_RecordLabel (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            mb_label_id UUID UNIQUE,
            country TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Genre (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            mb_genre_id UUID UNIQUE,
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Instrument (
            kb_id UUID PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            mb_instrument_id UUID UNIQUE,
            instrument_type VARCHAR,
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Venue (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            location_id UUID NULL REFERENCES kb_Location(kb_id),
            mb_id UUID UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Event (
            kb_id UUID PRIMARY KEY,
            event_name TEXT NULL,
            kb_event_type event_type,
            start_date DATE NULL,
            end_date DATE NULL,
            description TEXT NULL,
            venue_id UUID NULL REFERENCES kb_Venue(kb_id),
            location_id UUID NULL REFERENCES kb_Location(kb_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Date_Entity (
            kb_id UUID PRIMARY KEY,
            full_date DATE NULL,
            year INTEGER NULL,
            month INTEGER NULL,
            day INTEGER NULL,
            qualifier VARCHAR(100) NULL,
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (full_date, qualifier)
        );''',
        '''CREATE TABLE kb_URL (
            kb_id UUID PRIMARY KEY,
            address TEXT NOT NULL UNIQUE,
            kb_link_type link_type,
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Role (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            category role_category,
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        '''CREATE TABLE kb_Artist_Person_Role (
            kb_id UUID PRIMARY KEY,
            kb_artist_id UUID NULL REFERENCES kb_Artist(kb_id),
            kb_person_id UUID REFERENCES kb_Person(kb_id),
            kb_role_id UUID REFERENCES kb_Role(kb_id),
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',

        # --- Relationship Tables ---
        '''CREATE TABLE rel_Artist_Performed_Song (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            PRIMARY KEY (kb_artist_id, kb_song_id)
        );''',
        '''CREATE TABLE rel_Artist_Member_Of_Artist (
            kb_group_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_member_artist_id UUID REFERENCES kb_Artist(kb_id),
            start_date DATE NULL,
            end_date DATE NULL,
            PRIMARY KEY (kb_group_artist_id, kb_member_artist_id)
        );''',
        '''CREATE TABLE rel_Song_Based_On_Work (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kb_work_id UUID REFERENCES kb_Work(kb_id),
            PRIMARY KEY (kb_song_id, kb_work_id)
        );''',
        '''CREATE TABLE rel_Song_Appears_On_Release (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kb_release_id UUID REFERENCES kb_Release(kb_id),
            track_number INTEGER NULL,
            PRIMARY KEY (kb_song_id, kb_release_id)
        );''',
        '''CREATE TABLE rel_Release_By_Label (
            kb_release_id UUID REFERENCES kb_Release(kb_id),
            kb_label_id UUID REFERENCES kb_RecordLabel(kb_id),
            PRIMARY KEY (kb_release_id, kb_label_id)
        );''',
        '''CREATE TABLE rel_Artist_Performed_At_Event (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_event_id UUID REFERENCES kb_Event(kb_id),
            PRIMARY KEY (kb_artist_id, kb_event_id)
        );''',
        '''CREATE TABLE rel_Artist_Plays_Instrument (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_instrument_id UUID REFERENCES kb_Instrument(kb_id),
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            PRIMARY KEY (kb_artist_id, kb_instrument_id, kb_song_id)
        );''',
        '''CREATE TABLE rel_Artist_Has_Genre (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_genre_id UUID REFERENCES kb_Genre(kb_id),
            PRIMARY KEY (kb_artist_id, kb_genre_id)
        );''',
        '''CREATE TABLE rel_Song_Has_Genre (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kb_genre_id UUID REFERENCES kb_Genre(kb_id),
            PRIMARY KEY (kb_song_id, kb_genre_id)
        );''',
        '''CREATE TABLE rel_Album_Has_Genre (
            kb_album_id UUID REFERENCES kb_Album(kb_id),
            kb_genre_id UUID REFERENCES kb_Genre(kb_id),
            PRIMARY KEY (kb_album_id, kb_genre_id)
        );''',
        '''CREATE TABLE rel_Artist_Originates_From_Location (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_location_id UUID REFERENCES kb_Location(kb_id),
            PRIMARY KEY (kb_artist_id, kb_location_id)
        );''',
        '''CREATE TABLE rel_Entity_Has_URL (
            kb_entity_id UUID NOT NULL,
            kb_url_id UUID REFERENCES kb_URL(kb_id),
            kb_entity_type entity_type,
            PRIMARY KEY (kb_entity_id, kb_url_id)
        );''',
        '''CREATE TABLE rel_Artist_Person_Role_Played_Role (
            kb_artist_person_role_id UUID NOT NULL REFERENCES kb_Artist_Person_Role(kb_id),
            kb_target_entity_kb_id UUID NOT NULL,
            target_entity_type entity_type,
            PRIMARY KEY (kb_artist_person_role_id, kb_target_entity_kb_id)
        );''',

        # --- Bridge Tables ---
        '''CREATE TABLE bridge_kb_artist_to_kexp (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kexp_artist_id_internal UUID REFERENCES dim_artists_master(artist_id_internal),
            PRIMARY KEY (kb_artist_id, kexp_artist_id_internal)
        );''',
        '''CREATE TABLE bridge_kb_song_to_kexp (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kexp_track_id_internal UUID REFERENCES dim_tracks(track_id_internal),
            PRIMARY KEY (kb_song_id, kexp_track_id_internal)
        );'''
    ]
    for stmt in table_statements:
        conn.execute(stmt)
    print("  - ✅ KB tables created.")


def main():
    """Main execution function to drop and recreate the KB schema."""
    print("--- KEXP Knowledge Base Schema Setup ---")
    conn: Optional[duckdb.DuckDBPyConnection] = None
    try:
        conn = duckdb.connect(DB_PATH)
        # Drop everything first for a clean slate
        drop_all_kb_objects(conn)

        # Now, create the schema
        create_enum_types(conn)
        create_kb_tables(conn)

        print("\n🎉 Schema creation process completed successfully!")

    except Exception as e:
        print(f"\n❌ An error occurred during schema creation: {e}")
        traceback.print_exc()

    finally:
        if conn:
            conn.close()
            print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
