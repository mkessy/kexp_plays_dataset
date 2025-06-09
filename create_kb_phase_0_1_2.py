import duckdb

# --- ENUM CREATION ---


def create_enum_types(conn: duckdb.DuckDBPyConnection):
    print("\n🏗️  Creating ENUM types...")
    enum_statements = [
        # Entity types
        "CREATE TYPE IF NOT EXISTS artist_type AS ENUM ('PERSON', 'GROUP', 'CHARACTER', 'ORCHESTRA', 'OTHER');",
        "CREATE TYPE IF NOT EXISTS event_type AS ENUM ('SHOW', 'FESTIVAL', 'IN_STUDIO_SESSION', 'OTHER');",
        "CREATE TYPE IF NOT EXISTS link_type AS ENUM ('OFFICIAL_WEBSITE', 'BANDCAMP', 'ARTICLE', 'PERFORMANCE_VIDEO', 'SOCIAL_MEDIA', 'EVENT_PAGE', 'DISCOGS', 'ALLMUSIC', 'LASTFM', 'WIKIDATA', 'STREAMING', 'OTHER');",
        "CREATE TYPE IF NOT EXISTS work_of_art_type AS ENUM ('SONG', 'ALBUM');",
        "CREATE TYPE IF NOT EXISTS entity_type AS ENUM ('ARTIST', 'SONG', 'RELEASE', 'LABEL', 'EVENT', 'GENRE', 'LOCATION', 'PERSON', 'ROLE', 'INSTRUMENT', 'WORK');",
        # Relationship types (for completeness, though not used as columns in specific tables)
        "CREATE TYPE IF NOT EXISTS rel_Artist_Performed_Song AS ENUM ('PERFORMED_SONG');",
        "CREATE TYPE IF NOT EXISTS rel_Song_Featured_Artist AS ENUM ('FEATURED_ARTIST');",
        "CREATE TYPE IF NOT EXISTS rel_Artist_Member_Of_Artist AS ENUM ('HAS_MEMBER');",
        "CREATE TYPE IF NOT EXISTS rel_Song_Appears_On_Release AS ENUM ('APPEARS_ON');",
        "CREATE TYPE IF NOT EXISTS rel_Release_By_Label AS ENUM ('RELEASED_BY_LABEL');",
        "CREATE TYPE IF NOT EXISTS rel_Artist_Performed_At_Event AS ENUM ('PERFORMED_AT');",
        "CREATE TYPE IF NOT EXISTS rel_Has_Genre AS ENUM ('HAS_GENRE');",
        "CREATE TYPE IF NOT EXISTS rel_Artist_Originates_From_Location AS ENUM ('ORIGINATES_FROM_LOCATION');",
        "CREATE TYPE IF NOT EXISTS rel_Entity_Has_URL AS ENUM ('HAS_URL');",
        "CREATE TYPE IF NOT EXISTS rel_Artist_Person_Role_Played_Role AS ENUM ('PLAYED_ROLE');",
        "CREATE TYPE IF NOT EXISTS rel_Artist_Plays_Instrument AS ENUM ('PLAYS_INSTRUMENT');",
        "CREATE TYPE IF NOT EXISTS rel_Song_Based_On_Work AS ENUM ('BASED_ON_WORK');",
    ]
    for stmt in enum_statements:
        conn.execute(stmt)
    print("✅ ENUM types created.")

# --- TABLE CREATION ---


def create_kb_tables(conn: duckdb.DuckDBPyConnection):
    print("\n🏗️  Creating KB tables...")
    table_statements = [
        # 9. kb_Location
        '''CREATE TABLE IF NOT EXISTS kb_Location (
            kb_id UUID PRIMARY KEY,
            city TEXT NULL,
            state_or_region TEXT NULL,
            country TEXT NULL,
            latitude DECIMAL(9,6) NULL,
            longitude DECIMAL(9,6) NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (city, state_or_region, country)
        );''',

        # 2. kb_Person
        '''CREATE TABLE IF NOT EXISTS kb_Person (
            kb_id UUID PRIMARY KEY,
            legal_name TEXT NULL,
            common_name TEXT NOT NULL,
            mb_person_id UUID NULL,
            gender VARCHAR(50) NULL,
            nationality VARCHAR(50) NULL,
            disambiguation TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (mb_person_id)
        );''',
        # 1. kb_Artist
        '''CREATE TABLE IF NOT EXISTS kb_Artist (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            mb_artist_id UUID NULL,
            country_id UUID NULL REFERENCES kb_Location(kb_id),
            kb_artist_type artist_type,
            kb_person_id UUID NULL REFERENCES kb_Person(kb_id),
            disambiguation TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # kb_Work
        '''CREATE TABLE IF NOT EXISTS kb_Work (
            kb_id UUID PRIMARY KEY,
            title VARCHAR NOT NULL,
            mb_work_id UUID,
            work_type VARCHAR,
            language VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 4. kb_Song
        '''CREATE TABLE IF NOT EXISTS kb_Song (
            kb_id UUID PRIMARY KEY,
            title TEXT NOT NULL,
            type work_of_art_type DEFAULT 'SONG',
            mb_recording_id UUID NULL,
            mb_work_id UUID NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 5. kb_Album
        '''CREATE TABLE IF NOT EXISTS kb_Album (
            kb_id UUID PRIMARY KEY,
            title TEXT NOT NULL,
            type work_of_art_type DEFAULT 'ALBUM',
            mb_release_group_id UUID NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 6. kb_Release
        '''CREATE TABLE IF NOT EXISTS kb_Release (
            kb_id UUID PRIMARY KEY,
            album_id UUID NULL REFERENCES kb_Album(kb_id),
            title TEXT NOT NULL,
            mb_release_id UUID NULL,
            release_date DATE NULL,
            country_id UUID NULL REFERENCES kb_Location(kb_id),
            format VARCHAR(100) NULL,
            barcode TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 7. kb_RecordLabel
        '''CREATE TABLE IF NOT EXISTS kb_RecordLabel (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            mb_label_id UUID NULL,
            country TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 8. kb_Genre
        '''CREATE TABLE IF NOT EXISTS kb_Genre (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            mb_genre_id UUID NULL,
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # kb_Instrument
        '''CREATE TABLE IF NOT EXISTS kb_Instrument (
            kb_id UUID PRIMARY KEY,
            name VARCHAR NOT NULL,
            mb_instrument_id UUID,
            instrument_type VARCHAR, -- 'string', 'percussion', 'wind', etc.
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 10. kb_Venue
        '''CREATE TABLE IF NOT EXISTS kb_Venue (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            location_id UUID NULL REFERENCES kb_Location(kb_id),
            mb_id UUID NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 11. kb_Event
        '''CREATE TABLE IF NOT EXISTS kb_Event (
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
        # 12. kb_Date_Entity
        '''CREATE TABLE IF NOT EXISTS kb_Date_Entity (
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
        # 13. kb_URL
        '''CREATE TABLE IF NOT EXISTS kb_URL (
            kb_id UUID PRIMARY KEY,
            address TEXT NOT NULL UNIQUE,
            kb_link_type link_type,
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 14. kb_Role
        '''CREATE TABLE IF NOT EXISTS kb_Role (
            kb_id UUID PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # 15. kb_Artist_Person_Role
        '''CREATE TABLE IF NOT EXISTS kb_Artist_Person_Role (
            kb_id UUID PRIMARY KEY,
            kb_artist_id UUID NULL REFERENCES kb_Artist(kb_id),
            kb_person_id UUID REFERENCES kb_Person(kb_id),
            kb_role_id UUID REFERENCES kb_Role(kb_id),
            description TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );''',
        # Relationship tables
        '''CREATE TABLE IF NOT EXISTS rel_Artist_Performed_Song (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            PRIMARY KEY (kb_artist_id, kb_song_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Artist_Member_Of_Artist (
            kb_group_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_member_artist_id UUID REFERENCES kb_Artist(kb_id),
            start_date DATE NULL,
            end_date DATE NULL,
            PRIMARY KEY (kb_group_artist_id, kb_member_artist_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Song_Based_On_Work (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kb_work_id UUID REFERENCES kb_Work(kb_id),
            PRIMARY KEY (kb_song_id, kb_work_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Song_Appears_On_Release (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kb_release_id UUID REFERENCES kb_Release(kb_id),
            track_number INTEGER NULL,
            PRIMARY KEY (kb_song_id, kb_release_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Release_By_Label (
            kb_release_id UUID REFERENCES kb_Release(kb_id),
            kb_label_id UUID REFERENCES kb_RecordLabel(kb_id),
            PRIMARY KEY (kb_release_id, kb_label_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Artist_Performed_At_Event (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_event_id UUID REFERENCES kb_Event(kb_id),
            PRIMARY KEY (kb_artist_id, kb_event_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Artist_Plays_Instrument (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_instrument_id UUID REFERENCES kb_Instrument(kb_id),
            kb_recording_id UUID REFERENCES kb_Song(kb_id),
            PRIMARY KEY (kb_artist_id, kb_instrument_id, kb_recording_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Artist_Has_Genre (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_genre_id UUID REFERENCES kb_Genre(kb_id),
            PRIMARY KEY (kb_artist_id, kb_genre_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Song_Has_Genre (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kb_genre_id UUID REFERENCES kb_Genre(kb_id),
            PRIMARY KEY (kb_song_id, kb_genre_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Album_Has_Genre (
            kb_album_id UUID REFERENCES kb_Album(kb_id),
            kb_genre_id UUID REFERENCES kb_Genre(kb_id),
            PRIMARY KEY (kb_album_id, kb_genre_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Artist_Originates_From_Location (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kb_location_id UUID REFERENCES kb_Location(kb_id),
            PRIMARY KEY (kb_artist_id, kb_location_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Entity_Has_URL (
            kb_entity_id UUID NOT NULL,
            kb_url_id UUID REFERENCES kb_URL(kb_id),
            kb_entity_type entity_type,
            PRIMARY KEY (kb_entity_id, kb_url_id)
        );''',
        '''CREATE TABLE IF NOT EXISTS rel_Artist_Person_Role_Played_Role (
            kb_artist_person_role_id UUID NOT NULL REFERENCES kb_Artist_Person_Role(kb_id),
            kb_target_entity_kb_id UUID NOT NULL,
            target_entity_type entity_type,
            PRIMARY KEY (kb_artist_person_role_id, kb_target_entity_kb_id)
        );''',
        # Bridge tables
        '''CREATE TABLE IF NOT EXISTS bridge_kb_artist_to_kexp (
            kb_artist_id UUID REFERENCES kb_Artist(kb_id),
            kexp_artist_id_internal UUID REFERENCES dim_artists_master(artist_id_internal),
            PRIMARY KEY (kb_artist_id, kexp_artist_id_internal)
        );''',
        '''CREATE TABLE IF NOT EXISTS bridge_kb_song_to_kexp (
            kb_song_id UUID REFERENCES kb_Song(kb_id),
            kexp_track_id_internal UUID REFERENCES dim_tracks(track_id_internal),
            PRIMARY KEY (kb_song_id, kexp_track_id_internal)
        );''',
    ]
    for stmt in table_statements:
        conn.execute(stmt)
    print("✅ KB tables created.")


def ingest_worldcities_to_kb_location(conn: duckdb.DuckDBPyConnection, csv_path: str):
    print(f"\n🌍 Ingesting world cities from {csv_path} into kb_Location...")
    # Read CSV into DuckDB temp table
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE tmp_worldcities AS
        SELECT * FROM read_csv_auto('{csv_path}', HEADER=TRUE)
    """)
    # Insert into kb_Location, generating UUIDs, upsert on unique constraint
    insert_sql = """
        INSERT INTO kb_Location (kb_id, city, state_or_region, country, latitude, longitude, created_at, updated_at)
        SELECT
            uuid(),
            city,
            admin_name AS state_or_region,
            country,
            CAST(lat AS DOUBLE),
            CAST(lng AS DOUBLE),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM tmp_worldcities
        ON CONFLICT (city, state_or_region, country) DO NOTHING
    """
    conn.execute(insert_sql)
    # Count how many were inserted
    row: tuple[int, ...] | None = conn.execute(
        "SELECT COUNT(*) FROM kb_Location").fetchone()
    count: int = row[0] if row else 0
    print(f"✅ kb_Location now has {count:,} rows.")
    # Drop temp table
    conn.execute("DROP TABLE IF EXISTS tmp_worldcities")

# --- MAIN ---


def main():
    db_path = "kexp_data.db"  # Default path, adjust as needed
    print(f"Connecting to DuckDB at {db_path} ...")
    conn = duckdb.connect(db_path)  # type: ignore
    try:
        create_enum_types(conn)
        create_kb_tables(conn)
        ingest_worldcities_to_kb_location(
            conn, "data/kb_dumps/worldcities.csv")
    finally:
        conn.close()
        print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
