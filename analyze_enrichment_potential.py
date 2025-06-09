#!/usr/bin/env python3
"""
Analyze MusicBrainz data to understand enrichment potential
"""

import duckdb
import os
import pandas as pd
from pathlib import Path

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
OUTPUT_DIR = Path("enrichment_reports")
OUTPUT_DIR.mkdir(exist_ok=True)


def generate_enrichment_report():
    """Generate comprehensive enrichment analysis report"""
    conn = duckdb.connect(DB_PATH, read_only=True)

    print("🔍 Analyzing MusicBrainz enrichment potential...")

    # 1. Overall Statistics
    print("\n📊 OVERALL STATISTICS")
    print("=" * 50)

    stats = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM dim_artists_master WHERE mb_id IS NOT NULL) as kexp_artists_with_mb,
            (SELECT COUNT(*) FROM mb_artists_raw) as total_mb_artists,
            (SELECT COUNT(*) FROM mb_artists_raw mb 
             WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)) as mb_artists_in_kexp
    """).fetchone()

    if not stats:
        print("Could not fetch overall statistics. Exiting.")
        return

    print(f"KEXP artists with MB ID: {stats[0]:,}")
    print(f"MB artists matching KEXP: {stats[2]:,}")
    print(f"Coverage: {stats[2]/stats[0]*100:.1f}%")

    # 2. Enhancement Opportunities
    print("\n🎯 ENHANCEMENT OPPORTUNITIES")
    print("-" * 50)

    enhancements = conn.execute("""
        SELECT
            COUNT(CASE WHEN mb.disambiguation IS NOT NULL THEN 1 END) as with_disambiguation,
            COUNT(CASE WHEN mb."life-span".begin IS NOT NULL THEN 1 END) as with_begin_date,
            COUNT(CASE WHEN mb."life-span".end IS NOT NULL THEN 1 END) as with_end_date,
            COUNT(CASE WHEN mb.gender IS NOT NULL THEN 1 END) as with_gender,
            COUNT(CASE WHEN mb.area IS NOT NULL THEN 1 END) as with_location,
            COUNT(CASE WHEN array_length(mb.aliases) > 0 THEN 1 END) as with_aliases,
            COUNT(CASE WHEN array_length(mb.genres) > 0 THEN 1 END) as with_genres,
            COUNT(*) as total
        FROM mb_artists_raw mb
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
    """).fetchone()

    if not enhancements:
        print("Could not fetch enhancement opportunities. Exiting.")
        return

    print(
        f"Artists with disambiguation: {enhancements[0]:,} ({enhancements[0]/enhancements[7]*100:.1f}%)")
    print(
        f"Artists with begin date: {enhancements[1]:,} ({enhancements[1]/enhancements[7]*100:.1f}%)")
    print(
        f"Artists with end date: {enhancements[2]:,} ({enhancements[2]/enhancements[7]*100:.1f}%)")
    print(
        f"Artists with gender: {enhancements[3]:,} ({enhancements[3]/enhancements[7]*100:.1f}%)")
    print(
        f"Artists with location: {enhancements[4]:,} ({enhancements[4]/enhancements[7]*100:.1f}%)")
    print(
        f"Artists with aliases: {enhancements[5]:,} ({enhancements[5]/enhancements[7]*100:.1f}%)")
    print(
        f"Artists with genres: {enhancements[6]:,} ({enhancements[6]/enhancements[7]*100:.1f}%)")

    # 3. Relationship Analysis
    print("\n🔗 RELATIONSHIP ANALYSIS")
    print("-" * 50)

    # First, let's create a flattened view of relations
    print("Creating flattened relations view...")
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW flattened_relations AS
        SELECT 
            mb.id as artist_id,
            mb.name as artist_name,
            relation
        FROM mb_artists_raw mb, UNNEST(mb.relations) AS t(relation)
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
    """)

    # Now analyze relationship types
    relationships_df = conn.execute("""
        SELECT 
            relation.type as relationship_type,
            relation."target-type" as target_type,
            COUNT(*) as count
        FROM flattened_relations
        GROUP BY relation.type, relation."target-type"
        ORDER BY count DESC
        LIMIT 50
    """).fetchdf()

    print("\nTop relationship types:")
    print(relationships_df.head(20).to_string(index=False))

    # Save full report
    relationships_df.to_csv(
        OUTPUT_DIR / "mb_relationships_full.csv", index=False)

    # 4. Role Analysis
    print("\n🎭 ROLE DISTRIBUTION ANALYSIS")
    print("-" * 50)

    roles_df = conn.execute("""
        SELECT 
            CASE 
                WHEN relation.type IN ('vocal', 'lead vocals', 'background vocals', 'choir vocals') THEN 'Vocals'
                WHEN relation.type LIKE '%guitar%' THEN 'Guitar'
                WHEN relation.type LIKE '%bass%' THEN 'Bass'
                WHEN relation.type LIKE '%drum%' OR relation.type LIKE '%percussion%' THEN 'Drums/Percussion'
                WHEN relation.type LIKE '%keyboard%' OR relation.type LIKE '%piano%' OR relation.type LIKE '%organ%' THEN 'Keys'
                WHEN relation.type IN ('producer', 'co-producer') THEN 'Production'
                WHEN relation.type IN ('engineer', 'recording', 'mix', 'mastering') THEN 'Engineering'
                WHEN relation.type IN ('composer', 'writer', 'lyricist', 'arranger') THEN 'Composition'
                WHEN relation.type = 'member of band' THEN 'Band Membership'
                WHEN relation."target-type" = 'url' THEN 'External Links'
                ELSE 'Other'
            END as role_category,
            COUNT(*) as count
        FROM flattened_relations
        GROUP BY role_category
        ORDER BY count DESC
    """).fetchdf()

    print("\nRelationships by category:")
    print(roles_df.to_string(index=False))

    # 5. New Band Members Analysis
    print("\n👥 NEW BAND MEMBERS TO ADD")
    print("-" * 50)

    new_members = conn.execute("""
        SELECT COUNT(DISTINCT relation.artist.id) as new_member_count
        FROM flattened_relations
        WHERE relation.type = 'member of band'
          AND relation."target-type" = 'artist'
          AND relation.artist.id IS NOT NULL
          AND CAST(relation.artist.id AS UUID) NOT IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
    """).fetchone()

    if new_members:
        print(f"Total new band members to add: {new_members[0]:,}")

    # Sample of new members
    sample_members = conn.execute("""
        SELECT DISTINCT
            relation.artist.name as person_name,
            relation.artist.type as person_type,
            artist_name as band_name,
            relation.begin as start_date,
            relation.end as end_date
        FROM flattened_relations
        WHERE relation.type = 'member of band'
          AND relation."target-type" = 'artist'
          AND relation.artist.id IS NOT NULL
          AND CAST(relation.artist.id AS UUID) NOT IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
        LIMIT 20
    """).fetchdf()

    print("\nSample new band members:")
    for _, member in sample_members.iterrows():
        dates = ""
        if pd.notna(member['start_date']):
            dates = f" ({member['start_date']}"
            if pd.notna(member['end_date']):
                dates += f" - {member['end_date']})"
            else:
                dates += " - present)"
        print(
            f"  • {member['person_name']} - member of {member['band_name']}{dates}")

    # 6. Genre Analysis
    print("\n🎵 GENRE ANALYSIS")
    print("-" * 50)

    # Create genre view
    conn.execute("""
        CREATE OR REPLACE TEMP VIEW artist_genres AS
        SELECT 
            mb.id as artist_id,
            mb.name as artist_name,
            g as genre
        FROM mb_artists_raw mb, UNNEST(mb.genres) as t(g)
        WHERE CAST(mb.id AS UUID) IN (SELECT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL)
    """)

    genres_df = conn.execute("""
        SELECT 
            genre.name as genre_name,
            COUNT(DISTINCT artist_id) as artist_count,
            SUM(genre.count) as total_votes
        FROM artist_genres
        GROUP BY genre.name
        ORDER BY artist_count DESC
        LIMIT 30
    """).fetchdf()

    print("\nTop 30 genres in KEXP artists:")
    print(genres_df.to_string(index=False))

    # 7. Sample Enhanced Artists
    print("\n🎨 SAMPLE ARTIST ENHANCEMENTS")
    print("-" * 50)

    samples = conn.execute("""
        SELECT 
            dam.primary_name_observed as kexp_name,
            mb.disambiguation,
            mb."life-span".begin as begin_date,
            mb."life-span".end as end_date,
            mb.area.name as location,
            array_length(mb.aliases) as alias_count,
            array_length(mb.genres) as genre_count
        FROM dim_artists_master dam
        JOIN mb_artists_raw mb ON dam.mb_id = CAST(mb.id AS UUID)
        WHERE dam.mb_id IS NOT NULL
            AND mb.disambiguation IS NOT NULL
        ORDER BY array_length(mb.genres) DESC NULLS LAST
        LIMIT 10
    """).fetchdf()

    print("\nArtists with disambiguation info:")
    for _, artist in samples.iterrows():
        print(f"\n{artist['kexp_name']}:")
        print(f"  Disambiguation: {artist['disambiguation']}")
        if pd.notna(artist['begin_date']):
            print(f"  Active since: {artist['begin_date']}")
        if pd.notna(artist['location']):
            print(f"  Location: {artist['location']}")

    # 8. Producers and Engineers
    print("\n🎛️ PRODUCERS AND ENGINEERS TO ADD")
    print("-" * 50)

    producers = conn.execute("""
        SELECT 
            relation.type as role,
            COUNT(DISTINCT COALESCE(relation."target-credit", relation.artist.name)) as unique_persons
        FROM flattened_relations
        WHERE relation.type IN ('producer', 'engineer', 'mixer', 'mastering', 'recording')
          AND relation."target-type" IN ('recording', 'release')
        GROUP BY relation.type
        ORDER BY unique_persons DESC
    """).fetchdf()

    print("\nProduction/Engineering roles:")
    print(producers.to_string(index=False))

    conn.close()
    print(f"\n📁 Full reports saved to: {OUTPUT_DIR}")
    print("\n✅ Analysis complete!")


if __name__ == "__main__":
    generate_enrichment_report()
