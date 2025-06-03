#!/usr/bin/env python3
"""
Core Analysis Views Creation Script

This script creates essential aggregation views and metrics directly within the DuckDB database
for analyzing normalized KEXP data. It implements the views specified in docs/core_analysis.md
adapted to match the actual database schema.
"""

import duckdb
import sys
from pathlib import Path


def connect_to_database(db_path: str = "kexp_data.db") -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB database."""
    try:
        conn = duckdb.connect(db_path)
        print(f"✅ Connected to database: {db_path}")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)


def create_foundational_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create foundational views for enhanced querying."""

    print("\n🏗️  Creating foundational views...")

    # View: Enriched Play Details
    print("Creating view_play_details...")
    conn.execute("""
        CREATE OR REPLACE VIEW view_play_details AS
        SELECT
            fp.play_id,
            fp.airdate_iso,
            fp.comment,
            fp.play_type,
            fp.rotation_status,
            fp.is_local,
            fp.is_request,
            fp.is_live,
            fp.original_artist_text AS play_artist_text,
            fp.original_album_text AS play_album_text,
            fp.original_song_text AS play_song_text,
            -- Track Dimension
            dt.track_id_internal,
            dt.primary_song_title_observed AS track_song_title,
            dt.mb_track_id AS musicbrainz_track_id,
            dt.mb_recording_id AS musicbrainz_recording_id,
            -- Release Dimension (via track)
            drm.release_id_internal AS track_release_id_internal,
            drm.primary_album_name_observed AS track_album_name,
            drm.mb_release_id AS track_mb_release_id,
            drm.mb_release_group_id AS track_mb_release_group_id,
            drm.release_date_iso AS track_release_date,
            -- Show Dimension
            ds.show_id,
            ds.start_time_iso AS show_start_time,
            ds.tagline_at_show_time AS show_tagline,
            ds.program_id AS show_program_id,
            dp.primary_name AS show_program_name
        FROM
            fact_plays fp
        LEFT JOIN
            dim_tracks dt ON fp.track_id_internal = dt.track_id_internal
        LEFT JOIN
            dim_releases_master drm ON dt.release_id_internal_on_track = drm.release_id_internal
        LEFT JOIN
            dim_shows ds ON fp.show_id = ds.show_id
        LEFT JOIN
            dim_programs dp ON ds.program_id = dp.program_id
    """)

    # View: Artist Play Summary
    print("Creating view_artist_play_summary...")
    conn.execute("""
        CREATE OR REPLACE VIEW view_artist_play_summary AS
        SELECT
            bpa.artist_id_internal,
            dam.primary_name_observed AS artist_primary_name,
            dam.mb_id AS artist_mbid,
            COUNT(DISTINCT bpa.play_id) AS total_plays,
            COUNT(DISTINCT fp.track_id_internal) AS distinct_tracks_played
        FROM
            bridge_play_to_artist bpa
        JOIN
            dim_artists_master dam ON bpa.artist_id_internal = dam.artist_id_internal
        JOIN
            fact_plays fp ON bpa.play_id = fp.play_id
        GROUP BY
            bpa.artist_id_internal, dam.primary_name_observed, dam.mb_id
    """)

    # View: Track Comment Summary
    print("Creating view_track_comment_summary...")
    conn.execute("""
        CREATE OR REPLACE VIEW view_track_comment_summary AS
        SELECT
            dt.track_id_internal,
            dt.primary_song_title_observed AS track_song_title,
            COUNT(fp.play_id) AS total_plays,
            SUM(CASE WHEN fp.comment IS NOT NULL AND fp.comment != '' THEN 1 ELSE 0 END) AS plays_with_comments,
            string_agg(fp.comment, ' ||| ') FILTER (WHERE fp.comment IS NOT NULL AND fp.comment != '') AS all_comments_concatenated
        FROM
            dim_tracks dt
        JOIN
            fact_plays fp ON dt.track_id_internal = fp.track_id_internal
        GROUP BY
            dt.track_id_internal, dt.primary_song_title_observed
    """)

    # View: Show Host Details
    print("Creating view_show_host_details...")
    conn.execute("""
        CREATE OR REPLACE VIEW view_show_host_details AS
        SELECT
            s.show_id,
            s.start_time_iso AS show_start_time,
            s.tagline_at_show_time,
            h.host_id,
            h.primary_name AS host_name
        FROM
            dim_shows s
        JOIN
            bridge_show_hosts bsh ON s.show_id = bsh.show_id
        JOIN
            dim_hosts h ON bsh.host_id = h.host_id
    """)

    print("✅ Foundational views created successfully!")


def run_sample_metrics_queries(conn: duckdb.DuckDBPyConnection) -> None:
    """Run sample metrics queries to demonstrate the views."""

    print("\n📊 Running sample metrics queries...")

    # Overall Data Counts
    print("\n📈 Overall Data Counts:")
    result = conn.execute("""
        SELECT
            (SELECT COUNT(*) FROM fact_plays) AS total_plays,
            (SELECT COUNT(*) FROM dim_tracks) AS total_unique_tracks,
            (SELECT COUNT(*) FROM dim_artists_master) AS total_unique_artists,
            (SELECT COUNT(*) FROM dim_shows) AS total_shows
    """).fetchone()
    print(f"Total plays: {result[0]:,}")
    print(f"Total unique tracks: {result[1]:,}")
    print(f"Total unique artists: {result[2]:,}")
    print(f"Total shows: {result[3]:,}")

    # Play Type Distribution
    print("\n🎵 Play Type Distribution:")
    results = conn.execute("""
        SELECT
            play_type,
            COUNT(*) AS count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM fact_plays
        GROUP BY play_type
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    for row in results:
        print(f"  {row[0] or 'NULL'}: {row[1]:,} ({row[2]}%)")

    # Comment Analysis
    print("\n💬 Comment Analysis:")
    result = conn.execute("""
        SELECT
            SUM(CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END) AS plays_with_comments,
            COUNT(*) AS total_plays,
            ROUND(SUM(CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS percentage_with_comments
        FROM fact_plays
    """).fetchone()
    print(f"Plays with comments: {result[0]:,} / {result[1]:,} ({result[2]}%)")

    # Comment length stats
    result = conn.execute("""
        SELECT
            MIN(length(comment)) AS min_comment_length,
            MAX(length(comment)) AS max_comment_length,
            ROUND(AVG(length(comment)), 2) AS avg_comment_length,
            ROUND(quantile_cont(length(comment), 0.5), 2) AS median_comment_length,
            ROUND(quantile_cont(length(comment), 0.95), 2) AS p95_comment_length
        FROM fact_plays
        WHERE comment IS NOT NULL AND comment != ''
    """).fetchone()
    print(
        f"Comment length - Min: {result[0]}, Max: {result[1]}, Avg: {result[2]}, Median: {result[3]}, 95th %ile: {result[4]}")

    # Top Most Played Tracks
    print("\n🎯 Top 10 Most Played Tracks:")
    results = conn.execute("""
        SELECT
            track_song_title,
            COUNT(play_id) AS play_count
        FROM view_play_details
        WHERE track_song_title IS NOT NULL
        GROUP BY track_song_title, track_id_internal
        ORDER BY play_count DESC
        LIMIT 10
    """).fetchall()

    for i, row in enumerate(results, 1):
        print(f"  {i:2}. {row[0]}: {row[1]:,} plays")

    # Top Most Played Artists
    print("\n🎤 Top 10 Most Played Artists:")
    results = conn.execute("""
        SELECT
            artist_primary_name,
            total_plays
        FROM view_artist_play_summary
        WHERE artist_primary_name IS NOT NULL
        ORDER BY total_plays DESC
        LIMIT 10
    """).fetchall()

    for i, row in enumerate(results, 1):
        print(f"  {i:2}. {row[0]}: {row[1]:,} plays")


def run_data_quality_checks(conn: duckdb.DuckDBPyConnection) -> None:
    """Run data quality and clean up identification queries."""

    print("\n🔍 Data Quality Checks:")

    # Plays with NULL track_id_internal
    result = conn.execute("""
        SELECT COUNT(*) AS plays_with_null_track_id
        FROM fact_plays
        WHERE track_id_internal IS NULL
    """).fetchone()
    print(f"Plays with NULL track_id_internal: {result[0]:,}")

    # Plays with NULL show_id
    result = conn.execute("""
        SELECT COUNT(*) AS plays_with_null_show_id
        FROM fact_plays
        WHERE show_id IS NULL
    """).fetchone()
    print(f"Plays with NULL show_id: {result[0]:,}")

    print("✅ Data quality checks completed!")


def list_created_views(conn: duckdb.DuckDBPyConnection) -> None:
    """List all views that were created."""

    print("\n📋 Created Views:")
    results = conn.execute("""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_type = 'VIEW' AND table_name LIKE 'view_%'
        ORDER BY table_name
    """).fetchall()

    for row in results:
        print(f"  ✓ {row[0]} ({row[1]})")


def main():
    """Main function to create views and run sample queries."""

    print("🎵 KEXP Core Analysis Views Creation")
    print("=" * 40)

    # Connect to database
    conn = connect_to_database()

    try:
        # Create foundational views
        create_foundational_views(conn)

        # List created views
        list_created_views(conn)

        # Run sample metrics queries
        run_sample_metrics_queries(conn)

        # Run data quality checks
        run_data_quality_checks(conn)

        print("\n🎉 Core analysis views created successfully!")
        print("You can now use these views for your analysis:")
        print("  - view_play_details")
        print("  - view_artist_play_summary")
        print("  - view_track_comment_summary")
        print("  - view_show_host_details")

    except Exception as e:
        print(f"❌ Error during execution: {e}")
        sys.exit(1)

    finally:
        conn.close()
        print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
