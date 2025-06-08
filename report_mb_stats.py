import duckdb
import os
import traceback

# --- Configuration ---
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
RAW_TABLE_NAME = "mb_artists_raw"


class MusicBrainzAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path, read_only=True)
        print(f"✅ Connected to DuckDB at {self.db_path} in read-only mode.")

    def run_analysis(self):
        """Executes all statistical queries and prints a comprehensive report."""
        try:
            print("\n--- Generating Comprehensive Analysis from Raw MusicBrainz Data ---")

            self.conn.execute(f"CREATE OR REPLACE TEMP TABLE kexp_artist_mbids AS SELECT DISTINCT mb_id FROM dim_artists_master WHERE mb_id IS NOT NULL;")
            
            # Create the flattened relations view once to be reused by other methods
            print("  - Pre-processing relations data for analysis (this may take a moment)...")
            self.conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE kexp_relations_flat AS
                SELECT UNNEST(relations) as r
                FROM {RAW_TABLE_NAME} raw
                WHERE CAST(raw.id AS UUID) IN (SELECT mb_id FROM kexp_artist_mbids)
                  AND json_type(relations) = 'ARRAY' AND array_length(relations) > 0;
            """)
            
            # --- Run Analysis Queries ---
            self._report_artist_stats()
            self._report_work_and_release_stats()
            self._report_enrichment_potential() # New section
            self._report_full_relation_types()

            print("\n🎉 Analysis complete.")

        except Exception as e:
            print(f"❌ An error occurred during analysis: {e}")
            traceback.print_exc()
        finally:
            self.conn.close()
            print("🔐 Database connection closed.")

    def _report_artist_stats(self):
        """Calculates and prints basic stats about the artists."""
        print("\n📊 Artist & Genre Statistics:")
        found_artists = self.conn.execute(f"SELECT COUNT(*) FROM {RAW_TABLE_NAME} raw WHERE CAST(raw.id AS UUID) IN (SELECT mb_id FROM kexp_artist_mbids);").fetchone()[0]
        print(f"  - KEXP artists found in MB dump: {found_artists:,}")
        
    def _report_work_and_release_stats(self):
        """Analyzes the nested recording, release, and release_group data."""
        print("\n🎼 Work, Release, and Recording Statistics (from MB Data):")
        
        rec_stats = self.conn.execute("SELECT COUNT(DISTINCT r.recording.id), ROUND(COUNT(CASE WHEN r.recording.length IS NOT NULL THEN 1 END) * 100.0 / COUNT(DISTINCT r.recording.id), 1) FROM kexp_relations_flat WHERE r.\"target-type\" = 'recording' AND r.recording IS NOT NULL;").fetchone()
        print("  - Recordings (Songs/Tracks):")
        print(f"    - Unique recordings found in relations: {rec_stats[0]:,}")
        print(f"    - Have length metadata: {rec_stats[1]}%")

        rel_stats = self.conn.execute("SELECT COUNT(DISTINCT r.release.id), ROUND(COUNT(CASE WHEN r.release.date IS NOT NULL AND r.release.date != '' THEN 1 END) * 100.0 / COUNT(DISTINCT r.release.id), 1) FROM kexp_relations_flat WHERE r.\"target-type\" = 'release' AND r.release IS NOT NULL;").fetchone()
        print("  - Releases (Albums/EPs/Singles):")
        print(f"    - Unique releases found in relations: {rel_stats[0]:,}")
        print(f"    - Have release date: {rel_stats[1]}%")

    def _report_enrichment_potential(self):
        """Compares MB data against existing dim tables to find enrichment opportunities."""
        print("\n💡 Enrichment Potential Analysis:")

        # --- Track/Recording Coverage ---
        print("  - Tracks/Recordings:")
        total_tracks_with_mbid = self.conn.execute("SELECT COUNT(DISTINCT mb_recording_id) FROM dim_tracks WHERE mb_recording_id IS NOT NULL;").fetchone()[0]
        q = """
            WITH MbRecordings AS (SELECT DISTINCT r.recording.id as id FROM kexp_relations_flat r WHERE r."target-type" = 'recording')
            SELECT COUNT(DISTINCT dt.mb_recording_id) 
            FROM dim_tracks dt JOIN MbRecordings mb ON dt.mb_recording_id = mb.id;
        """
        found_track_relations = self.conn.execute(q).fetchone()[0]
        print(f"    - Your dim_tracks has {total_tracks_with_mbid:,} tracks with a MusicBrainz Recording ID.")
        print(f"    - Of those, {found_track_relations:,} ({found_track_relations/total_tracks_with_mbid:.1%}) were found in the artist relations data.")
        
        # --- Release Coverage & Gap-Filling ---
        print("  - Releases/Albums:")
        total_releases_with_mbid = self.conn.execute("SELECT COUNT(DISTINCT mb_release_id) FROM dim_releases_master WHERE mb_release_id IS NOT NULL;").fetchone()[0]
        print(f"    - Your dim_releases_master has {total_releases_with_mbid:,} releases with a MusicBrainz Release ID.")

        # Create a temp table of release info from MB for efficient joins
        self.conn.execute("""
            CREATE OR REPLACE TEMP TABLE mb_release_info AS
            SELECT DISTINCT
                r.release.id as mb_id,
                r.release.date as mb_date,
                r.release.status as mb_status,
                r.release.barcode as mb_barcode
            FROM kexp_relations_flat r
            WHERE r."target-type" = 'release' AND r.release IS NOT NULL;
        """)

        q_missing_date = """
            SELECT COUNT(*) FROM dim_releases_master dr
            JOIN mb_release_info mb ON dr.mb_release_id = mb.mb_id
            WHERE dr.release_date_iso IS NULL AND mb.mb_date IS NOT NULL;
        """
        fillable_dates = self.conn.execute(q_missing_date).fetchone()[0]
        print(f"    - Releases missing a date that can be enriched: {fillable_dates:,}")
        
    def _report_full_relation_types(self):
        """Provides a complete list of all relationship types and their frequencies."""
        print("\n🔗 Complete Relationship Type Report:")
        q = "SELECT r.type as relation_type, r.\"target-type\" as target_type, COUNT(*) as count FROM kexp_relations_flat GROUP BY ALL ORDER BY count DESC;"
        relation_stats = self.conn.execute(q).fetchall()
        print(f"  {'RELATION TYPE':<30} | {'TARGET TYPE':<15} | {'COUNT':>10}")
        print("  " + "-" * 60)
        for rel_type, target_type, count in relation_stats:
            print(f"  {rel_type:<30} | {str(target_type):<15} | {count:>10,}")


if __name__ == '__main__':
    try:
        analyzer = MusicBrainzAnalyzer(DB_PATH)
        analyzer.run_analysis()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        traceback.print_exc()