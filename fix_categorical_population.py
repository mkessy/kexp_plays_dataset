#!/usr/bin/env python3
"""
Fix Categorical Entity Population
Corrects the issues in Phase 1 and adds record label population
"""

import duckdb
import os
import logging
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "kexp_data.db")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CategoricalEntityFixer:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)

    def check_current_state(self):
        """Check current state of KB tables"""
        logger.info("🔍 Checking current KB table state...")

        entities = ['kb_Genre', 'kb_Location', 'kb_RecordLabel']
        staging = ['stage_genre_extraction', 'stage_location_extraction']

        print("\nCurrent Entity Counts:")
        for entity in entities:
            try:
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {entity}").fetchone()[0]
                print(f"  {entity}: {count:,}")
            except Exception as e:
                print(f"  {entity}: ERROR - {e}")

        print("\nStaging Table Counts:")
        for table in staging:
            try:
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {count:,}")
            except Exception as e:
                print(f"  {table}: ERROR - {e}")

        # Check dim_labels_master for record labels
        try:
            label_count = self.conn.execute("""
                SELECT COUNT(DISTINCT mb_id) 
                FROM dim_labels_master 
                WHERE mb_id IS NOT NULL AND mb_id != 'None'
            """).fetchone()[0]
            print(f"  dim_labels_master (unique MB IDs): {label_count:,}")
        except Exception as e:
            print(f"  dim_labels_master: ERROR - {e}")

    def fix_genre_population(self):
        """Fix kb_Genre population from staging"""
        logger.info("🎵 Fixing kb_Genre population...")

        # Clear existing data first (if any)
        self.conn.execute("DELETE FROM kb_Genre")

        # Use corrected INSERT syntax for DuckDB
        insert_sql = """
        INSERT INTO kb_Genre (kb_id, name, mb_genre_id, description, created_at)
        SELECT
            uuid() as kb_id,
            genre_name as name,
            mb_genre_id,
            COALESCE(
                genre_disambiguation, 
                'Genre with ' || total_votes || ' votes from ' || artist_count || ' artists'
            ) as description,
            CURRENT_TIMESTAMP as created_at
        FROM stage_genre_extraction
        WHERE quality_score >= 1
        ON CONFLICT DO NOTHING
        """

        try:
            self.conn.execute(insert_sql)
            count = self.conn.execute(
                "SELECT COUNT(*) FROM kb_Genre").fetchone()[0]
            logger.info(f"✅ Populated kb_Genre with {count:,} genres")

            # Show quality distribution
            quality_dist = self.conn.execute("""
                SELECT 
                    CASE 
                        WHEN sg.quality_score >= 4 THEN 'High (4-5)'
                        WHEN sg.quality_score >= 2 THEN 'Medium (2-3)'
                        ELSE 'Low (1)'
                    END as quality_category,
                    COUNT(*) as count
                FROM kb_Genre kg
                JOIN stage_genre_extraction sg ON kg.mb_genre_id = sg.mb_genre_id
                GROUP BY 1
                ORDER BY 1
            """).fetchall()

            print("    Quality distribution:")
            for category, count in quality_dist:
                print(f"      {category}: {count:,} genres")

        except Exception as e:
            logger.error(f"❌ Failed to populate kb_Genre: {e}")
            raise

    def fix_location_population(self):
        """Fix kb_Location population from staging"""
        logger.info("🌍 Fixing kb_Location population...")

        # Clear existing data first (if any)
        self.conn.execute("DELETE FROM kb_Location")

        # Use corrected INSERT syntax, handling the unique constraint on mb_area_id
        insert_sql = """
        WITH deduplicated_locations AS (
            SELECT 
                mb_area_id,
                location_name,
                location_type,
                country_code,
                artist_count,
                ROW_NUMBER() OVER (
                    PARTITION BY mb_area_id 
                    ORDER BY artist_count DESC, location_type
                ) as rn
            FROM stage_location_extraction
        )
        INSERT INTO kb_Location (kb_id, mb_area_id, name, type, country_code, created_at)
        SELECT
            uuid() as kb_id,
            mb_area_id,
            location_name as name,
            location_type as type,
            country_code,
            CURRENT_TIMESTAMP as created_at
        FROM deduplicated_locations
        WHERE rn = 1  -- Take the location with highest artist count for each area
        AND artist_count >= 1
        """

        try:
            self.conn.execute(insert_sql)
            count = self.conn.execute(
                "SELECT COUNT(*) FROM kb_Location").fetchone()[0]
            logger.info(f"✅ Populated kb_Location with {count:,} locations")

            # Show top countries by artist count
            top_countries = self.conn.execute("""
                SELECT 
                    kl.country_code,
                    COUNT(*) as location_count,
                    SUM(sl.artist_count) as total_artists
                FROM kb_Location kl
                JOIN stage_location_extraction sl ON kl.mb_area_id = sl.mb_area_id
                WHERE kl.country_code IS NOT NULL
                GROUP BY kl.country_code
                ORDER BY total_artists DESC
                LIMIT 10
            """).fetchall()

            print("    Top countries by artist count:")
            for code, loc_count, artist_count in top_countries:
                print(
                    f"      {code}: {loc_count:,} locations, {artist_count:,} artists")

        except Exception as e:
            logger.error(f"❌ Failed to populate kb_Location: {e}")
            raise

    def create_record_label_staging(self):
        """Create staging table for record labels"""
        logger.info("🏷️ Creating record label staging...")

        # Drop and create staging table
        self.conn.execute("DROP TABLE IF EXISTS stage_label_extraction")

        create_staging_sql = """
        CREATE TABLE stage_label_extraction AS
        SELECT 
            dlm.mb_id as mb_label_id,
            dlm.primary_name_observed as label_name,
            COUNT(DISTINCT p.play_id) as play_count,
            COUNT(DISTINCT bpa.artist_id_internal) as artist_count,
            COUNT(DISTINCT dlm.label_id_internal) as label_variants,
            MIN(p.airdate_iso) as first_seen,
            MAX(p.airdate_iso) as last_seen
        FROM dim_labels_master dlm
        JOIN bridge_play_to_label bpl ON dlm.label_id_internal = bpl.label_id_internal
        JOIN fact_plays p ON bpl.play_id = p.play_id
        JOIN bridge_play_to_artist bpa ON p.play_id = bpa.play_id
        WHERE dlm.mb_id IS NOT NULL 
        AND dlm.primary_name_observed IS NOT NULL
        AND dlm.primary_name_observed != ''
        GROUP BY dlm.mb_id, dlm.primary_name_observed
        HAVING play_count >= 5  -- Quality threshold: at least 5 plays
        AND artist_count >= 2   -- At least 2 different artists
        ORDER BY play_count DESC
        """

        try:
            self.conn.execute(create_staging_sql)
            count = self.conn.execute(
                "SELECT COUNT(*) FROM stage_label_extraction").fetchone()[0]
            logger.info(
                f"✅ Created stage_label_extraction with {count:,} labels")

            # Show quality distribution
            quality_stats = self.conn.execute("""
                SELECT 
                    CASE 
                        WHEN play_count >= 1000 THEN 'Major (1000+)'
                        WHEN play_count >= 100 THEN 'Medium (100-999)'  
                        WHEN play_count >= 20 THEN 'Small (20-99)'
                        ELSE 'Indie (5-19)'
                    END as label_size,
                    COUNT(*) as count,
                    SUM(play_count) as total_plays
                FROM stage_label_extraction
                GROUP BY 1
                ORDER BY SUM(play_count) DESC
            """).fetchall()

            print("    Label distribution by play count:")
            for size, count, plays in quality_stats:
                print(f"      {size}: {count:,} labels, {plays:,} total plays")

        except Exception as e:
            logger.error(f"❌ Failed to create label staging: {e}")
            raise

    def populate_record_labels(self):
        """Populate kb_RecordLabel from staging"""
        logger.info("🏷️ Populating kb_RecordLabel...")

        # Clear existing data first (if any)
        self.conn.execute("DELETE FROM kb_RecordLabel")

        insert_sql = """
        INSERT INTO kb_RecordLabel (kb_id, mb_label_id, name, created_at)
        SELECT
            uuid() as kb_id,
            mb_label_id,
            label_name as name,
            CURRENT_TIMESTAMP as created_at
        FROM stage_label_extraction
        ORDER BY play_count DESC
        """

        try:
            self.conn.execute(insert_sql)
            count = self.conn.execute(
                "SELECT COUNT(*) FROM kb_RecordLabel").fetchone()[0]
            logger.info(f"✅ Populated kb_RecordLabel with {count:,} labels")

            # Show top labels by play count
            top_labels = self.conn.execute("""
                SELECT 
                    krl.name,
                    sl.play_count,
                    sl.artist_count
                FROM kb_RecordLabel krl
                JOIN stage_label_extraction sl ON krl.mb_label_id = sl.mb_label_id
                ORDER BY sl.play_count DESC
                LIMIT 10
            """).fetchall()

            print("    Top labels by KEXP play count:")
            for name, plays, artists in top_labels:
                print(f"      {name}: {plays:,} plays, {artists:,} artists")

        except Exception as e:
            logger.error(f"❌ Failed to populate kb_RecordLabel: {e}")
            raise

    def validate_final_state(self):
        """Validate the final state of all categorical entities"""
        logger.info("✅ Validating final categorical entity state...")

        # Check all entity counts
        final_counts = {}
        entities = ['kb_Genre', 'kb_Location', 'kb_RecordLabel']

        for entity in entities:
            count = self.conn.execute(
                f"SELECT COUNT(*) FROM {entity}").fetchone()[0]
            final_counts[entity] = count

        # Validation thresholds
        thresholds = {
            'kb_Genre': 1000,
            'kb_Location': 5000,
            'kb_RecordLabel': 1000
        }

        print(f"\nFinal Entity Validation:")
        print(f"{'='*50}")

        all_passed = True
        for entity, count in final_counts.items():
            threshold = thresholds[entity]
            status = "✅" if count >= threshold else "⚠️"
            print(f"  {status} {entity}: {count:,} (threshold: {threshold:,})")
            if count < threshold:
                all_passed = False

        # Check for MB ID coverage
        mb_coverage = {}
        coverage_queries = {
            'kb_Genre': "SELECT COUNT(*) FROM kb_Genre WHERE mb_genre_id IS NOT NULL",
            'kb_Location': "SELECT COUNT(*) FROM kb_Location WHERE mb_area_id IS NOT NULL",
            'kb_RecordLabel': "SELECT COUNT(*) FROM kb_RecordLabel WHERE mb_label_id IS NOT NULL"
        }

        print(f"\nMusicBrainz ID Coverage:")
        for entity, query in coverage_queries.items():
            with_mb = self.conn.execute(query).fetchone()[0]
            total = final_counts[entity]
            coverage = (with_mb / total * 100) if total > 0 else 0
            print(f"  {entity}: {with_mb:,}/{total:,} ({coverage:.1f}%)")

        return all_passed, final_counts

    def run_complete_fix(self):
        """Run the complete categorical entity population fix"""
        print("🔧 KEXP Knowledge Base - Categorical Entity Population Fix")
        print("="*60)

        try:
            # Check initial state
            self.check_current_state()

            # Fix genre population
            print(
                f"\n{datetime.now().strftime('%H:%M:%S')} - Fixing Genre Population")
            self.fix_genre_population()

            # Fix location population
            print(
                f"\n{datetime.now().strftime('%H:%M:%S')} - Fixing Location Population")
            self.fix_location_population()

            # Create and populate record labels
            print(
                f"\n{datetime.now().strftime('%H:%M:%S')} - Creating Record Label Staging")
            self.create_record_label_staging()

            print(
                f"\n{datetime.now().strftime('%H:%M:%S')} - Populating Record Labels")
            self.populate_record_labels()

            # Final validation
            print(f"\n{datetime.now().strftime('%H:%M:%S')} - Final Validation")
            success, counts = self.validate_final_state()

            if success:
                print(f"\n🎉 Categorical entity population completed successfully!")
                print(f"✅ All validation thresholds met")
                print(f"✅ Ready for Phase 4 relationship extraction")
            else:
                print(f"\n⚠️ Categorical entity population completed with warnings")
                print(f"📋 Some entities below recommended thresholds")
                print(f"✅ Can proceed with Phase 4, but monitor coverage")

            return True

        except Exception as e:
            logger.error(f"❌ Categorical entity population failed: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            if self.conn:
                self.conn.close()


def main():
    """Main execution function"""
    fixer = CategoricalEntityFixer()
    success = fixer.run_complete_fix()

    if success:
        print(f"\n📋 Next Steps:")
        print(f"  1. Run Phase 4: entities_phase_4_genre_location_label.py")
        print(f"  2. Update main pipeline to include categorical phases")
        print(f"  3. Validate relationship creation")
    else:
        print(f"\n❌ Fix failed. Check logs and staging data.")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
