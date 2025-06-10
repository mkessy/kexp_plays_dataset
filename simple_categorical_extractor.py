#!/usr/bin/env python3
"""
Simple Categorical Relationship Extractor
=========================================
Basic extraction of working categorical relationships using actual schema.
"""

import duckdb
import pandas as pd
import logging
import hashlib
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleCategoricalExtractor:
    """Simple extractor that works with the actual schema"""

    def __init__(self, db_path: str = "kexp_data.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)

    def extract_artist_genre_relationships(self) -> pd.DataFrame:
        """Extract artist-genre relationships from mb_artists_raw.genres array"""

        query = """
        WITH artist_genres AS (
            SELECT DISTINCT
                ka.kb_id as subject_id,
                ka.name as subject_name,
                UNNEST(mar.genres) as genre_struct
            FROM kb_Artist ka
            JOIN mb_artists_raw mar ON ka.mb_artist_id = mar.id
            WHERE ka.mb_artist_id IS NOT NULL 
            AND mar.genres IS NOT NULL
            AND len(mar.genres) > 0
        )
        SELECT DISTINCT
            ag.subject_id,
            kg.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Genre' as object_type,
            'has_genre' as predicate,
            ag.subject_name,
            kg.name as object_name,
            'artist_genre' as mb_relation_type,
            'genre' as mb_target_type
        FROM artist_genres ag
        JOIN kb_Genre kg ON LOWER(TRIM(ag.genre_struct.name)) = LOWER(TRIM(kg.name))
        WHERE ag.genre_struct.name IS NOT NULL
        """

        try:
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Artist-Genre relationships: {len(df):,}")
            return self._add_triple_metadata(df)
        except Exception as e:
            logger.error(f"Artist-Genre extraction failed: {e}")
            return pd.DataFrame()

    def extract_artist_location_relationships(self) -> pd.DataFrame:
        """Extract artist-location relationships from mb_artists_raw area data"""

        query = """
        SELECT DISTINCT
            ka.kb_id as subject_id,
            kl.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Location' as object_type,
            'originates_from' as predicate,
            ka.name as subject_name,
            kl.name as object_name,
            'artist_location' as mb_relation_type,
            'location' as mb_target_type
        FROM kb_Artist ka
        JOIN mb_artists_raw mar ON ka.mb_artist_id = mar.id
        JOIN kb_Location kl ON mar.area.id = kl.mb_area_id
        WHERE ka.mb_artist_id IS NOT NULL
        AND mar.area.id IS NOT NULL
        AND kl.mb_area_id IS NOT NULL
        """

        try:
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Artist-Location relationships: {len(df):,}")
            return self._add_triple_metadata(df)
        except Exception as e:
            logger.error(f"Artist-Location extraction failed: {e}")
            return pd.DataFrame()

    def extract_musicbrainz_relationships(self) -> pd.DataFrame:
        """Extract key MusicBrainz relationships that work with current schema"""

        # Focus on artist-artist relationships that are well-supported
        query = """
        SELECT DISTINCT
            ka1.kb_id as subject_id,
            ka2.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Artist' as object_type,
            CASE 
                WHEN mre.relation_type = 'member of band' THEN 'member_of'
                WHEN mre.relation_type = 'collaboration' THEN 'collaborates_with'
                ELSE REPLACE(mre.relation_type, ' ', '_')
            END as predicate,
            ka1.name as subject_name,
            ka2.name as object_name,
            mre.relation_type as mb_relation_type,
            mre.target_type as mb_target_type
        FROM mb_relations_enhanced mre
        JOIN kb_Artist ka1 ON mre.artist_mb_id = ka1.mb_artist_id
        JOIN kb_Artist ka2 ON ka2.mb_artist_id IS NOT NULL  -- Need to find target artist somehow
        WHERE mre.target_type = 'artist'
        AND mre.relation_type IN ('member of band', 'collaboration', 'married', 'sibling')
        LIMIT 0  -- Disable for now since we don't have target_entity_id
        """

        try:
            # Return empty for now since mb_relations_enhanced doesn't have target entity details
            df = pd.DataFrame()
            logger.info(
                f"MusicBrainz relationships: {len(df):,} (skipped - need target entity data)")
            return self._add_triple_metadata(df)
        except Exception as e:
            logger.error(f"MusicBrainz extraction failed: {e}")
            return pd.DataFrame()

    def _add_triple_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add standard triple metadata"""

        if df.empty:
            return df

        # Generate triple IDs
        df['triple_content'] = df['subject_id'] + '-' + \
            df['predicate'] + '-' + df['object_id']
        df['triple_id'] = df['triple_content'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest())
        df['created_at'] = datetime.now().isoformat()

        # Clean up
        df = df.drop(['triple_content'], axis=1)

        return df

    def extract_all_relationships(self) -> pd.DataFrame:
        """Extract all working categorical relationships"""

        logger.info("🎵 Extracting simple categorical relationships...")

        all_relationships = []

        # Artist-Genre relationships
        logger.info("  🎨 Processing artist-genre relationships...")
        artist_genres = self.extract_artist_genre_relationships()
        if not artist_genres.empty:
            all_relationships.append(artist_genres)

        # Artist-Location relationships
        logger.info("  🌍 Processing artist-location relationships...")
        artist_locations = self.extract_artist_location_relationships()
        if not artist_locations.empty:
            all_relationships.append(artist_locations)

        # MusicBrainz relationships (currently disabled)
        logger.info("  🎼 Processing MusicBrainz relationships...")
        mb_relationships = self.extract_musicbrainz_relationships()
        if not mb_relationships.empty:
            all_relationships.append(mb_relationships)

        if not all_relationships:
            logger.warning("No relationships extracted")
            return pd.DataFrame()

        # Combine all relationships
        combined_df = pd.concat(all_relationships, ignore_index=True)

        # Remove duplicates
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['triple_id'])
        final_count = len(combined_df)

        if initial_count > final_count:
            logger.info(
                f"  🧹 Removed {initial_count - final_count:,} duplicate relationships")

        logger.info(f"✅ Total categorical relationships: {final_count:,}")

        return combined_df

    def insert_into_kb_relationship(self, relationships_df: pd.DataFrame) -> int:
        """Insert relationships into kb_Relationship table"""

        if relationships_df.empty:
            logger.warning("No relationships to insert")
            return 0

        logger.info(
            f"💾 Inserting {len(relationships_df):,} relationships into kb_Relationship...")

        # Get current count
        current_count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
        logger.info(f"  Current kb_Relationship count: {current_count:,}")

        inserted_count = 0

        try:
            self.conn.execute("BEGIN TRANSACTION")

            for _, row in relationships_df.iterrows():
                # Use INSERT OR IGNORE to avoid duplicates
                insert_query = """
                INSERT OR IGNORE INTO kb_Relationship 
                (triple_id, subject_type, subject_id, predicate, object_type, object_id,
                 source_name, target_name, mb_relation_type, mb_target_type, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                result = self.conn.execute(insert_query, (
                    row['triple_id'],
                    row['subject_type'],
                    row['subject_id'],
                    row['predicate'],
                    row['object_type'],
                    row['object_id'],
                    row['subject_name'],
                    row['object_name'],
                    row['mb_relation_type'],
                    row['mb_target_type'],
                    row['created_at']
                ))

                if result.rowcount > 0:
                    inserted_count += 1

            self.conn.execute("COMMIT")

            # Get new count
            new_count = self.conn.execute(
                "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]

            logger.info(
                f"  ✅ Successfully inserted {inserted_count:,} new relationships")
            logger.info(
                f"  📊 kb_Relationship total: {new_count:,} (was {current_count:,})")

            return inserted_count

        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Failed to insert relationships: {e}")
            raise

    def run_extraction(self, output_dir: str = "simple_categorical_output") -> dict:
        """Run complete extraction and return summary"""

        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        try:
            # Extract relationships
            relationships_df = self.extract_all_relationships()

            if relationships_df.empty:
                logger.warning("No relationships extracted")
                return {'success': False, 'relationships_extracted': 0}

            # Save to CSV
            output_file = output_path / \
                f"simple_categorical_relationships_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            relationships_df.to_csv(output_file, index=False)
            logger.info(f"📁 Saved relationships to: {output_file}")

            # Insert into database
            inserted_count = self.insert_into_kb_relationship(relationships_df)

            # Generate summary
            summary = {
                'success': True,
                'relationships_extracted': len(relationships_df),
                'relationships_inserted': inserted_count,
                'output_file': str(output_file),
                'breakdown': {
                    'artist_genre': len(relationships_df[relationships_df['mb_relation_type'] == 'artist_genre']),
                    'artist_location': len(relationships_df[relationships_df['mb_relation_type'] == 'artist_location']),
                }
            }

            # Print summary
            print(f"\n🎉 SIMPLE CATEGORICAL EXTRACTION COMPLETE")
            print(f"{'='*60}")
            print(
                f"Total relationships extracted: {summary['relationships_extracted']:,}")
            print(
                f"Successfully inserted: {summary['relationships_inserted']:,}")

            for rel_type, count in summary['breakdown'].items():
                if count > 0:
                    print(f"  {rel_type}: {count:,}")

            print(f"\n📁 Output: {output_file}")

            return summary

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return {'success': False, 'error': str(e)}

        finally:
            self.conn.close()


def main():
    """Main execution"""

    extractor = SimpleCategoricalExtractor()
    summary = extractor.run_extraction()

    if summary['success']:
        print(f"\n✅ Extraction completed successfully!")
        print(f"🎯 Ready to continue with semantic enhancement")
    else:
        print(
            f"\n❌ Extraction failed: {summary.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
