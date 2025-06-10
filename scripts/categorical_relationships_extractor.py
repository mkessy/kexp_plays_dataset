#!/usr/bin/env python3
"""
Enhanced Categorical Relationships Extractor
============================================
Specialized extractor for kb_Genre, kb_Location, kb_RecordLabel relationships
that mines MusicBrainz data while staying grounded in KEXP plays.
"""

import duckdb
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Tuple
import hashlib

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CategoricalRelationshipExtractor:
    """Extract comprehensive categorical relationships from MusicBrainz data"""

    def __init__(self, db_path: str = "kexp_data.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)

    def extract_all_categorical_relationships(self) -> Dict[str, pd.DataFrame]:
        """Extract all categorical relationships in one comprehensive pass"""

        logger.info("🎵 Extracting comprehensive categorical relationships...")

        results = {}

        # 1. Artist-Genre relationships (multi-source)
        logger.info("  🎨 Extracting artist-genre relationships...")
        results['artist_genre'] = self._extract_artist_genre_relationships()

        # 2. Song-Genre relationships (derived)
        logger.info("  🎵 Extracting song-genre relationships...")
        results['song_genre'] = self._extract_song_genre_relationships()

        # 3. Album-Genre relationships (derived)
        logger.info("  💿 Extracting album-genre relationships...")
        results['album_genre'] = self._extract_album_genre_relationships()

        # 4. Artist-Location relationships
        logger.info("  🌍 Extracting artist-location relationships...")
        results['artist_location'] = self._extract_artist_location_relationships()

        # 5. Release-Label relationships
        logger.info("  🏷️ Extracting release-label relationships...")
        results['release_label'] = self._extract_release_label_relationships()

        # 6. Artist-Label relationships (derived from releases)
        logger.info("  🎤 Extracting artist-label relationships...")
        results['artist_label'] = self._extract_artist_label_relationships()

        return results

    def _extract_artist_genre_relationships(self) -> pd.DataFrame:
        """Extract artist-genre relationships from multiple MusicBrainz sources"""

        # Method 1: From artist primary genre in mb_artists_raw
        primary_genre_query = """
        SELECT DISTINCT
            ka.kb_id as subject_id,
            kg.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Genre' as object_type,
            'has_primary_genre' as predicate,
            ka.name as subject_name,
            kg.name as object_name,
            'artist_primary_genre' as source_type,
            'primary' as relationship_strength
        FROM kb_Artist ka
        JOIN mb_artists_raw mar ON ka.mb_artist_id = mar.id
        JOIN kb_Genre kg ON LOWER(TRIM(mar.primary_genre)) = LOWER(TRIM(kg.name))
        WHERE ka.mb_artist_id IS NOT NULL 
        AND mar.primary_genre IS NOT NULL
        AND mar.primary_genre != ''
        """

        # Method 2: From MusicBrainz artist-genre tag relationships
        tag_genre_query = """
        SELECT DISTINCT
            ka.kb_id as subject_id,
            kg.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Genre' as object_type,
            'has_genre' as predicate,
            ka.name as subject_name,
            kg.name as object_name,
            'artist_tag_genre' as source_type,
            'secondary' as relationship_strength
        FROM kb_Artist ka
        JOIN mb_relations_enhanced mre ON ka.mb_artist_id = mre.artist_mb_id
        JOIN kb_Genre kg ON LOWER(TRIM(mre.target_entity_name)) = LOWER(TRIM(kg.name))
        WHERE mre.relation_type IN ('genre', 'style', 'tag')
        AND mre.target_type = 'genre'
        AND ka.mb_artist_id IS NOT NULL
        """

        # Method 3: Derived from recordings (songs the artist performs)
        derived_genre_query = """
        SELECT DISTINCT
            ka.kb_id as subject_id,
            kg.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Genre' as object_type,
            'performs_in_genre' as predicate,
            ka.name as subject_name,
            kg.name as object_name,
            'derived_from_recordings' as source_type,
            'derived' as relationship_strength
        FROM kb_Artist ka
        JOIN bridge_kb_artist_to_kexp bka ON ka.kb_id = bka.kb_artist_id
        JOIN fact_plays fp ON bka.kexp_artist_id_internal = fp.track_id_internal
        JOIN dim_tracks dt ON fp.track_id_internal = dt.track_id_internal
        JOIN kb_Song ks ON dt.mb_recording_id = ks.mb_recording_id
        JOIN mb_relations_enhanced mre ON ks.mb_recording_id = mre.recording_data.id
        JOIN kb_Genre kg ON LOWER(TRIM(mre.target_entity_name)) = LOWER(TRIM(kg.name))
        WHERE mre.relation_type IN ('genre', 'style')
        AND mre.target_type = 'genre'
        """

        # Combine all methods
        queries = [
            ("Primary Genre", primary_genre_query),
            ("Tag Genre", tag_genre_query),
            ("Derived Genre", derived_genre_query)
        ]

        all_relationships = []

        for method_name, query in queries:
            try:
                df = self.conn.execute(query).fetchdf()
                logger.info(f"    {method_name}: {len(df):,} relationships")
                all_relationships.append(df)
            except Exception as e:
                logger.warning(f"    {method_name} failed: {e}")
                continue

        if not all_relationships:
            return pd.DataFrame()

        # Combine and deduplicate
        combined_df = pd.concat(all_relationships, ignore_index=True)

        # Prioritize by relationship strength (primary > secondary > derived)
        priority_map = {'primary': 1, 'secondary': 2, 'derived': 3}
        combined_df['priority'] = combined_df['relationship_strength'].map(
            priority_map)

        # Keep highest priority relationship for each artist-genre pair
        deduped_df = combined_df.sort_values('priority').groupby(
            ['subject_id', 'object_id']).first().reset_index()

        return self._add_triple_metadata(deduped_df, 'artist_genre')

    def _extract_song_genre_relationships(self) -> pd.DataFrame:
        """Extract song-genre relationships from recordings and artist genres"""

        # Method 1: Direct recording-genre from MusicBrainz
        direct_query = """
        SELECT DISTINCT
            ks.kb_id as subject_id,
            kg.kb_id as object_id,
            'kb_Song' as subject_type,
            'kb_Genre' as object_type,
            'has_genre' as predicate,
            ks.title as subject_name,
            kg.name as object_name,
            'recording_direct_genre' as source_type
        FROM kb_Song ks
        JOIN mb_relations_enhanced mre ON ks.mb_recording_id = mre.recording_data.id
        JOIN kb_Genre kg ON LOWER(TRIM(mre.target_entity_name)) = LOWER(TRIM(kg.name))
        WHERE mre.relation_type IN ('genre', 'style')
        AND mre.target_type = 'genre'
        AND ks.mb_recording_id IS NOT NULL
        """

        # Method 2: Inherited from artist (for KEXP-played songs)
        inherited_query = """
        SELECT DISTINCT
            ks.kb_id as subject_id,
            kg.kb_id as object_id,
            'kb_Song' as subject_type,
            'kb_Genre' as object_type,
            'inherits_genre_from_artist' as predicate,
            ks.title as subject_name,
            kg.name as object_name,
            'inherited_from_artist' as source_type
        FROM kb_Song ks
        JOIN bridge_kb_song_to_kexp bks ON ks.kb_id = bks.kb_song_id
        JOIN fact_plays fp ON bks.kexp_track_id_internal = fp.track_id_internal
        JOIN dim_tracks dt ON fp.track_id_internal = dt.track_id_internal
        JOIN kb_Artist ka ON dt.mb_artist_id = ka.mb_artist_id
        JOIN kb_Relationship kr ON (
            kr.subject_id = ka.kb_id 
            AND kr.subject_type = 'kb_Artist'
            AND kr.object_type = 'kb_Genre'
            AND kr.predicate IN ('has_genre', 'has_primary_genre')
        )
        JOIN kb_Genre kg ON kr.object_id = kg.kb_id
        WHERE ks.mb_recording_id IS NOT NULL
        """

        # Combine methods
        queries = [
            ("Direct Recording Genre", direct_query),
            ("Inherited from Artist", inherited_query)
        ]

        all_relationships = []

        for method_name, query in queries:
            try:
                df = self.conn.execute(query).fetchdf()
                logger.info(f"    {method_name}: {len(df):,} relationships")
                all_relationships.append(df)
            except Exception as e:
                logger.warning(f"    {method_name} failed: {e}")
                continue

        if not all_relationships:
            return pd.DataFrame()

        combined_df = pd.concat(all_relationships, ignore_index=True)

        # Deduplicate - prefer direct over inherited
        priority_map = {'recording_direct_genre': 1,
                        'inherited_from_artist': 2}
        combined_df['priority'] = combined_df['source_type'].map(priority_map)

        deduped_df = combined_df.sort_values('priority').groupby(
            ['subject_id', 'object_id']).first().reset_index()

        return self._add_triple_metadata(deduped_df, 'song_genre')

    def _extract_album_genre_relationships(self) -> pd.DataFrame:
        """Extract album-genre relationships from constituent songs"""

        album_genre_query = """
        WITH album_genre_counts AS (
            SELECT 
                ka.kb_id as album_id,
                kg.kb_id as genre_id,
                ka.title as album_name,
                kg.name as genre_name,
                COUNT(*) as song_count,
                COUNT(*) * 1.0 / COUNT(*) OVER (PARTITION BY ka.kb_id) as genre_ratio
            FROM kb_Album ka
            JOIN kb_Release kr ON ka.kb_id = kr.album_id
            JOIN kb_Song ks ON ks.kb_id = kr.kb_id  -- Simplified: assumes songs link to releases
            JOIN kb_Relationship krel ON (
                krel.subject_id = ks.kb_id 
                AND krel.subject_type = 'kb_Song'
                AND krel.object_type = 'kb_Genre'
            )
            JOIN kb_Genre kg ON krel.object_id = kg.kb_id
            GROUP BY ka.kb_id, kg.kb_id, ka.title, kg.name
        )
        SELECT DISTINCT
            album_id as subject_id,
            genre_id as object_id,
            'kb_Album' as subject_type,
            'kb_Genre' as object_type,
            CASE 
                WHEN genre_ratio >= 0.5 THEN 'has_primary_genre'
                ELSE 'has_genre'
            END as predicate,
            album_name as subject_name,
            genre_name as object_name,
            'derived_from_songs' as source_type
        FROM album_genre_counts
        WHERE song_count >= 2  -- Album must have multiple songs in this genre
        OR genre_ratio >= 0.3    -- Or genre represents significant portion
        """

        try:
            df = self.conn.execute(album_genre_query).fetchdf()
            logger.info(f"    Album-Genre relationships: {len(df):,}")
            return self._add_triple_metadata(df, 'album_genre')
        except Exception as e:
            logger.warning(f"    Album-Genre extraction failed: {e}")
            return pd.DataFrame()

    def _extract_artist_location_relationships(self) -> pd.DataFrame:
        """Extract comprehensive artist-location relationships"""

        # Primary location from MusicBrainz area data
        location_query = """
        SELECT DISTINCT
            ka.kb_id as subject_id,
            kl.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Location' as object_type,
            'originates_from' as predicate,
            ka.name as subject_name,
            kl.name as object_name,
            'mb_artist_area' as source_type
        FROM kb_Artist ka
        JOIN mb_artists_raw mar ON ka.mb_artist_id = mar.id
        JOIN kb_Location kl ON mar.area_id = kl.mb_area_id
        WHERE ka.mb_artist_id IS NOT NULL
        AND mar.area_id IS NOT NULL
        AND kl.mb_area_id IS NOT NULL
        
        UNION ALL
        
        -- Secondary: from begin area (birth place for persons)
        SELECT DISTINCT
            ka.kb_id as subject_id,
            kl.kb_id as object_id,
            'kb_Artist' as subject_type,
            'kb_Location' as object_type,
            'born_in' as predicate,
            ka.name as subject_name,
            kl.name as object_name,
            'mb_artist_begin_area' as source_type
        FROM kb_Artist ka
        JOIN mb_artists_raw mar ON ka.mb_artist_id = mar.id
        JOIN kb_Location kl ON mar.begin_area_id = kl.mb_area_id
        WHERE ka.mb_artist_id IS NOT NULL
        AND mar.begin_area_id IS NOT NULL
        AND kl.mb_area_id IS NOT NULL
        AND ka.kb_artist_type = 'PERSON'  -- Only for person entities
        """

        try:
            df = self.conn.execute(location_query).fetchdf()
            logger.info(f"    Artist-Location relationships: {len(df):,}")
            return self._add_triple_metadata(df, 'artist_location')
        except Exception as e:
            logger.warning(f"    Artist-Location extraction failed: {e}")
            return pd.DataFrame()

    def _extract_release_label_relationships(self) -> pd.DataFrame:
        """Extract release-label relationships from KEXP releases"""

        label_query = """
        SELECT DISTINCT
            kr.kb_id as subject_id,
            krl.kb_id as object_id,
            'kb_Release' as subject_type,
            'kb_RecordLabel' as object_type,
            'released_by' as predicate,
            kr.title as subject_name,
            krl.name as object_name,
            'kexp_release_data' as source_type
        FROM kb_Release kr
        JOIN dim_releases_master drm ON kr.mb_release_id = drm.mb_release_id
        JOIN kb_RecordLabel krl ON LOWER(TRIM(drm.label_name)) = LOWER(TRIM(krl.name))
        WHERE kr.mb_release_id IS NOT NULL
        AND drm.label_name IS NOT NULL
        AND drm.label_name != ''
        
        UNION ALL
        
        -- From MusicBrainz release-label relationships
        SELECT DISTINCT
            kr.kb_id as subject_id,
            krl.kb_id as object_id,
            'kb_Release' as subject_type,
            'kb_RecordLabel' as object_type,
            'released_by' as predicate,
            kr.title as subject_name,
            krl.name as object_name,
            'mb_release_label' as source_type
        FROM kb_Release kr
        JOIN mb_relations_enhanced mre ON kr.mb_release_id = mre.release_data.id
        JOIN kb_RecordLabel krl ON mre.target_entity_id = krl.mb_label_id
        WHERE mre.relation_type = 'label'
        AND mre.target_type = 'label'
        AND kr.mb_release_id IS NOT NULL
        """

        try:
            df = self.conn.execute(label_query).fetchdf()

            # Deduplicate - prefer KEXP data over MB data
            priority_map = {'kexp_release_data': 1, 'mb_release_label': 2}
            df['priority'] = df['source_type'].map(priority_map)
            deduped_df = df.sort_values('priority').groupby(
                ['subject_id', 'object_id']).first().reset_index()

            logger.info(
                f"    Release-Label relationships: {len(deduped_df):,}")
            return self._add_triple_metadata(deduped_df, 'release_label')
        except Exception as e:
            logger.warning(f"    Release-Label extraction failed: {e}")
            return pd.DataFrame()

    def _extract_artist_label_relationships(self) -> pd.DataFrame:
        """Extract artist-label relationships from releases"""

        artist_label_query = """
        WITH artist_label_counts AS (
            SELECT 
                ka.kb_id as artist_id,
                krl.kb_id as label_id,
                ka.name as artist_name,
                krl.name as label_name,
                COUNT(DISTINCT kr.kb_id) as release_count,
                MIN(kr.release_date) as first_release,
                MAX(kr.release_date) as latest_release
            FROM kb_Artist ka
            JOIN bridge_kb_artist_to_kexp bka ON ka.kb_id = bka.kb_artist_id
            JOIN fact_plays fp ON bka.kexp_artist_id_internal = fp.track_id_internal
            JOIN dim_tracks dt ON fp.track_id_internal = dt.track_id_internal
            JOIN kb_Release kr ON dt.mb_release_id = kr.mb_release_id
            JOIN kb_Relationship krel ON (
                krel.subject_id = kr.kb_id 
                AND krel.subject_type = 'kb_Release'
                AND krel.object_type = 'kb_RecordLabel'
                AND krel.predicate = 'released_by'
            )
            JOIN kb_RecordLabel krl ON krel.object_id = krl.kb_id
            GROUP BY ka.kb_id, krl.kb_id, ka.name, krl.name
        )
        SELECT DISTINCT
            artist_id as subject_id,
            label_id as object_id,
            'kb_Artist' as subject_type,
            'kb_RecordLabel' as object_type,
            CASE 
                WHEN release_count >= 3 THEN 'signed_to'
                ELSE 'released_on'
            END as predicate,
            artist_name as subject_name,
            label_name as object_name,
            'derived_from_releases' as source_type
        FROM artist_label_counts
        WHERE release_count >= 1  -- At least one release
        """

        try:
            df = self.conn.execute(artist_label_query).fetchdf()
            logger.info(f"    Artist-Label relationships: {len(df):,}")
            return self._add_triple_metadata(df, 'artist_label')
        except Exception as e:
            logger.warning(f"    Artist-Label extraction failed: {e}")
            return pd.DataFrame()

    def _add_triple_metadata(self, df: pd.DataFrame, relation_category: str) -> pd.DataFrame:
        """Add standard triple metadata fields"""

        if df.empty:
            return df

        # Generate triple IDs
        df['triple_content'] = df['subject_id'] + '-' + \
            df['predicate'] + '-' + df['object_id']
        df['triple_id'] = df['triple_content'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest())

        # Add metadata
        df['mb_relation_type'] = relation_category
        df['mb_target_type'] = df['object_type'].str.replace(
            'kb_', '').str.lower()
        df['created_at'] = datetime.now().isoformat()

        # Clean up
        df = df.drop(['triple_content'], axis=1)
        if 'priority' in df.columns:
            df = df.drop(['priority'], axis=1)
        if 'source_type' in df.columns:
            df = df.drop(['source_type'], axis=1)

        return df

    def generate_comprehensive_report(self, results: Dict[str, pd.DataFrame]) -> Dict:
        """Generate comprehensive analysis report"""

        report = {
            'extraction_timestamp': datetime.now().isoformat(),
            'relationship_summary': {},
            'entity_coverage': {},
            'data_quality_insights': {}
        }

        # Relationship summary
        total_triples = 0
        for category, df in results.items():
            count = len(df)
            total_triples += count

            report['relationship_summary'][category] = {
                'total_relationships': count,
                'unique_subjects': df['subject_id'].nunique() if not df.empty else 0,
                'unique_objects': df['object_id'].nunique() if not df.empty else 0,
                'predicates_used': df['predicate'].unique().tolist() if not df.empty else []
            }

        report['relationship_summary']['total_categorical_triples'] = total_triples

        # Entity coverage analysis
        entity_coverage_query = """
        SELECT 
            'kb_Artist' as entity_type,
            COUNT(*) as total_entities,
            COUNT(CASE WHEN mb_artist_id IS NOT NULL THEN 1 END) as with_mb_id
        FROM kb_Artist
        UNION ALL
        SELECT 'kb_Song', COUNT(*), COUNT(CASE WHEN mb_recording_id IS NOT NULL THEN 1 END) FROM kb_Song
        UNION ALL
        SELECT 'kb_Album', COUNT(*), COUNT(CASE WHEN mb_release_group_id IS NOT NULL THEN 1 END) FROM kb_Album
        UNION ALL
        SELECT 'kb_Release', COUNT(*), COUNT(CASE WHEN mb_release_id IS NOT NULL THEN 1 END) FROM kb_Release
        """

        coverage_data = self.conn.execute(entity_coverage_query).fetchdf()
        report['entity_coverage'] = coverage_data.to_dict('records')

        return report

    def save_results(self, results: Dict[str, pd.DataFrame], output_dir: str = "categorical_output"):
        """Save all results and generate report"""

        from pathlib import Path
        import json

        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        # Save individual category files
        saved_files = []
        for category, df in results.items():
            if not df.empty:
                filename = output_path / f"{category}_relationships.csv"
                df.to_csv(filename, index=False)
                saved_files.append(str(filename))
                logger.info(
                    f"  💾 Saved {len(df):,} {category} relationships -> {filename}")

        # Generate and save comprehensive report
        report = self.generate_comprehensive_report(results)
        report['saved_files'] = saved_files

        report_file = output_path / \
            f"categorical_extraction_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"  📊 Report saved -> {report_file}")

        # Print summary
        print(f"\n🎉 CATEGORICAL RELATIONSHIP EXTRACTION COMPLETE")
        print(f"{'='*60}")
        total_relationships = sum(len(df) for df in results.values())
        print(f"Total Relationships Extracted: {total_relationships:,}")

        for category, df in results.items():
            if not df.empty:
                print(f"  {category}: {len(df):,} relationships")

        print(f"\n📁 Output saved to: {output_path}")

        return report


def main():
    """Main execution function"""

    extractor = CategoricalRelationshipExtractor()

    try:
        # Extract all categorical relationships
        results = extractor.extract_all_categorical_relationships()

        # Save results and generate report
        report = extractor.save_results(results)

        logger.info("✅ Categorical relationship extraction complete!")

    except Exception as e:
        logger.error(f"Error in categorical extraction: {e}")
        import traceback
        traceback.print_exc()

    finally:
        extractor.conn.close()


if __name__ == "__main__":
    main()
