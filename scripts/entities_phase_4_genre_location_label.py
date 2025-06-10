#!/usr/bin/env python3
"""
KEXP Knowledge Base - Phase 4: Genre, Location, and Record Label Relationships
Extract and populate genre, location, and record label relationships following
the pattern established in entities_phase_3_v2_analysis.py
"""

import duckdb
import pandas as pd
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Set, List, Tuple
import os

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
OUTPUT_DIR = Path("output/phase_4_relationships")

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    relationship_type: str
    total_source_relations: int
    created_triples: int
    processing_time_seconds: float


class GenreLocationLabelProcessor:
    def __init__(self):
        self.conn = duckdb.connect(DB_PATH)
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Track processing results
        self.processing_results: List[ProcessingResult] = []

        # Cache KB entity mappings for validation
        self.kb_mappings = {}
        self._load_kb_mappings()

    def _load_kb_mappings(self) -> None:
        """Load KB entity mappings for validation"""
        logger.info("Loading KB entity mappings...")

        # Artists (mb_artist_id -> kb_id)
        artist_df = self.conn.execute("""
            SELECT mb_artist_id, kb_id 
            FROM kb_Artist 
            WHERE mb_artist_id IS NOT NULL
        """).fetchdf()
        self.kb_mappings['artist'] = dict(zip(
            artist_df['mb_artist_id'].astype(str),
            artist_df['kb_id'].astype(str)
        ))

        # Genres (mb_id -> kb_id) - need to check how genres are mapped
        genre_df = self.conn.execute("""
            SELECT kb_id, name
            FROM kb_Genre
        """).fetchdf()
        # Create mapping by name for now - will refine based on stage data
        self.kb_mappings['genre_by_name'] = dict(zip(
            genre_df['name'].str.lower(),
            genre_df['kb_id'].astype(str)
        ))

        # Locations (mb_area_id -> kb_id and by name)
        location_df = self.conn.execute("""
            SELECT kb_id, mb_area_id, name, country_code
            FROM kb_Location
        """).fetchdf()

        # Create mapping by mb_area_id
        self.kb_mappings['location'] = {}
        location_by_mb_id = location_df[location_df['mb_area_id'].notna()]
        if len(location_by_mb_id) > 0:
            self.kb_mappings['location'] = dict(zip(
                location_by_mb_id['mb_area_id'].astype(str),
                location_by_mb_id['kb_id'].astype(str)
            ))

        # Create mapping by name for fallback
        self.kb_mappings['location_by_name'] = dict(zip(
            location_df['name'].str.lower().fillna(''),
            location_df['kb_id'].astype(str)
        ))

        # Record Labels (mb_label_id -> kb_id)
        label_df = self.conn.execute("""
            SELECT mb_label_id, kb_id 
            FROM kb_RecordLabel 
            WHERE mb_label_id IS NOT NULL
        """).fetchdf()
        self.kb_mappings['label'] = dict(zip(
            label_df['mb_label_id'].astype(str),
            label_df['kb_id'].astype(str)
        ))

        # Releases for label relationships
        release_df = self.conn.execute("""
            SELECT mb_release_id, kb_id
            FROM kb_Release
            WHERE mb_release_id IS NOT NULL
        """).fetchdf()
        self.kb_mappings['release'] = dict(zip(
            release_df['mb_release_id'].astype(str),
            release_df['kb_id'].astype(str)
        ))

        logger.info(f"Loaded mappings: {len(self.kb_mappings['artist'])} artists, "
                    f"{len(self.kb_mappings.get('genre_by_name', []))} genres, "
                    f"{len(self.kb_mappings.get('location', []))} locations (by id), "
                    f"{len(self.kb_mappings.get('location_by_name', []))} locations (by name), "
                    f"{len(self.kb_mappings['label'])} labels, "
                    f"{len(self.kb_mappings['release'])} releases")

    def _generate_triple_id(self, subject_id: str, predicate: str, object_id: str) -> str:
        """Generate consistent triple ID"""
        content = f"{subject_id}|{predicate}|{object_id}"
        return hashlib.md5(content.encode()).hexdigest()

    def process_artist_genre_relationships(self) -> ProcessingResult:
        """Extract artist-genre relationships from mb_artists_raw.genres"""
        start_time = datetime.now()
        logger.info("Processing artist-genre relationships...")

        # Extract genre data from mb_artists_raw
        query = """
        SELECT DISTINCT
            a.id as artist_mb_id,
            unnest(a.genres) as genre_data
        FROM mb_artists_raw a
        WHERE a.genres IS NOT NULL 
          AND array_length(a.genres) > 0
        """

        genre_data = self.conn.execute(query).fetchdf()
        total_source = len(genre_data)

        if total_source == 0:
            return ProcessingResult("artist_genre", 0, 0, 0.0)

        # Extract genre info and create relationships
        triples = []
        for _, row in genre_data.iterrows():
            artist_mb_id = str(row['artist_mb_id'])
            genre_info = row['genre_data']

            if artist_mb_id not in self.kb_mappings['artist']:
                continue

            # Extract genre name from the struct
            genre_name = genre_info.get('name', '').lower() if isinstance(
                genre_info, dict) else str(genre_info).lower()

            if genre_name and genre_name in self.kb_mappings['genre_by_name']:
                artist_kb_id = self.kb_mappings['artist'][artist_mb_id]
                genre_kb_id = self.kb_mappings['genre_by_name'][genre_name]

                triple_id = self._generate_triple_id(
                    artist_kb_id, "has_genre", genre_kb_id)

                triples.append({
                    'triple_id': triple_id,
                    'subject_type': 'kb_Artist',
                    'subject_id': artist_kb_id,
                    'predicate': 'has_genre',
                    'object_type': 'kb_Genre',
                    'object_id': genre_kb_id,
                    'source_name': '',  # Would need artist name lookup
                    'target_name': genre_name,
                    'mb_relation_type': 'artist_genre',
                    'mb_target_type': 'genre',
                    'created_at': datetime.now()
                })

        # Save and return results
        triples_df = pd.DataFrame(triples)
        if len(triples_df) > 0:
            output_file = self.output_dir / "artist_genre_triples.csv"
            triples_df.to_csv(output_file, index=False)

        processing_time = (datetime.now() - start_time).total_seconds()
        return ProcessingResult("artist_genre", total_source, len(triples), processing_time)

    def process_artist_location_relationships(self) -> ProcessingResult:
        """Extract artist-location relationships from mb_artists_raw.area and begin-area"""
        start_time = datetime.now()
        logger.info("Processing artist-location relationships...")

        # Extract location data from mb_artists_raw
        query = """
        SELECT DISTINCT
            a.id as artist_mb_id,
            a.area as main_area,
            a."begin-area" as begin_area
        FROM mb_artists_raw a
        WHERE (a.area IS NOT NULL OR a."begin-area" IS NOT NULL)
        """

        location_data = self.conn.execute(query).fetchdf()
        total_source = len(location_data)

        if total_source == 0:
            return ProcessingResult("artist_location", 0, 0, 0.0)

        triples = []
        for _, row in location_data.iterrows():
            artist_mb_id = str(row['artist_mb_id'])

            if artist_mb_id not in self.kb_mappings['artist']:
                continue

            artist_kb_id = self.kb_mappings['artist'][artist_mb_id]

            # Process main area and begin area
            for area_field, relation_type in [('main_area', 'from_location'), ('begin_area', 'born_in_location')]:
                area_data = row[area_field]
                if pd.isna(area_data) or not isinstance(area_data, dict):
                    continue

                area_id = area_data.get('id')
                area_name = area_data.get('name', '')

                location_kb_id = None

                # Try mapping by MB area ID first
                if area_id and str(area_id) in self.kb_mappings['location']:
                    location_kb_id = self.kb_mappings['location'][str(area_id)]
                # Fallback to name mapping
                elif area_name and area_name.lower() in self.kb_mappings['location_by_name']:
                    location_kb_id = self.kb_mappings['location_by_name'][area_name.lower(
                    )]

                if location_kb_id:
                    triple_id = self._generate_triple_id(
                        artist_kb_id, relation_type, location_kb_id)

                    triples.append({
                        'triple_id': triple_id,
                        'subject_type': 'kb_Artist',
                        'subject_id': artist_kb_id,
                        'predicate': relation_type,
                        'object_type': 'kb_Location',
                        'object_id': location_kb_id,
                        'source_name': '',  # Would need artist name lookup
                        'target_name': area_name,
                        'mb_relation_type': 'artist_location',
                        'mb_target_type': 'area',
                        'created_at': datetime.now()
                    })

        # Save and return results
        triples_df = pd.DataFrame(triples)
        if len(triples_df) > 0:
            output_file = self.output_dir / "artist_location_triples.csv"
            triples_df.to_csv(output_file, index=False)

        processing_time = (datetime.now() - start_time).total_seconds()
        return ProcessingResult("artist_location", total_source, len(triples), processing_time)

    def process_release_label_relationships(self) -> ProcessingResult:
        """Extract release-label relationships from plays and label data"""
        start_time = datetime.now()
        logger.info("Processing release-label relationships...")

        # Extract release-label relationships through plays data
        query = """
        SELECT DISTINCT
            r.kb_id as release_kb_id,
            l.mb_id as label_mb_id,
            l.primary_name_observed as label_name
        FROM fact_plays p
        JOIN bridge_play_to_label bpl ON p.play_id = bpl.play_id
        JOIN dim_labels_master l ON bpl.label_id_internal = l.label_id_internal
        JOIN bridge_kb_song_to_kexp bs ON p.track_id_internal = bs.kexp_track_id_internal
        JOIN kb_Song s ON bs.kb_song_id = s.kb_id
        JOIN kb_Release r ON s.mb_recording_id IS NOT NULL  -- Would need proper release linkage
        WHERE l.mb_id IS NOT NULL
        """

        # Simplified version using existing kb_Release and kb_RecordLabel mappings
        simplified_query = """
        SELECT DISTINCT
            rl.kb_id as label_kb_id,
            rl.mb_label_id,
            rl.name as label_name
        FROM kb_RecordLabel rl
        WHERE rl.mb_label_id IS NOT NULL
        LIMIT 1000  -- Start with subset for testing
        """

        label_data = self.conn.execute(simplified_query).fetchdf()
        total_source = len(label_data)

        if total_source == 0:
            return ProcessingResult("release_label", 0, 0, 0.0)

        # For now, create placeholder relationships - would need actual release-label mapping
        # This would require connecting through the play data or finding direct MB relationships
        triples = []

        # TODO: Implement actual release-label relationship extraction once data mapping is clear
        # This requires understanding how releases are connected to labels in the source data

        triples_df = pd.DataFrame(triples)
        if len(triples_df) > 0:
            output_file = self.output_dir / "release_label_triples.csv"
            triples_df.to_csv(output_file, index=False)

        processing_time = (datetime.now() - start_time).total_seconds()
        return ProcessingResult("release_label", total_source, len(triples), processing_time)

    def consolidate_all_triples(self) -> None:
        """Consolidate all triple files into kb_Relationship table"""
        logger.info("Consolidating relationship triples...")

        all_files = list(self.output_dir.glob("*_triples.csv"))
        if not all_files:
            logger.warning("No triple files found to consolidate")
            return

        all_triples = []
        for csv_file in all_files:
            df = pd.read_csv(csv_file)
            all_triples.append(df)

        if not all_triples:
            return

        consolidated_df = pd.concat(all_triples, ignore_index=True)

        # Remove duplicates by triple_id
        initial_count = len(consolidated_df)
        consolidated_df = consolidated_df.drop_duplicates(
            subset=['triple_id'], keep='first')
        final_count = len(consolidated_df)

        # Save consolidated file
        output_file = self.output_dir / "kb_relationships_genre_location_label.csv"
        consolidated_df.to_csv(output_file, index=False)

        logger.info(
            f"Consolidated {final_count:,} unique triples from {initial_count:,} total")

        # Append to existing kb_Relationship table
        self._append_to_kb_relationship_table(consolidated_df)

    def _append_to_kb_relationship_table(self, triples_df: pd.DataFrame) -> None:
        """Append new triples to existing kb_Relationship table"""
        try:
            # Insert new relationships, ignoring duplicates
            self.conn.execute(
                "INSERT OR IGNORE INTO kb_Relationship SELECT * FROM triples_df")

            count = self.conn.execute(
                "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
            logger.info(
                f"kb_Relationship table now contains {count:,} total relationships")

        except Exception as e:
            logger.error(f"Failed to append to kb_Relationship table: {e}")

    def generate_report(self) -> None:
        """Generate processing report"""
        total_source = sum(
            r.total_source_relations for r in self.processing_results)
        total_created = sum(r.created_triples for r in self.processing_results)
        total_time = sum(
            r.processing_time_seconds for r in self.processing_results)

        report = {
            'processing_summary': {
                'relationship_types_processed': len(self.processing_results),
                'total_source_relations_examined': total_source,
                'total_relationships_created': total_created,
                'overall_conversion_rate': total_created / total_source if total_source > 0 else 0,
                'total_processing_time_seconds': total_time
            },
            'relationship_results': [
                {
                    'type': r.relationship_type,
                    'source_relations': r.total_source_relations,
                    'created_triples': r.created_triples,
                    'conversion_rate': r.created_triples / r.total_source_relations if r.total_source_relations > 0 else 0,
                    'processing_time': r.processing_time_seconds
                } for r in self.processing_results
            ],
            'generated_at': datetime.now().isoformat()
        }

        # Save report
        import json
        report_file = self.output_dir / "processing_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        # Print summary
        print(f"\nGenre/Location/Label Relationship Processing Complete")
        print(f"=" * 60)
        print(f"Relationship types processed: {len(self.processing_results)}")
        print(f"Total source relations examined: {total_source:,}")
        print(f"Relationships created: {total_created:,}")
        print(
            f"Conversion rate: {total_created/total_source*100:.1f}%" if total_source > 0 else "N/A")
        print(f"Processing time: {total_time:.1f}s")

        print(f"\nResults by Relationship Type:")
        for r in self.processing_results:
            conversion = f"{r.created_triples/r.total_source_relations*100:.1f}%" if r.total_source_relations > 0 else "N/A"
            print(
                f"  {r.relationship_type}: {r.created_triples:,} created from {r.total_source_relations:,} sources ({conversion})")

    def run_full_processing(self) -> None:
        """Execute complete pipeline"""
        logger.info("Starting genre/location/label relationship processing")

        # Process each relationship type
        result1 = self.process_artist_genre_relationships()
        self.processing_results.append(result1)

        result2 = self.process_artist_location_relationships()
        self.processing_results.append(result2)

        result3 = self.process_release_label_relationships()
        self.processing_results.append(result3)

        # Consolidate and finalize
        self.consolidate_all_triples()
        self.generate_report()

        self.conn.close()
        logger.info("Processing pipeline completed")


def main():
    processor = GenreLocationLabelProcessor()
    processor.run_full_processing()


if __name__ == "__main__":
    main()
