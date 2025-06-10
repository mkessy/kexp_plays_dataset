#!/usr/bin/env python3
"""
Phase 3 Relationship Processing - Simplified
============================================
Extract MusicBrainz relationships as clean RDF triples without attribute complexity.
"""

import duckdb
import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Set
from dataclasses import dataclass, asdict
import hashlib

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    relation_type: str
    target_type: str
    total_mb_relations: int
    kb_viable_relations: int
    created_triples: int
    processing_time_seconds: float


class RelationshipProcessor:

    def __init__(self, db_path: str = "kexp_data.db", output_dir: str = "phase3_output"):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.conn = duckdb.connect(db_path)
        self.kb_mappings = self._load_kb_mappings()
        self.relationship_types = self._discover_relationship_types()
        self.processing_results = []

        logger.info(
            f"Initialized: {len(self.relationship_types)} relationship types, {sum(len(v) for v in self.kb_mappings.values())} KB entities")

    def _load_kb_mappings(self) -> Dict[str, Dict[str, str]]:
        """Load clean KB entity mappings"""
        mappings = {}

        # Artist mappings
        artists = self.conn.execute("""
            SELECT mb_artist_id::VARCHAR as mb_id, kb_id::VARCHAR as kb_id
            FROM kb_Artist 
            WHERE mb_artist_id IS NOT NULL
        """).fetchdf()
        mappings['artist'] = dict(zip(artists['mb_id'], artists['kb_id']))

        # Recording mappings
        recordings = self.conn.execute("""
            SELECT mb_recording_id::VARCHAR as mb_id, kb_id::VARCHAR as kb_id
            FROM kb_Song 
            WHERE mb_recording_id IS NOT NULL
        """).fetchdf()
        mappings['recording'] = dict(
            zip(recordings['mb_id'], recordings['kb_id']))

        # Release mappings
        releases = self.conn.execute("""
            SELECT mb_release_id::VARCHAR as mb_id, kb_id::VARCHAR as kb_id
            FROM kb_Release 
            WHERE mb_release_id IS NOT NULL
        """).fetchdf()
        mappings['release'] = dict(zip(releases['mb_id'], releases['kb_id']))

        # Album mappings
        albums = self.conn.execute("""
            SELECT mb_release_group_id::VARCHAR as mb_id, kb_id::VARCHAR as kb_id
            FROM kb_Album 
            WHERE mb_release_group_id IS NOT NULL
        """).fetchdf()
        mappings['release_group'] = dict(zip(albums['mb_id'], albums['kb_id']))

        logger.info(
            f"Loaded mappings: {len(mappings['artist'])} artists, {len(mappings['recording'])} recordings, {len(mappings['release'])} releases, {len(mappings['release_group'])} albums")
        return mappings

    def _discover_relationship_types(self) -> list:
        """Discover viable relationship types with KB coverage"""
        query = """
        SELECT relation_type, target_type, COUNT(*) as count
        FROM mb_relations_basic_v2
        WHERE target_entity_id IS NOT NULL
        GROUP BY relation_type, target_type
        HAVING COUNT(*) >= 1000
        ORDER BY COUNT(*) DESC
        """

        discovered = self.conn.execute(query).fetchdf()
        viable_types = []

        for _, row in discovered.iterrows():
            relation_type = row['relation_type']
            target_type = row['target_type']

            # Only process types we have KB mappings for
            if target_type in self.kb_mappings and len(self.kb_mappings[target_type]) > 0:
                viable_types.append((relation_type, target_type))

        return viable_types

    def _extract_relationships(self, relation_type: str, target_type: str) -> pd.DataFrame:
        """Extract relationships for specific type pair"""

        # Base query template
        base_query = """
        SELECT 
            artist_mb_id,
            {target_id_field} as target_mb_id,
            artist_name,
            {target_name_field} as target_name
        FROM mb_relations_basic_v2
        WHERE relation_type = '{relation_type}' 
          AND target_type = '{target_type}'
          AND {target_id_field} IS NOT NULL
        """

        # Configure field names based on target type
        if target_type == 'recording':
            query = base_query.format(
                target_id_field='recording_data.id',
                target_name_field='recording_data.title',
                relation_type=relation_type,
                target_type=target_type
            )
        elif target_type == 'artist':
            query = base_query.format(
                target_id_field='target_entity_id',
                target_name_field='target_entity_name',
                relation_type=relation_type,
                target_type=target_type
            )
        elif target_type == 'release':
            query = base_query.format(
                target_id_field='release_data.id',
                target_name_field='release_data.title',
                relation_type=relation_type,
                target_type=target_type
            )
        elif target_type == 'release_group':
            query = base_query.format(
                target_id_field='release_group_data.id',
                target_name_field='release_group_data.title',
                relation_type=relation_type,
                target_type=target_type
            )
        else:
            return pd.DataFrame()

        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(
                f"Failed to extract {relation_type}->{target_type}: {e}")
            return pd.DataFrame()

    def _validate_and_create_triples(self, relations_df: pd.DataFrame, relation_type: str, target_type: str) -> pd.DataFrame:
        """Validate KB entities exist and create clean triples"""

        if relations_df.empty:
            return pd.DataFrame()

        # Get mappings for validation
        source_mapping = self.kb_mappings['artist']
        target_mapping = self.kb_mappings[target_type]

        # Convert UUIDs to strings and validate both endpoints exist in KB
        relations_df['source_str'] = relations_df['artist_mb_id'].astype(str)
        relations_df['target_str'] = relations_df['target_mb_id'].astype(str)

        valid_mask = (
            relations_df['source_str'].isin(source_mapping.keys()) &
            relations_df['target_str'].isin(target_mapping.keys())
        )

        valid_relations = relations_df[valid_mask].copy()

        if valid_relations.empty:
            return pd.DataFrame()

        # Create triples
        triples = []
        for _, row in valid_relations.iterrows():
            source_kb_id = source_mapping[row['source_str']]
            target_kb_id = target_mapping[row['target_str']]

            # Generate predicate name
            predicate = self._generate_predicate(relation_type, target_type)

            # Create triple ID
            triple_content = f"{source_kb_id}-{predicate}-{target_kb_id}"
            triple_id = hashlib.md5(triple_content.encode()).hexdigest()

            triple = {
                'triple_id': triple_id,
                'subject_type': 'kb_Artist',
                'subject_id': source_kb_id,
                'predicate': predicate,
                'object_type': self._get_kb_entity_type(target_type),
                'object_id': target_kb_id,
                'source_name': str(row['artist_name']) if pd.notna(row['artist_name']) else '',
                'target_name': str(row['target_name']) if pd.notna(row['target_name']) else '',
                'mb_relation_type': relation_type,
                'mb_target_type': target_type,
                'created_at': datetime.now().isoformat()
            }
            triples.append(triple)

        return pd.DataFrame(triples)

    def _generate_predicate(self, relation_type: str, target_type: str) -> str:
        """Generate clean predicate names"""
        clean_relation = relation_type.replace(
            ' ', '_').replace('-', '_').lower()

        if target_type == 'recording':
            if 'instrument' in relation_type:
                return 'plays_instrument_on'
            elif 'vocal' in relation_type:
                return 'provides_vocals_on'
            elif 'producer' in relation_type:
                return 'produces'
            elif 'perform' in relation_type:
                return 'performs_on'
            elif 'conduct' in relation_type:
                return 'conducts'
            else:
                return f'{clean_relation}_recording'
        elif target_type == 'artist':
            if 'member' in relation_type:
                return 'member_of'
            else:
                return f'{clean_relation}_artist'
        else:
            return f'{clean_relation}_{target_type.replace("-", "_")}'

    def _get_kb_entity_type(self, target_type: str) -> str:
        """Map MB types to KB entity types"""
        mapping = {
            'recording': 'kb_Song',
            'artist': 'kb_Artist',
            'release': 'kb_Release',
            'release_group': 'kb_Album'
        }
        return mapping.get(target_type, f'kb_{target_type.replace("-", "_").title()}')

    def process_relationship_type(self, relation_type: str, target_type: str) -> ProcessingResult:
        """Process single relationship type"""
        start_time = datetime.now()

        # Extract relationships
        relations_df = self._extract_relationships(relation_type, target_type)
        total_mb_relations = len(relations_df)

        if total_mb_relations == 0:
            return ProcessingResult(relation_type, target_type, 0, 0, 0, 0.0)

        # Create triples
        triples_df = self._validate_and_create_triples(
            relations_df, relation_type, target_type)
        created_triples = len(triples_df)

        # Save output
        if created_triples > 0:
            # Sanitize filename by replacing problematic characters
            safe_relation = relation_type.replace(
                ' ', '_').replace('/', '_').replace('-', '_')
            safe_target = target_type.replace(
                ' ', '_').replace('/', '_').replace('-', '_')
            filename = f"{safe_relation}_{safe_target}_triples.csv"
            output_path = self.output_dir / filename
            triples_df.to_csv(output_path, index=False)

        processing_time = (datetime.now() - start_time).total_seconds()

        return ProcessingResult(
            relation_type=relation_type,
            target_type=target_type,
            total_mb_relations=total_mb_relations,
            # All extracted relations are potentially viable
            kb_viable_relations=total_mb_relations,
            created_triples=created_triples,
            processing_time_seconds=processing_time
        )

    def consolidate_all_triples(self) -> None:
        """Consolidate all triple files"""
        all_files = list(self.output_dir.glob("*_triples.csv"))

        if not all_files:
            logger.warning("No triple files found to consolidate")
            return

        all_triples = []
        for csv_file in all_files:
            df = pd.read_csv(csv_file)
            all_triples.append(df)

        consolidated_df = pd.concat(all_triples, ignore_index=True)

        # Remove duplicates by triple_id
        initial_count = len(consolidated_df)
        consolidated_df = consolidated_df.drop_duplicates(
            subset=['triple_id'], keep='first')
        final_count = len(consolidated_df)

        # Save consolidated file
        output_file = self.output_dir / "kb_relationships_all.csv"
        consolidated_df.to_csv(output_file, index=False)

        logger.info(
            f"Consolidated {final_count:,} unique triples from {initial_count:,} total")

        # Create database table
        self._create_kb_relationship_table(consolidated_df)

    def _create_kb_relationship_table(self, triples_df: pd.DataFrame) -> None:
        """Create final kb_Relationship table"""
        try:
            self.conn.execute("DROP TABLE IF EXISTS kb_Relationship")

            self.conn.execute("""
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

            self.conn.execute(
                "INSERT INTO kb_Relationship SELECT * FROM triples_df")

            count = self.conn.execute(
                "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
            logger.info(
                f"Created kb_Relationship table with {count:,} relationships")

        except Exception as e:
            logger.error(f"Failed to create kb_Relationship table: {e}")

    def generate_report(self) -> None:
        """Generate processing report"""
        total_mb = sum(r.total_mb_relations for r in self.processing_results)
        total_created = sum(r.created_triples for r in self.processing_results)
        total_time = sum(
            r.processing_time_seconds for r in self.processing_results)

        report = {
            'processing_summary': {
                'total_relationship_types_processed': len(self.processing_results),
                'total_mb_relations_examined': total_mb,
                'total_kb_relationships_created': total_created,
                'overall_conversion_rate': total_created / total_mb if total_mb > 0 else 0,
                'total_processing_time_seconds': total_time
            },
            'relationship_results': [asdict(r) for r in self.processing_results],
            'kb_entity_counts': {k: len(v) for k, v in self.kb_mappings.items()},
            'generated_at': datetime.now().isoformat()
        }

        # Save report
        report_file = self.output_dir / "processing_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        # Print summary
        print(f"\nRelationship Processing Complete")
        print(f"=" * 50)
        print(f"Relationship types processed: {len(self.processing_results)}")
        print(f"Total MB relations examined: {total_mb:,}")
        print(f"KB relationships created: {total_created:,}")
        print(f"Conversion rate: {total_created/total_mb*100:.1f}%")
        print(f"Processing time: {total_time:.1f}s")

        # Top results
        top_results = sorted(self.processing_results,
                             key=lambda x: x.created_triples, reverse=True)[:10]
        print(f"\nTop 10 Relationship Types by Triples Created:")
        for r in top_results:
            print(
                f"  {r.relation_type} -> {r.target_type}: {r.created_triples:,} triples")

    def run_full_processing(self) -> None:
        """Execute complete pipeline"""
        logger.info("Starting relationship processing pipeline")

        for i, (relation_type, target_type) in enumerate(self.relationship_types, 1):
            logger.info(
                f"Processing {i}/{len(self.relationship_types)}: {relation_type} -> {target_type}")

            result = self.process_relationship_type(relation_type, target_type)
            self.processing_results.append(result)

            if result.created_triples > 0:
                logger.info(f"  Created {result.created_triples:,} triples")

        # Consolidate and finalize
        self.consolidate_all_triples()
        self.generate_report()

        self.conn.close()
        logger.info("Processing pipeline completed")


def main():
    processor = RelationshipProcessor()
    processor.run_full_processing()


if __name__ == "__main__":
    main()
