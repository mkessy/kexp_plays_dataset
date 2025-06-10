#!/usr/bin/env python3
"""
Legacy Relationship Analysis
===========================
Analyze existing relationship data before migration to triple format.
"""

import duckdb
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LegacyRelationshipAnalyzer:

    def __init__(self, db_path: str = "kexp_data.db", output_dir: str = "migration_analysis"):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.conn = duckdb.connect(db_path)
        logger.info(f"Connected to database: {db_path}")

        self.analysis_results = {}

    def analyze_legacy_table_structure(self) -> Dict:
        """Analyze structure and content of legacy relationship tables"""
        logger.info("Analyzing legacy relationship table structures...")

        legacy_tables = {
            'rel_Artist_Plays_Instrument': {
                'description': 'Artist plays instrument on song (ternary relationship)',
                'expected_predicates': ['plays_instrument_on'],
                'complexity': 'high'
            },
            'rel_Artist_Member_Of_Artist': {
                'description': 'Artist membership in bands (temporal relationship)',
                'expected_predicates': ['member_of'],
                'complexity': 'medium'
            }
        }

        table_analysis = {}

        for table_name, metadata in legacy_tables.items():
            try:
                # Get row count
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

                # Get table schema
                schema = self.conn.execute(f"DESCRIBE {table_name}").fetchdf()

                # Sample data if table has content
                sample_data = None
                if count > 0:
                    sample_data = self.conn.execute(
                        f"SELECT * FROM {table_name} LIMIT 5").fetchdf()

                table_analysis[table_name] = {
                    'row_count': count,
                    'schema': schema.to_dict('records'),
                    'sample_data': sample_data.to_dict('records') if sample_data is not None else None,
                    'metadata': metadata
                }

                logger.info(f"  {table_name}: {count:,} rows")

            except Exception as e:
                logger.error(f"Failed to analyze {table_name}: {e}")
                table_analysis[table_name] = {'error': str(e)}

        self.analysis_results['legacy_tables'] = table_analysis
        return table_analysis

    def analyze_embedded_relationships(self) -> Dict:
        """Analyze relationships embedded in entity table foreign keys"""
        logger.info("Analyzing embedded relationships in entity tables...")

        embedded_relationships = {
            'artist_person': {
                'source_table': 'kb_Artist',
                'source_column': 'kb_person_id',
                'target_table': 'kb_Person',
                'predicate': 'is_person',
                'description': 'Artist is a specific person'
            },
            'release_album': {
                'source_table': 'kb_Release',
                'source_column': 'album_id',
                'target_table': 'kb_Album',
                'predicate': 'part_of',
                'description': 'Release is part of album'
            }
        }

        embedded_analysis = {}

        for rel_name, config in embedded_relationships.items():
            try:
                # Count non-null relationships
                query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT({config['source_column']}) as non_null_relationships,
                    ROUND(COUNT({config['source_column']}) * 100.0 / COUNT(*), 2) as percentage_with_relationship
                FROM {config['source_table']}
                """

                stats = self.conn.execute(query).fetchone()

                # Sample relationships
                sample_query = f"""
                SELECT 
                    s.kb_id as source_id,
                    s.name as source_name,
                    t.kb_id as target_id,
                    t.name as target_name
                FROM {config['source_table']} s
                JOIN {config['target_table']} t ON s.{config['source_column']} = t.kb_id
                WHERE s.{config['source_column']} IS NOT NULL
                LIMIT 5
                """

                sample_data = self.conn.execute(sample_query).fetchdf()

                embedded_analysis[rel_name] = {
                    'total_records': stats[0],
                    'relationship_count': stats[1],
                    'coverage_percentage': stats[2],
                    'sample_data': sample_data.to_dict('records'),
                    'config': config
                }

                logger.info(
                    f"  {rel_name}: {stats[1]:,} relationships ({stats[2]:.1f}% coverage)")

            except Exception as e:
                logger.error(f"Failed to analyze {rel_name}: {e}")
                embedded_analysis[rel_name] = {'error': str(e)}

        self.analysis_results['embedded_relationships'] = embedded_analysis
        return embedded_analysis

    def check_conflicts_with_existing_triples(self) -> Dict:
        """Check for potential conflicts with existing kb_Relationship data"""
        logger.info(
            "Checking for conflicts with existing relationship triples...")

        conflicts = {}

        # Check for existing member_of relationships
        try:
            existing_member_count = self.conn.execute("""
                SELECT COUNT(*) FROM kb_Relationship 
                WHERE predicate = 'member_of'
            """).fetchone()[0]

            legacy_member_count = self.conn.execute("""
                SELECT COUNT(*) FROM rel_Artist_Member_Of_Artist
            """).fetchone()[0]

            conflicts['member_of'] = {
                'existing_triples': existing_member_count,
                'legacy_relationships': legacy_member_count,
                'potential_duplicates': 'high' if existing_member_count > 0 else 'none'
            }

            logger.info(
                f"  member_of: {existing_member_count:,} existing vs {legacy_member_count:,} legacy")

        except Exception as e:
            conflicts['member_of'] = {'error': str(e)}

        # Check for existing plays_instrument_on relationships
        try:
            existing_instrument_count = self.conn.execute("""
                SELECT COUNT(*) FROM kb_Relationship 
                WHERE predicate = 'plays_instrument_on'
            """).fetchone()[0]

            legacy_instrument_count = self.conn.execute("""
                SELECT COUNT(*) FROM rel_Artist_Plays_Instrument
            """).fetchone()[0]

            conflicts['plays_instrument_on'] = {
                'existing_triples': existing_instrument_count,
                'legacy_relationships': legacy_instrument_count,
                'potential_duplicates': 'high' if existing_instrument_count > 0 else 'none'
            }

            logger.info(
                f"  plays_instrument_on: {existing_instrument_count:,} existing vs {legacy_instrument_count:,} legacy")

        except Exception as e:
            conflicts['plays_instrument_on'] = {'error': str(e)}

        self.analysis_results['conflicts'] = conflicts
        return conflicts

    def validate_referential_integrity(self) -> Dict:
        """Validate referential integrity in legacy tables"""
        logger.info("Validating referential integrity in legacy tables...")

        integrity_checks = {}

        # Check rel_Artist_Plays_Instrument integrity
        try:
            integrity_query = """
            SELECT 
                'rel_Artist_Plays_Instrument' as table_name,
                COUNT(*) as total_relationships,
                COUNT(CASE WHEN a.kb_id IS NULL THEN 1 END) as orphaned_artists,
                COUNT(CASE WHEN i.kb_id IS NULL THEN 1 END) as orphaned_instruments, 
                COUNT(CASE WHEN s.kb_id IS NULL THEN 1 END) as orphaned_songs
            FROM rel_Artist_Plays_Instrument r
            LEFT JOIN kb_Artist a ON r.kb_artist_id = a.kb_id
            LEFT JOIN kb_Instrument i ON r.kb_instrument_id = i.kb_id
            LEFT JOIN kb_Song s ON r.kb_song_id = s.kb_id
            """

            result = self.conn.execute(integrity_query).fetchone()

            integrity_checks['rel_Artist_Plays_Instrument'] = {
                'total_relationships': result[1],
                'orphaned_artists': result[2],
                'orphaned_instruments': result[3],
                'orphaned_songs': result[4],
                'integrity_score': 1.0 - (result[2] + result[3] + result[4]) / (result[1] * 3) if result[1] > 0 else 1.0
            }

        except Exception as e:
            integrity_checks['rel_Artist_Plays_Instrument'] = {'error': str(e)}

        # Check rel_Artist_Member_Of_Artist integrity
        try:
            integrity_query = """
            SELECT 
                'rel_Artist_Member_Of_Artist' as table_name,
                COUNT(*) as total_relationships,
                COUNT(CASE WHEN member.kb_id IS NULL THEN 1 END) as orphaned_members,
                COUNT(CASE WHEN group_artist.kb_id IS NULL THEN 1 END) as orphaned_groups
            FROM rel_Artist_Member_Of_Artist r
            LEFT JOIN kb_Artist member ON r.kb_member_artist_id = member.kb_id
            LEFT JOIN kb_Artist group_artist ON r.kb_group_artist_id = group_artist.kb_id
            """

            result = self.conn.execute(integrity_query).fetchone()

            integrity_checks['rel_Artist_Member_Of_Artist'] = {
                'total_relationships': result[1],
                'orphaned_members': result[2],
                'orphaned_groups': result[3],
                'integrity_score': 1.0 - (result[2] + result[3]) / (result[1] * 2) if result[1] > 0 else 1.0
            }

        except Exception as e:
            integrity_checks['rel_Artist_Member_Of_Artist'] = {'error': str(e)}

        self.analysis_results['integrity'] = integrity_checks
        return integrity_checks

    def analyze_relationship_patterns(self) -> Dict:
        """Analyze patterns in relationship data"""
        logger.info("Analyzing relationship patterns...")

        patterns = {}

        # Analyze instrument usage patterns
        try:
            instrument_patterns = self.conn.execute("""
            SELECT 
                i.name as instrument_name,
                COUNT(*) as usage_count,
                COUNT(DISTINCT r.kb_artist_id) as unique_artists,
                COUNT(DISTINCT r.kb_song_id) as unique_songs
            FROM rel_Artist_Plays_Instrument r
            JOIN kb_Instrument i ON r.kb_instrument_id = i.kb_id
            GROUP BY i.kb_id, i.name
            ORDER BY usage_count DESC
            LIMIT 20
            """).fetchdf()

            patterns['top_instruments'] = instrument_patterns.to_dict(
                'records')

        except Exception as e:
            patterns['top_instruments'] = {'error': str(e)}

        # Analyze band membership patterns
        try:
            membership_patterns = self.conn.execute("""
            SELECT 
                group_artist.name as band_name,
                COUNT(*) as member_count,
                COUNT(CASE WHEN r.start_date IS NOT NULL THEN 1 END) as relationships_with_start_date,
                COUNT(CASE WHEN r.end_date IS NOT NULL THEN 1 END) as relationships_with_end_date
            FROM rel_Artist_Member_Of_Artist r
            JOIN kb_Artist group_artist ON r.kb_group_artist_id = group_artist.kb_id
            GROUP BY group_artist.kb_id, group_artist.name
            ORDER BY member_count DESC
            LIMIT 20
            """).fetchdf()

            patterns['largest_bands'] = membership_patterns.to_dict('records')

        except Exception as e:
            patterns['largest_bands'] = {'error': str(e)}

        self.analysis_results['patterns'] = patterns
        return patterns

    def generate_migration_recommendations(self) -> Dict:
        """Generate recommendations for migration strategy"""
        logger.info("Generating migration recommendations...")

        recommendations = {
            'migration_order': [
                {
                    'phase': 1,
                    'operation': 'Migrate rel_Artist_Member_Of_Artist',
                    'rationale': 'Binary relationship, lower complexity, potential conflicts with existing data',
                    'risk': 'medium'
                },
                {
                    'phase': 2,
                    'operation': 'Migrate embedded artist→person relationships',
                    'rationale': 'High volume, simple binary relationship, no conflicts expected',
                    'risk': 'low'
                },
                {
                    'phase': 3,
                    'operation': 'Migrate embedded release→album relationships',
                    'rationale': 'High volume, simple binary relationship, no conflicts expected',
                    'risk': 'low'
                },
                {
                    'phase': 4,
                    'operation': 'Migrate rel_Artist_Plays_Instrument',
                    'rationale': 'Ternary relationship requiring complex transformation, highest risk',
                    'risk': 'high'
                }
            ],
            'data_backup_strategy': [
                'Full database backup before starting',
                'Table-level backups of relationship tables',
                'Incremental backups between phases'
            ],
            'validation_requirements': [
                'Row count validation for each migration',
                'Referential integrity checks',
                'Duplicate triple detection',
                'Performance impact assessment'
            ]
        }

        # Add specific recommendations based on analysis
        conflicts = self.analysis_results.get('conflicts', {})
        if conflicts.get('member_of', {}).get('existing_triples', 0) > 0:
            recommendations['special_considerations'] = [
                'Existing member_of relationships detected - need deduplication strategy',
                'Consider merging vs replacing existing relationships'
            ]

        self.analysis_results['recommendations'] = recommendations
        return recommendations

    def generate_comprehensive_report(self) -> None:
        """Generate comprehensive analysis report"""
        logger.info("Generating comprehensive migration analysis report...")

        # Ensure all analyses are complete
        if not self.analysis_results:
            self.run_full_analysis()

        # Create summary statistics
        summary = {
            'analysis_timestamp': datetime.now().isoformat(),
            'total_legacy_relationships': sum([
                self.analysis_results.get('legacy_tables', {}).get(
                    table, {}).get('row_count', 0)
                for table in ['rel_Artist_Plays_Instrument', 'rel_Artist_Member_Of_Artist']
            ]),
            'total_embedded_relationships': sum([
                self.analysis_results.get('embedded_relationships', {}).get(
                    rel, {}).get('relationship_count', 0)
                for rel in ['artist_person', 'release_album']
            ]),
            'estimated_new_triples': 0  # Will be calculated
        }

        # Calculate estimated new triples
        summary['estimated_new_triples'] = (
            summary['total_legacy_relationships'] +
            summary['total_embedded_relationships']
        )

        # Add summary to results
        self.analysis_results['summary'] = summary

        # Save comprehensive report
        report_file = self.output_dir / "migration_analysis_report.json"
        with open(report_file, 'w') as f:
            json.dump(self.analysis_results, f, indent=2, default=str)

        # Save human-readable summary
        summary_file = self.output_dir / "migration_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("KEXP Knowledge Base - Legacy Relationship Migration Analysis\n")
            f.write("=" * 60 + "\n\n")

            f.write(f"Analysis Date: {summary['analysis_timestamp']}\n")
            f.write(
                f"Total Legacy Relationships: {summary['total_legacy_relationships']:,}\n")
            f.write(
                f"Total Embedded Relationships: {summary['total_embedded_relationships']:,}\n")
            f.write(
                f"Estimated New Triples: {summary['estimated_new_triples']:,}\n\n")

            f.write("Legacy Tables Analysis:\n")
            f.write("-" * 25 + "\n")
            for table, data in self.analysis_results.get('legacy_tables', {}).items():
                if 'row_count' in data:
                    f.write(f"{table}: {data['row_count']:,} rows\n")
                    f.write(
                        f"  Complexity: {data['metadata']['complexity']}\n")
                    f.write(
                        f"  Expected Predicates: {data['metadata']['expected_predicates']}\n\n")

            f.write("Embedded Relationships:\n")
            f.write("-" * 22 + "\n")
            for rel, data in self.analysis_results.get('embedded_relationships', {}).items():
                if 'relationship_count' in data:
                    f.write(
                        f"{rel}: {data['relationship_count']:,} relationships\n")
                    f.write(f"  Predicate: {data['config']['predicate']}\n")
                    f.write(
                        f"  Coverage: {data['coverage_percentage']:.1f}%\n\n")

        logger.info(f"Analysis report saved to: {report_file}")
        logger.info(f"Summary saved to: {summary_file}")

    def run_full_analysis(self) -> Dict:
        """Run complete analysis pipeline"""
        logger.info("Starting comprehensive legacy relationship analysis...")

        # Run all analyses
        self.analyze_legacy_table_structure()
        self.analyze_embedded_relationships()
        self.check_conflicts_with_existing_triples()
        self.validate_referential_integrity()
        self.analyze_relationship_patterns()
        self.generate_migration_recommendations()

        # Generate comprehensive report
        self.generate_comprehensive_report()

        logger.info("Analysis complete!")
        return self.analysis_results

    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()


def main():
    analyzer = LegacyRelationshipAnalyzer()
    results = analyzer.run_full_analysis()

    # Print key findings
    print("\nKey Findings:")
    print("=" * 40)

    summary = results.get('summary', {})
    print(
        f"Total relationships to migrate: {summary.get('estimated_new_triples', 0):,}")

    legacy_tables = results.get('legacy_tables', {})
    for table, data in legacy_tables.items():
        if 'row_count' in data and data['row_count'] > 0:
            print(f"{table}: {data['row_count']:,} rows")

    embedded = results.get('embedded_relationships', {})
    for rel, data in embedded.items():
        if 'relationship_count' in data:
            print(f"{rel}: {data['relationship_count']:,} relationships")


if __name__ == "__main__":
    main()
