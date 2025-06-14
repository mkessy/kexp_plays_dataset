#!/usr/bin/env python3
"""
KEXP Knowledge Base - Single Entry Point
From download.py output to complete RDF triple knowledge base with enriched MB data
Aligned version - only Genre/Location from MB, everything else as relationships
"""

import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional
import duckdb
import os
from datetime import datetime

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f"kb_build_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)


class KexpKnowledgeBaseBuilder:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.logger = logging.getLogger("kb_builder")

    def build_complete_kb(self, skip_download: bool = False, skip_normalization: bool = False) -> None:
        """Build complete knowledge base from scratch"""

        self.logger.info("🎵 Starting KEXP Knowledge Base Build (RDF-Aligned)")
        self.logger.info("=" * 60)
        start_time = time.time()

        try:
            # Phase 0: Data Acquisition (Optional)
            if not skip_download:
                self._run_phase("📥 Data Download", ["python", "download.py"])
            else:
                self.logger.info("📥 Data Download - SKIPPED")

            # Phase 1: Data Normalization & Ingestion
            if not skip_normalization:
                self._run_phase("🔄 Data Normalization", [
                                "python", "normalize_kexp.py"])
                self._run_phase("📊 Data Ingestion", [
                                "python", "ingest_kexp_data.py"])
            else:
                self.logger.info("🔄 Data Normalization - SKIPPED")
                self.logger.info("📊 Data Ingestion - SKIPPED")

            # Phase 2: Schema Creation (RDF-Only, no Person/Instrument entities)
            self._run_phase("🏗️  Schema Creation", [
                            "python", "create_kb_schema_rdf.py"])

            # Phase 3: Foundation Entity Population (Genre + Location only)
            self._run_phase("🌱 Foundation Entities", [
                            "python", "entities_phase_1_foundation_extraction.py"])

            # Phase 4: Core Entity Population (Artist, Song, Album, Release)
            self._run_phase("🎯 Core Entities", [
                            "python", "scripts/entities_phase_2_core_extraction.py"])

            # Phase 5: MusicBrainz Relationship Extraction (RDF triples)
            self._run_phase("🔗 MusicBrainz Relationships", [
                            "python", "scripts/entities_phase_3_v2_analysis.py"])

            # Phase 6: Genre/Location/Label Relationships (RDF triples)
            self._run_phase("🏷️  Genre/Location/Label",
                            ["python", "scripts/entities_phase_4_genre_location_label.py"])

            # Phase 7: KEXP Broadcast Entities & Relationships (RDF triples)
            self._run_phase("📻 KEXP Broadcast Layer", [
                            "python", "scripts/entities_phase_5_kexp_broadcast.py"])

            # Phase 8: Validation & Reporting
            self._validate_final_kb()

            total_time = time.time() - start_time
            self.logger.info(
                f"🎉 Knowledge Base Build Complete in {total_time:.1f} seconds!")

        except Exception as e:
            self.logger.error(f"❌ Knowledge Base Build Failed: {e}")
            raise

    def _run_phase(self, phase_name: str, command: list[str]) -> None:
        """Execute pipeline phase with error handling and real-time output"""
        self.logger.info(f"Starting {phase_name}...")
        self.logger.info(f"Running: {' '.join(command)}")
        phase_start = time.time()

        try:
            # Use Popen for real-time output streaming
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Combine stderr with stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True,
                cwd=Path(__file__).parent
            )

            # Stream output in real-time
            last_important_line = ""
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    line = output.strip()
                    # Print progress bars and important info directly
                    if any(indicator in line for indicator in ['%|', '✅', '❌', '📊', '🎵', '🔍', 'Processing', 'Created', 'Complete']):
                        print(line)  # Print directly for real-time feedback
                        if any(indicator in line for indicator in ['✅', 'Created', 'Complete', 'entities']):
                            last_important_line = line

            # Wait for process to complete
            return_code = process.poll()
            if return_code != 0:
                process.wait()  # Ensure we get the final return code
                raise subprocess.CalledProcessError(
                    process.returncode, command)

            phase_time = time.time() - phase_start
            self.logger.info(f"✅ {phase_name} completed in {phase_time:.1f}s")
            if last_important_line:
                self.logger.info(f"   {last_important_line}")

        except subprocess.CalledProcessError as e:
            phase_time = time.time() - phase_start
            self.logger.error(
                f"❌ {phase_name} failed after {phase_time:.1f}s with exit code {e.returncode}")
            self.logger.error(f"Command: {' '.join(command)}")
            raise
        except Exception as e:
            phase_time = time.time() - phase_start
            self.logger.error(
                f"❌ {phase_name} failed after {phase_time:.1f}s with error: {e}")
            raise

    def _validate_final_kb(self) -> None:
        """Validate final knowledge base state"""
        self.logger.info("🔍 Validating final Knowledge Base state...")

        conn = duckdb.connect(self.db_path)
        try:
            # Core entity counts (no Person/Instrument)
            core_entity_tables = [
                'kb_Artist', 'kb_Song', 'kb_Album', 'kb_Release',
                'kb_Genre', 'kb_Location', 'kb_RecordLabel'
            ]

            # KEXP entity counts
            kexp_entity_tables = [
                'kb_Host', 'kb_Program', 'kb_Show', 'kb_Play', 'kb_KexpComment'
            ]

            entity_counts = {}
            for table in core_entity_tables + kexp_entity_tables:
                try:
                    count = conn.execute(
                        f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    entity_counts[table] = count
                except:
                    entity_counts[table] = 0

            # Relationship counts
            try:
                relationship_count = conn.execute(
                    "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
                predicate_count = conn.execute(
                    "SELECT COUNT(DISTINCT predicate) FROM kb_Relationship").fetchone()[0]

                # Top predicates by type
                top_predicates = conn.execute("""
                    SELECT predicate, COUNT(*) as count
                    FROM kb_Relationship 
                    GROUP BY predicate 
                    ORDER BY count DESC 
                    LIMIT 10
                """).fetchall()

                # Predicate categories
                mb_predicates = conn.execute("""
                    SELECT COUNT(*) FROM kb_Relationship 
                    WHERE mb_relation_type IS NOT NULL
                """).fetchone()[0]

                kexp_predicates = conn.execute("""
                    SELECT COUNT(*) FROM kb_Relationship 
                    WHERE predicate IN ('comment_about_song', 'comment_about_play', 'play_during_show', 'show_hosted_by')
                """).fetchone()[0]

            except Exception as e:
                self.logger.error(f"Error querying kb_Relationship: {e}")
                relationship_count = 0
                predicate_count = 0
                top_predicates = []
                mb_predicates = 0
                kexp_predicates = 0

            # Bridge table counts
            bridge_counts = {}
            for table in ['bridge_kb_artist_to_kexp', 'bridge_kb_song_to_kexp']:
                try:
                    count = conn.execute(
                        f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    bridge_counts[table] = count
                except:
                    bridge_counts[table] = 0

            # Legacy table check (should be 0)
            legacy_tables = [
                'kb_Person', 'kb_Instrument',  # Should not exist in aligned version
                'rel_Artist_Has_Genre', 'rel_Song_Has_Genre', 'rel_Album_Has_Genre',
                'rel_Artist_Member_Of_Artist', 'rel_Artist_Performed_Song'
            ]
            legacy_found = []
            for table in legacy_tables:
                try:
                    conn.execute(f"SELECT COUNT(*) FROM {table}")
                    legacy_found.append(table)
                except:
                    pass  # Table doesn't exist (good!)

        finally:
            conn.close()

        # Report final state
        print("\n" + "=" * 60)
        print("🎉 KEXP KNOWLEDGE BASE BUILD COMPLETE!")
        print("=" * 60)

        print("\nCORE MUSIC ENTITIES:")
        core_total = 0
        for entity in core_entity_tables:
            count = entity_counts.get(entity, 0)
            print(f"  {entity:<20} {count:>10,}")
            core_total += count
        print(f"  {'CORE TOTAL':<20} {core_total:>10,}")

        print("\nKEXP BROADCAST ENTITIES:")
        kexp_total = 0
        for entity in kexp_entity_tables:
            count = entity_counts.get(entity, 0)
            print(f"  {entity:<20} {count:>10,}")
            kexp_total += count
        print(f"  {'KEXP TOTAL':<20} {kexp_total:>10,}")

        total_entities = core_total + kexp_total
        print(f"  {'GRAND TOTAL':<20} {total_entities:>10,}")

        print(f"\nRDF RELATIONSHIP SUMMARY:")
        print(f"  Total RDF Triples:      {relationship_count:>10,}")
        print(f"  Unique Predicates:      {predicate_count:>10,}")
        print(f"  MusicBrainz Relations:  {mb_predicates:>10,}")
        print(f"  KEXP Relations:         {kexp_predicates:>10,}")

        if top_predicates:
            print(f"\nTOP PREDICATES:")
            for predicate, count in top_predicates[:5]:
                print(f"  {predicate:<25} {count:>10,}")

        print(f"\nBRIDGE TABLE SUMMARY:")
        for bridge, count in bridge_counts.items():
            print(f"  {bridge:<30} {count:>10,}")

        # Check for legacy tables (should be none)
        if legacy_found:
            print(
                f"\n⚠️  Legacy tables still exist: {', '.join(legacy_found)}")
            print("   These indicate incomplete RDF alignment")
        else:
            print("\n✅ No legacy tables found - perfect RDF alignment!")

        # Final validation
        if relationship_count == 0:
            print("❌ No relationships found - build may have failed")
        elif total_entities == 0:
            print("❌ No entities found - build may have failed")
        elif relationship_count < 100000:
            print(
                f"⚠️  Low relationship count: {relationship_count:,} - expected >100k")
        else:
            print("✅ Knowledge Base validation passed")

        # RDF Alignment Score
        alignment_score = 0
        max_score = 5

        if len(legacy_found) == 0:
            alignment_score += 1
        if relationship_count > 100000:
            alignment_score += 1
        if predicate_count > 10:
            alignment_score += 1
        if total_entities > 100000:
            alignment_score += 1
        if bridge_counts.get('bridge_kb_artist_to_kexp', 0) > 50000:
            alignment_score += 1

        print(f"\n📊 RDF Alignment Score: {alignment_score}/{max_score}")

        if alignment_score == max_score:
            print("🏆 Perfect RDF Alignment - Ready for production!")
        elif alignment_score >= 3:
            print("✅ Good RDF Alignment - Minor issues to resolve")
        else:
            print("⚠️  Poor RDF Alignment - Major issues need attention")

        print("=" * 60)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Build complete KEXP Knowledge Base (RDF-Aligned)")
    parser.add_argument("--skip-download",
                        action="store_true", help="Skip data download")
    parser.add_argument("--skip-normalization", action="store_true",
                        help="Skip normalization and ingestion")
    parser.add_argument("--db-path", type=str,
                        default=DB_PATH, help="Database path")

    args = parser.parse_args()

    builder = KexpKnowledgeBaseBuilder(args.db_path)
    builder.build_complete_kb(
        skip_download=args.skip_download,
        skip_normalization=args.skip_normalization
    )


if __name__ == "__main__":
    main()
