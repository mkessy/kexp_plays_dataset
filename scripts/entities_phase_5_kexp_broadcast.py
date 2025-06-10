#!/usr/bin/env python3
"""
KEXP Knowledge Base - Phase 5: KEXP Broadcast Entities
Create KB entities for plays, shows, programs, comments, and hosts
Following established RDF relationship patterns
"""

import duckdb
import uuid
import hashlib
import logging
from datetime import datetime
from pathlib import Path
import os

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
OUTPUT_DIR = Path("output/phase_5_kexp_entities")

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KexpEntityProcessor:
    def __init__(self):
        self.conn = duckdb.connect(DB_PATH)
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Entity creation stats
        self.entity_stats = {}
        self.relationship_stats = {}

    def generate_triple_id(self, subject_id: str, predicate: str, object_id: str) -> str:
        """Generate consistent triple ID"""
        content = f"{subject_id}|{predicate}|{object_id}"
        return hashlib.md5(content.encode()).hexdigest()

    def create_kb_host(self) -> None:
        """Create kb_Host entities from dim_hosts"""
        logger.info("Creating kb_Host entities...")

        self.conn.execute("DROP TABLE IF EXISTS kb_Host")
        self.conn.execute("""
            CREATE TABLE kb_Host (
                kb_id UUID PRIMARY KEY,
                name VARCHAR NOT NULL,
                kexp_host_id BIGINT UNIQUE,
                host_uri VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert hosts
        self.conn.execute("""
            INSERT INTO kb_Host (kb_id, name, kexp_host_id, host_uri)
            SELECT 
                uuid() as kb_id,
                primary_name as name,
                host_id as kexp_host_id,
                host_uri
            FROM dim_hosts
            WHERE host_id IS NOT NULL
        """)

        count = self.conn.execute("SELECT COUNT(*) FROM kb_Host").fetchone()[0]
        self.entity_stats['kb_Host'] = count
        logger.info(f"Created {count:,} kb_Host entities")

    def create_kb_program(self) -> None:
        """Create kb_Program entities from dim_programs"""
        logger.info("Creating kb_Program entities...")

        self.conn.execute("DROP TABLE IF EXISTS kb_Program")
        self.conn.execute("""
            CREATE TABLE kb_Program (
                kb_id UUID PRIMARY KEY,
                name VARCHAR NOT NULL,
                kexp_program_id BIGINT UNIQUE,
                description TEXT,
                tags VARCHAR,
                program_uri VARCHAR,
                image_uri VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert programs
        self.conn.execute("""
            INSERT INTO kb_Program (kb_id, name, kexp_program_id, description, tags, program_uri, image_uri)
            SELECT 
                uuid() as kb_id,
                primary_name as name,
                program_id as kexp_program_id,
                description,
                tags,
                program_uri,
                image_uri
            FROM dim_programs
            WHERE program_id IS NOT NULL
        """)

        count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Program").fetchone()[0]
        self.entity_stats['kb_Program'] = count
        logger.info(f"Created {count:,} kb_Program entities")

    def create_kb_show(self) -> None:
        """Create kb_Show entities from dim_shows"""
        logger.info("Creating kb_Show entities...")

        self.conn.execute("DROP TABLE IF EXISTS kb_Show")
        self.conn.execute("""
            CREATE TABLE kb_Show (
                kb_id UUID PRIMARY KEY,
                kexp_show_id BIGINT UNIQUE NOT NULL,
                show_uri VARCHAR,
                start_time TIMESTAMP,
                title VARCHAR,
                tagline VARCHAR,
                program_name VARCHAR,
                program_tags VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert shows
        self.conn.execute("""
            INSERT INTO kb_Show (kb_id, kexp_show_id, show_uri, start_time, title, tagline, program_name, program_tags)
            SELECT 
                uuid() as kb_id,
                show_id as kexp_show_id,
                show_uri,
                start_time_iso as start_time,
                CASE 
                    WHEN title_at_show_time IS NOT NULL 
                    THEN CAST(title_at_show_time AS VARCHAR)
                    ELSE NULL 
                END as title,
                tagline_at_show_time as tagline,
                program_name_at_show_time as program_name,
                program_tags_at_show_time as program_tags
            FROM dim_shows
            WHERE show_id IS NOT NULL
        """)

        count = self.conn.execute("SELECT COUNT(*) FROM kb_Show").fetchone()[0]
        self.entity_stats['kb_Show'] = count
        logger.info(f"Created {count:,} kb_Show entities")

    def create_kb_play(self) -> None:
        """Create kb_Play entities from fact_plays"""
        logger.info("Creating kb_Play entities...")

        self.conn.execute("DROP TABLE IF EXISTS kb_Play")
        self.conn.execute("""
            CREATE TABLE kb_Play (
                kb_id UUID PRIMARY KEY,
                play_id BIGINT UNIQUE NOT NULL,
                airdate TIMESTAMP,
                rotation_status VARCHAR,
                is_local BOOLEAN,
                is_request BOOLEAN,
                is_live BOOLEAN,
                play_type VARCHAR,
                has_comment BOOLEAN,
                comment_length INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert plays with deduplication
        self.conn.execute("""
            INSERT INTO kb_Play (kb_id, play_id, airdate, rotation_status, is_local, is_request, is_live, play_type, has_comment, comment_length)
            SELECT 
                uuid() as kb_id,
                play_id,
                airdate_iso as airdate,
                rotation_status,
                is_local,
                is_request,
                is_live,
                play_type,
                CASE WHEN comment IS NOT NULL AND comment != '' THEN TRUE ELSE FALSE END as has_comment,
                CASE WHEN comment IS NOT NULL THEN LENGTH(comment) ELSE 0 END as comment_length
            FROM (
                SELECT DISTINCT ON (play_id) 
                    play_id,
                    airdate_iso,
                    rotation_status,
                    is_local,
                    is_request,
                    is_live,
                    play_type,
                    comment
                FROM fact_plays
                WHERE play_id IS NOT NULL
                ORDER BY play_id, airdate_iso DESC
            ) deduplicated_plays
        """)

        count = self.conn.execute("SELECT COUNT(*) FROM kb_Play").fetchone()[0]
        self.entity_stats['kb_Play'] = count
        logger.info(f"Created {count:,} kb_Play entities")

    def create_kb_kexpcomment(self) -> None:
        """Create kb_KexpComment entities from substantial comments"""
        logger.info("Creating kb_KexpComment entities...")

        self.conn.execute("DROP TABLE IF EXISTS kb_KexpComment")
        self.conn.execute("""
            CREATE TABLE kb_KexpComment (
                kb_id UUID PRIMARY KEY,
                play_id BIGINT NOT NULL,
                comment_text TEXT NOT NULL,
                comment_length INTEGER NOT NULL,
                has_links BOOLEAN,
                contains_url BOOLEAN,
                comment_type VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert substantial comments with deduplication by play_id
        self.conn.execute("""
            INSERT INTO kb_KexpComment (kb_id, play_id, comment_text, comment_length, has_links, contains_url, comment_type)
            SELECT 
                uuid() as kb_id,
                play_id,
                comment as comment_text,
                LENGTH(comment) as comment_length,
                comment ILIKE '%http%' as has_links,
                comment ILIKE '%http%' as contains_url,
                CASE 
                    WHEN comment ILIKE '%http%' THEN 'contains_link'
                    WHEN comment ILIKE '%interview%' OR comment ILIKE '%review%' THEN 'interview_review'
                    WHEN comment ILIKE '%live%' OR comment ILIKE '%studio%' THEN 'performance_info'
                    WHEN comment ILIKE '%album%' OR comment ILIKE '%released%' THEN 'release_info'
                    ELSE 'general_commentary'
                END as comment_type
            FROM (
                SELECT DISTINCT ON (play_id)
                    play_id,
                    comment
                FROM fact_plays
                WHERE comment IS NOT NULL 
                  AND comment != ''
                  AND LENGTH(comment) > 50
                ORDER BY play_id, airdate_iso DESC
            ) deduplicated_comments
        """)

        count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_KexpComment").fetchone()[0]
        self.entity_stats['kb_KexpComment'] = count
        logger.info(f"Created {count:,} kb_KexpComment entities")

    def create_relationships(self) -> None:
        """Create RDF relationships between KEXP entities"""
        logger.info("Creating KEXP entity relationships...")

        relationships = []

        # 1. Play → Show relationships
        logger.info("  Creating play_during_show relationships...")
        try:
            play_show_query = """
            SELECT 
                kp.kb_id as play_kb_id,
                ks.kb_id as show_kb_id,
                kp.play_id,
                ks.kexp_show_id
            FROM kb_Play kp
            JOIN fact_plays fp ON kp.play_id = fp.play_id
            JOIN kb_Show ks ON fp.show_id = ks.kexp_show_id
            """

            play_show_df = self.conn.execute(play_show_query).fetchdf()
            for _, row in play_show_df.iterrows():
                triple_id = self.generate_triple_id(
                    row['play_kb_id'], 'play_during_show', row['show_kb_id'])
                relationships.append({
                    'triple_id': triple_id,
                    'subject_type': 'kb_Play',
                    'subject_id': row['play_kb_id'],
                    'predicate': 'play_during_show',
                    'object_type': 'kb_Show',
                    'object_id': row['show_kb_id'],
                    'source_name': f"Play {row['play_id']}",
                    'target_name': f"Show {row['kexp_show_id']}",
                    'mb_relation_type': 'kexp_broadcast',
                    'mb_target_type': 'show'
                })

            self.relationship_stats['play_during_show'] = len(play_show_df)
            logger.info(
                f"    Created {len(play_show_df):,} play_during_show relationships")
        except Exception as e:
            logger.warning(
                f"    Failed to create play_during_show relationships: {e}")
            self.relationship_stats['play_during_show'] = 0

        # 2. Show → Host relationships
        logger.info("  Creating show_hosted_by relationships...")
        try:
            show_host_query = """
            SELECT 
                ks.kb_id as show_kb_id,
                kh.kb_id as host_kb_id,
                ks.kexp_show_id,
                kh.name as host_name
            FROM kb_Show ks
            JOIN bridge_show_hosts bsh ON ks.kexp_show_id = bsh.show_id
            JOIN kb_Host kh ON bsh.host_id = kh.kexp_host_id
            """

            show_host_df = self.conn.execute(show_host_query).fetchdf()
            for _, row in show_host_df.iterrows():
                triple_id = self.generate_triple_id(
                    row['show_kb_id'], 'show_hosted_by', row['host_kb_id'])
                relationships.append({
                    'triple_id': triple_id,
                    'subject_type': 'kb_Show',
                    'subject_id': row['show_kb_id'],
                    'predicate': 'show_hosted_by',
                    'object_type': 'kb_Host',
                    'object_id': row['host_kb_id'],
                    'source_name': f"Show {row['kexp_show_id']}",
                    'target_name': row['host_name'],
                    'mb_relation_type': 'kexp_broadcast',
                    'mb_target_type': 'host'
                })

            self.relationship_stats['show_hosted_by'] = len(show_host_df)
            logger.info(
                f"    Created {len(show_host_df):,} show_hosted_by relationships")
        except Exception as e:
            logger.warning(
                f"    Failed to create show_hosted_by relationships: {e}")
            self.relationship_stats['show_hosted_by'] = 0

        # 3. Comment → Play relationships
        logger.info("  Creating comment_about_play relationships...")
        try:
            comment_play_query = """
            SELECT 
                kc.kb_id as comment_kb_id,
                kp.kb_id as play_kb_id,
                kc.play_id,
                LEFT(kc.comment_text, 50) as comment_preview
            FROM kb_KexpComment kc
            JOIN kb_Play kp ON kc.play_id = kp.play_id
            """

            comment_play_df = self.conn.execute(comment_play_query).fetchdf()
            for _, row in comment_play_df.iterrows():
                triple_id = self.generate_triple_id(
                    row['comment_kb_id'], 'comment_about_play', row['play_kb_id'])
                relationships.append({
                    'triple_id': triple_id,
                    'subject_type': 'kb_KexpComment',
                    'subject_id': row['comment_kb_id'],
                    'predicate': 'comment_about_play',
                    'object_type': 'kb_Play',
                    'object_id': row['play_kb_id'],
                    'source_name': row['comment_preview'] + '...',
                    'target_name': f"Play {row['play_id']}",
                    'mb_relation_type': 'kexp_editorial',
                    'mb_target_type': 'play'
                })

            self.relationship_stats['comment_about_play'] = len(
                comment_play_df)
            logger.info(
                f"    Created {len(comment_play_df):,} comment_about_play relationships")
        except Exception as e:
            logger.warning(
                f"    Failed to create comment_about_play relationships: {e}")
            self.relationship_stats['comment_about_play'] = 0

        # 4. Comment → Song relationships (via play)
        logger.info("  Creating comment_about_song relationships...")
        try:
            comment_song_query = """
            SELECT 
                kc.kb_id as comment_kb_id,
                ks.kb_id as song_kb_id,
                ks.title as song_title,
                LEFT(kc.comment_text, 50) as comment_preview
            FROM kb_KexpComment kc
            JOIN fact_plays fp ON kc.play_id = fp.play_id
            JOIN bridge_kb_song_to_kexp bks ON fp.track_id_internal = bks.kexp_track_id_internal
            JOIN kb_Song ks ON bks.kb_song_id = ks.kb_id
            """

            comment_song_df = self.conn.execute(comment_song_query).fetchdf()
            for _, row in comment_song_df.iterrows():
                triple_id = self.generate_triple_id(
                    row['comment_kb_id'], 'comment_about_song', row['song_kb_id'])
                relationships.append({
                    'triple_id': triple_id,
                    'subject_type': 'kb_KexpComment',
                    'subject_id': row['comment_kb_id'],
                    'predicate': 'comment_about_song',
                    'object_type': 'kb_Song',
                    'object_id': row['song_kb_id'],
                    'source_name': row['comment_preview'] + '...',
                    'target_name': row['song_title'],
                    'mb_relation_type': 'kexp_editorial',
                    'mb_target_type': 'song'
                })

            self.relationship_stats['comment_about_song'] = len(
                comment_song_df)
            logger.info(
                f"    Created {len(comment_song_df):,} comment_about_song relationships")
        except Exception as e:
            logger.warning(
                f"    Failed to create comment_about_song relationships: {e}")
            self.relationship_stats['comment_about_song'] = 0

        # Insert all relationships into kb_Relationship
        if relationships:
            try:
                import pandas as pd
                relationships_df = pd.DataFrame(relationships)
                relationships_df['created_at'] = datetime.now()

                before_count = self.conn.execute(
                    "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]
                self.conn.execute("""
                    INSERT OR IGNORE INTO kb_Relationship 
                    (triple_id, subject_type, subject_id, predicate, object_type, object_id, 
                     source_name, target_name, mb_relation_type, mb_target_type, created_at)
                    SELECT * FROM relationships_df
                """)
                after_count = self.conn.execute(
                    "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]

                total_new = after_count - before_count
                logger.info(
                    f"  Inserted {total_new:,} new relationships into kb_Relationship")
            except Exception as e:
                logger.error(f"  Failed to insert relationships: {e}")
        else:
            logger.warning("  No relationships to insert")

    def validate_entities(self) -> None:
        """Validate created entities and relationships"""
        logger.info("Validating KEXP entities...")

        # Check entity counts
        for entity_type, expected_count in self.entity_stats.items():
            actual_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {entity_type}").fetchone()[0]
            if actual_count == expected_count:
                logger.info(f"  ✅ {entity_type}: {actual_count:,} entities")
            else:
                logger.warning(
                    f"  ❌ {entity_type}: Expected {expected_count:,}, got {actual_count:,}")

        # Check relationship coverage
        total_new_relationships = sum(self.relationship_stats.values())
        logger.info(
            f"  📊 Total new relationships: {total_new_relationships:,}")

        # Check KB completeness
        participating_entities = self.conn.execute("""
            SELECT DISTINCT subject_type as entity_type FROM kb_Relationship
            UNION 
            SELECT DISTINCT object_type as entity_type FROM kb_Relationship
        """).fetchdf()['entity_type'].tolist()

        kexp_entities = ['kb_Play', 'kb_Show', 'kb_Host', 'kb_KexpComment']
        for entity in kexp_entities:
            status = "✅ In relationships" if entity in participating_entities else "❌ Missing from relationships"
            logger.info(f"  {entity}: {status}")

    def generate_report(self) -> None:
        """Generate final processing report"""
        total_entities = sum(self.entity_stats.values())
        total_relationships = sum(self.relationship_stats.values())

        final_kb_count = self.conn.execute(
            "SELECT COUNT(*) FROM kb_Relationship").fetchone()[0]

        print("\n" + "="*60)
        print("KEXP BROADCAST ENTITIES - CREATION COMPLETE")
        print("="*60)

        print(f"\nENTITIES CREATED:")
        for entity_type, count in self.entity_stats.items():
            print(f"  {entity_type:<20} {count:>8,} entities")
        print(f"  {'TOTAL':<20} {total_entities:>8,} entities")

        print(f"\nRELATIONSHIPS CREATED:")
        for rel_type, count in self.relationship_stats.items():
            print(f"  {rel_type:<25} {count:>8,} relationships")
        print(f"  {'TOTAL NEW':<25} {total_relationships:>8,} relationships")

        print(f"\nKB STATE:")
        print(f"  Total KB relationships: {final_kb_count:,}")
        print(f"  KEXP broadcast layer: ✅ Complete")

        print("\n" + "="*60)

    def run_full_processing(self) -> None:
        """Execute complete KEXP entity creation pipeline"""
        logger.info("Starting KEXP broadcast entity creation...")

        # Create entities in dependency order
        self.create_kb_host()
        self.create_kb_program()
        self.create_kb_show()
        self.create_kb_play()
        self.create_kb_kexpcomment()

        # Create relationships
        self.create_relationships()

        # Validate and report
        self.validate_entities()
        self.generate_report()

        self.conn.close()
        logger.info("KEXP broadcast entity creation completed")


def main():
    processor = KexpEntityProcessor()
    processor.run_full_processing()


if __name__ == "__main__":
    main()
