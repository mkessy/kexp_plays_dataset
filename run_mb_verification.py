#!/usr/bin/env python3
"""
Fixed Attribute Extraction - Focus on Mining Instrument Data
Skip table recreation, fix UNNEST syntax, extract instrument attributes properly.
"""

import duckdb
import os
from datetime import datetime

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
OUTPUT_DIR = "verification_results"


def connect_db() -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB database."""
    try:
        conn = duckdb.connect(DB_PATH)
        print(f"✅ Connected to database: {DB_PATH}")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        raise


def save_results_to_file(results, filename):
    """Save query results to a text file."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, 'w') as f:
        f.write(f"MusicBrainz Attribute Extraction Results\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*80}\n\n")

        for result in results:
            f.write(f"QUERY: {result['query_name']}\n")
            f.write(f"{'-'*50}\n")
            f.write(f"SQL:\n{result['query_sql']}\n\n")

            if 'error' in result:
                f.write(f"❌ ERROR: {result['error']}\n\n")
            else:
                f.write(f"RESULT: {result['row_count']} rows\n")
                if result['columns']:
                    f.write(f"COLUMNS: {', '.join(result['columns'])}\n")

                if result['rows']:
                    f.write(f"DATA:\n")
                    for i, row in enumerate(result['rows'][:50]):
                        f.write(f"  {i+1}. {row}\n")
                    if len(result['rows']) > 50:
                        f.write(
                            f"  ... and {len(result['rows'])-50} more rows\n")
                f.write(f"\n")

            f.write(f"{'='*80}\n\n")

    print(f"💾 Results saved to: {filepath}")


def run_query_section(conn, section_name, queries, output_file):
    """Run a section of queries and save results."""
    print(f"\n{'='*60}")
    print(f"🔍 RUNNING: {section_name}")
    print(f"{'='*60}")

    results = []

    for query_name, query_sql in queries:
        try:
            print(f"\n📊 Executing: {query_name}")

            # Execute query
            result = conn.execute(query_sql).fetchall()

            # Get column names
            description = conn.description
            columns = [desc[0] for desc in description] if description else []

            # Store result
            query_result = {
                'query_name': query_name,
                'query_sql': query_sql,
                'columns': columns,
                'rows': result,
                'row_count': len(result)
            }
            results.append(query_result)

            # Print result summary
            print(f"   ✅ Returned {len(result)} rows")
            if result and len(result) <= 5:
                for row in result:
                    print(f"   → {row}")
            elif result:
                print(f"   → First 3 rows:")
                for i, row in enumerate(result[:3]):
                    print(f"     {i+1}. {row}")
                if len(result) > 3:
                    print(f"     ... and {len(result)-3} more rows")

        except Exception as e:
            print(f"   ❌ Error: {e}")
            error_result = {
                'query_name': query_name,
                'query_sql': query_sql,
                'error': str(e),
                'columns': [],
                'rows': [],
                'row_count': 0
            }
            results.append(error_result)

    save_results_to_file(results, output_file)
    return results


def main():
    """Main execution function - Focus on attribute extraction only."""
    print("🎵 KEXP MusicBrainz Attribute Extraction - FIXED UNNEST")
    print("="*60)

    conn = connect_db()

    try:
        # Check if corrected table exists
        table_exists = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'mb_relations_basic_v2'
        """).fetchone()[0]

        if table_exists == 0:
            print(
                "❌ mb_relations_basic_v2 table not found. Please run the table creation first.")
            return

        print(f"✅ Using existing mb_relations_basic_v2 table")

        # Verify table content
        count = conn.execute(
            "SELECT COUNT(*) FROM mb_relations_basic_v2").fetchone()[0]
        print(f"   Table contains {count:,} relations")

        # FIXED UNNEST QUERIES - Using proper DuckDB syntax
        attribute_queries = [
            ("Instrument Attributes Analysis - FIXED", """
                SELECT 
                    attr as instrument_attribute,
                    COUNT(*) as usage_count,
                    COUNT(DISTINCT artist_mb_id) as unique_artists
                FROM mb_relations_basic_v2, UNNEST(attributes_array) AS t(attr)
                WHERE relation_type = 'instrument'
                  AND attributes_array IS NOT NULL
                  AND array_length(attributes_array) > 0
                GROUP BY attr
                ORDER BY usage_count DESC
                LIMIT 30
            """),

            ("Vocal Attributes Analysis - FIXED", """
                SELECT 
                    attr as vocal_attribute,
                    COUNT(*) as usage_count,
                    COUNT(DISTINCT artist_mb_id) as unique_artists
                FROM mb_relations_basic_v2, UNNEST(attributes_array) AS t(attr)
                WHERE relation_type = 'vocal'
                  AND attributes_array IS NOT NULL
                  AND array_length(attributes_array) > 0
                GROUP BY attr
                ORDER BY usage_count DESC
                LIMIT 20
            """),

            ("All Recording Performance Attributes - FIXED", """
                SELECT 
                    relation_type,
                    attr as performance_attribute,
                    COUNT(*) as usage_count
                FROM mb_relations_basic_v2, UNNEST(attributes_array) AS t(attr)
                WHERE target_type = 'recording'
                  AND attributes_array IS NOT NULL
                  AND array_length(attributes_array) > 0
                GROUP BY relation_type, attr
                ORDER BY usage_count DESC
                LIMIT 40
            """),

            ("Producer Attributes Analysis", """
                SELECT 
                    attr as producer_attribute,
                    COUNT(*) as usage_count,
                    COUNT(DISTINCT artist_mb_id) as unique_artists
                FROM mb_relations_basic_v2, UNNEST(attributes_array) AS t(attr)
                WHERE relation_type = 'producer'
                  AND attributes_array IS NOT NULL
                  AND array_length(attributes_array) > 0
                GROUP BY attr
                ORDER BY usage_count DESC
                LIMIT 15
            """),

            ("Attribute Coverage Analysis", """
                SELECT 
                    relation_type,
                    COUNT(*) as total_relations,
                    SUM(CASE WHEN attributes_array IS NOT NULL AND array_length(attributes_array) > 0 THEN 1 ELSE 0 END) as relations_with_attributes,
                    ROUND(SUM(CASE WHEN attributes_array IS NOT NULL AND array_length(attributes_array) > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as attribute_coverage_percent
                FROM mb_relations_basic_v2
                WHERE target_type = 'recording'
                GROUP BY relation_type
                ORDER BY total_relations DESC
                LIMIT 15
            """)
        ]

        run_query_section(conn, "FIXED ATTRIBUTE EXTRACTION",
                          attribute_queries, "07_fixed_attributes.txt")

        # Now create the corrected extraction tables using proper syntax
        print(f"\n{'='*60}")
        print(f"🏗️ CREATING EXTRACTION TABLES - FIXED SYNTAX")
        print(f"{'='*60}")

        # Create instruments extraction table with corrected UNNEST
        print("Creating fixed instruments extraction table...")
        conn.execute("DROP TABLE IF EXISTS extract_instruments_fixed")

        conn.execute("""
            CREATE TABLE extract_instruments_fixed AS
            SELECT 
                row_number() OVER (ORDER BY usage_count DESC) as extract_id,
                instrument_name,
                instrument_category,
                usage_count,
                unique_artists,
                sample_artists,
                'Extracted from MusicBrainz instrument/vocal relations' as source
            FROM (
                SELECT 
                    attr as instrument_name,
                    CASE 
                        WHEN LOWER(attr) LIKE '%vocal%' OR LOWER(attr) LIKE '%sing%' OR LOWER(attr) LIKE '%choir%'
                            THEN 'Vocals'
                        WHEN LOWER(attr) LIKE '%guitar%' OR LOWER(attr) LIKE '%bass%' OR LOWER(attr) LIKE '%banjo%' OR LOWER(attr) LIKE '%mandolin%'
                            THEN 'Strings'
                        WHEN LOWER(attr) LIKE '%drum%' OR LOWER(attr) LIKE '%percussion%' OR LOWER(attr) LIKE '%timpani%'
                            THEN 'Percussion'
                        WHEN LOWER(attr) LIKE '%keyboard%' OR LOWER(attr) LIKE '%piano%' OR LOWER(attr) LIKE '%organ%' OR LOWER(attr) LIKE '%synthesizer%'
                            THEN 'Keys'
                        WHEN LOWER(attr) LIKE '%trumpet%' OR LOWER(attr) LIKE '%horn%' OR LOWER(attr) LIKE '%trombone%' OR LOWER(attr) LIKE '%tuba%'
                            THEN 'Brass'
                        WHEN LOWER(attr) LIKE '%flute%' OR LOWER(attr) LIKE '%clarinet%' OR LOWER(attr) LIKE '%saxophone%' OR LOWER(attr) LIKE '%oboe%'
                            THEN 'Woodwind'
                        WHEN LOWER(attr) LIKE '%violin%' OR LOWER(attr) LIKE '%viola%' OR LOWER(attr) LIKE '%cello%' OR LOWER(attr) LIKE '%double bass%'
                            THEN 'Orchestra Strings'
                        ELSE 'Other'
                    END as instrument_category,
                    COUNT(*) as usage_count,
                    COUNT(DISTINCT artist_mb_id) as unique_artists,
                    string_agg(DISTINCT artist_name, ', ') as sample_artists
                FROM mb_relations_basic_v2, UNNEST(attributes_array) AS t(attr)
                WHERE relation_type IN ('instrument', 'vocal')
                  AND attributes_array IS NOT NULL
                  AND array_length(attributes_array) > 0
                GROUP BY attr
                HAVING COUNT(*) >= 5
            ) instruments
            ORDER BY usage_count DESC
        """)

        instrument_count = conn.execute(
            "SELECT COUNT(*) FROM extract_instruments_fixed").fetchone()[0]
        print(
            f"✅ Created fixed instruments extraction table with {instrument_count} instruments")

        # Show top instruments
        print("\n🎸 Top instruments extracted:")
        top_instruments = conn.execute("""
            SELECT instrument_name, instrument_category, usage_count, unique_artists
            FROM extract_instruments_fixed
            ORDER BY usage_count DESC
            LIMIT 15
        """).fetchall()

        for instrument, category, usage, artists in top_instruments:
            print(
                f"   {instrument} ({category}): {usage:,} uses by {artists:,} artists")

        # Create roles extraction table
        print("\nCreating performance roles extraction table...")
        conn.execute("DROP TABLE IF EXISTS extract_performance_roles")

        conn.execute("""
            CREATE TABLE extract_performance_roles AS
            SELECT 
                row_number() OVER (ORDER BY usage_count DESC) as extract_id,
                role_name,
                role_category,
                relation_type,
                usage_count,
                unique_artists,
                unique_recordings,
                'Extracted from MusicBrainz performance relations' as source
            FROM (
                SELECT 
                    relation_type as role_name,
                    CASE 
                        WHEN relation_type IN ('vocal', 'lead vocals', 'background vocals', 'choir vocals') 
                            THEN 'Vocals'
                        WHEN relation_type = 'instrument' 
                            THEN 'Instrument Performance'
                        WHEN relation_type IN ('producer', 'co-producer', 'executive producer') 
                            THEN 'Production'
                        WHEN relation_type IN ('engineer', 'recording', 'mix', 'mastering', 'sound') 
                            THEN 'Engineering'
                        WHEN relation_type IN ('composer', 'writer', 'lyricist', 'arranger', 'orchestrator') 
                            THEN 'Composition'
                        WHEN relation_type IN ('conductor', 'performing orchestra', 'performer', 'main performer') 
                            THEN 'Performance Direction'
                        WHEN relation_type IN ('remixer', 'mix-DJ') 
                            THEN 'Remix/DJ'
                        ELSE 'Other'
                    END as role_category,
                    relation_type,
                    COUNT(*) as usage_count,
                    COUNT(DISTINCT artist_mb_id) as unique_artists,
                    COUNT(DISTINCT target_entity_id) as unique_recordings
                FROM mb_relations_basic_v2
                WHERE target_type = 'recording'
                GROUP BY relation_type
                HAVING COUNT(*) >= 100
            ) roles
            ORDER BY usage_count DESC
        """)

        role_count = conn.execute(
            "SELECT COUNT(*) FROM extract_performance_roles").fetchone()[0]
        print(
            f"✅ Created performance roles table with {role_count} role types")

        # Final summary with all the data we've extracted
        print(f"\n{'='*60}")
        print(f"📊 COMPREHENSIVE EXTRACTION SUMMARY")
        print(f"{'='*60}")

        summary_queries = [
            ("Total Relations in v2 Table",
             "SELECT COUNT(*) FROM mb_relations_basic_v2"),
            ("Unique Artists",
             "SELECT COUNT(DISTINCT artist_mb_id) FROM mb_relations_basic_v2"),
            ("Unique Recordings", "SELECT COUNT(DISTINCT target_entity_id) FROM mb_relations_basic_v2 WHERE target_type = 'recording'"),
            ("Instrument Relations",
             "SELECT COUNT(*) FROM mb_relations_basic_v2 WHERE relation_type = 'instrument'"),
            ("Vocal Relations",
             "SELECT COUNT(*) FROM mb_relations_basic_v2 WHERE relation_type = 'vocal'"),
            ("Relations with Attributes",
             "SELECT COUNT(*) FROM mb_relations_basic_v2 WHERE attributes_array IS NOT NULL AND array_length(attributes_array) > 0"),
            ("Extracted Instruments", "SELECT COUNT(*) FROM extract_instruments_fixed"),
            ("Extracted Performance Roles",
             "SELECT COUNT(*) FROM extract_performance_roles")
        ]

        for description, query in summary_queries:
            count = conn.execute(query).fetchone()[0]
            print(f"   {description}: {count:,}")

        # Show some key insights
        print(f"\n🎯 KEY INSIGHTS:")

        # Top instrument categories
        categories = conn.execute("""
            SELECT instrument_category, SUM(usage_count) as total_uses, COUNT(*) as instrument_count
            FROM extract_instruments_fixed
            GROUP BY instrument_category
            ORDER BY total_uses DESC
        """).fetchall()

        print(f"\n🎵 Instrument Categories:")
        for category, uses, count in categories:
            print(f"   {category}: {count} instruments, {uses:,} total uses")

        # Top performance role categories
        role_categories = conn.execute("""
            SELECT role_category, SUM(usage_count) as total_uses, COUNT(*) as role_count
            FROM extract_performance_roles
            GROUP BY role_category
            ORDER BY total_uses DESC
        """).fetchall()

        print(f"\n🎭 Performance Role Categories:")
        for category, uses, count in role_categories:
            print(f"   {category}: {count} role types, {uses:,} total uses")

        print(f"\n✅ Fixed extraction complete!")
        print(f"📋 New tables: extract_instruments_fixed, extract_performance_roles")
        print(f"📁 Detailed results in: {OUTPUT_DIR}/07_fixed_attributes.txt")

    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        import traceback
        traceback.print_exc()

    finally:
        conn.close()
        print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
