#!/usr/bin/env python3
"""
Script to analyze MusicBrainz relationship types and their attributes.
This helps us understand what entities are related and how they should
be mapped to our knowledge base relationship tables.
"""

import duckdb
import pandas as pd
import json
from pathlib import Path
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Output directory for reports
OUTPUT_DIR = Path("./relationship_analysis")
OUTPUT_DIR.mkdir(exist_ok=True)

# Connect to the database
conn = duckdb.connect('kexp_data.db')

def analyze_relation_types():
    """Extract and analyze all unique relation types."""
    logger.info("Analyzing relation types...")
    
    query = """
    SELECT 
        relation_type, 
        target_type, 
        COUNT(*) as count 
    FROM 
        mb_relations_basic_v2 
    GROUP BY 
        relation_type, target_type 
    ORDER BY 
        count DESC
    """
    
    relation_types_df = conn.execute(query).fetchdf()
    
    logger.info(f"Found {len(relation_types_df)} unique relation type/target combinations")
    
    # Save to CSV
    output_path = OUTPUT_DIR / "relation_types_summary.csv"
    relation_types_df.to_csv(output_path, index=False)
    logger.info(f"Saved relation types summary to {output_path}")
    
    return relation_types_df

def analyze_attributes_by_relation(relation_type, target_type, sample_size=100):
    """Analyze attributes used in a specific relation type."""
    logger.info(f"Analyzing attributes for {relation_type} -> {target_type} relation...")
    
    # Get sample records
    query = f"""
    SELECT 
        artist_mb_id,
        artist_name,
        relation_type,
        target_type,
        direction,
        attributes_array,
        attribute_values,
        target_entity_id,
        target_entity_name,
        recording_data,
        release_data,
        artist_data,
        url_data
    FROM 
        mb_relations_basic_v2
    WHERE 
        relation_type = '{relation_type}'
        AND target_type = '{target_type}'
    LIMIT {sample_size}
    """
    
    samples_df = conn.execute(query).fetchdf()
    
    if len(samples_df) == 0:
        logger.warning(f"No data found for {relation_type} -> {target_type}")
        return None
    
    # Analyze attributes 
    attribute_stats = defaultdict(int)
    for attrs in samples_df['attributes_array']:
        if attrs and isinstance(attrs, list):
            for attr in attrs:
                attribute_stats[attr] += 1
    
    # Analyze attribute values if available
    attribute_value_examples = {}
    for attr_values in samples_df['attribute_values']:
        if attr_values and isinstance(attr_values, dict):
            for key, value in attr_values.items():
                if key not in attribute_value_examples:
                    attribute_value_examples[key] = []
                if value not in attribute_value_examples[key]:
                    attribute_value_examples[key].append(value)
    
    # Convert to DataFrames for easier handling
    attributes_df = pd.DataFrame([
        {"attribute": attr, "count": count} 
        for attr, count in attribute_stats.items()
    ]).sort_values('count', ascending=False)
    
    # Save to CSV files
    relation_dir = OUTPUT_DIR / f"{relation_type}_{target_type}"
    relation_dir.mkdir(exist_ok=True)
    
    # Save attribute stats
    if not attributes_df.empty:
        output_path = relation_dir / "attribute_stats.csv"
        attributes_df.to_csv(output_path, index=False)
    
    # Save attribute value examples
    if attribute_value_examples:
        output_path = relation_dir / "attribute_value_examples.json"
        with open(output_path, 'w') as f:
            json.dump(attribute_value_examples, f, indent=2)
    
    # Save sample records
    output_path = relation_dir / "samples.csv"
    samples_df.to_csv(output_path, index=False)
    
    logger.info(f"Saved analysis for {relation_type} -> {target_type} to {relation_dir}")
    
    return {
        'relation_type': relation_type,
        'target_type': target_type,
        'sample_size': len(samples_df),
        'unique_attributes': len(attributes_df) if not attributes_df.empty else 0,
        'top_attributes': attributes_df['attribute'].tolist()[:10] if not attributes_df.empty else []
    }

def create_kb_mapping_tables():
    """Create intermediate tables to map relationship entities to KB IDs."""
    logger.info("Creating KB mapping tables...")
    
    # 1. Artist Member of Band mapping
    logger.info("Creating stage_member_of_band table...")
    conn.execute("""
    CREATE OR REPLACE TABLE stage_member_of_band AS
    SELECT 
        a.artist_mb_id as group_mb_id,
        a.artist_name as group_name,
        a.target_entity_id as member_mb_id,
        a.target_entity_name as member_name,
        a.start_date,
        a.end_date,
        g.kb_id as group_kb_id,
        m.kb_id as member_kb_id
    FROM mb_relations_basic_v2 a
    LEFT JOIN kb_Artist g ON a.artist_mb_id = g.mb_artist_id
    LEFT JOIN kb_Artist m ON a.target_entity_id = m.mb_artist_id
    WHERE a.relation_type = 'member of band' 
      AND a.target_type = 'artist'
      AND a.direction = 'backward';
    """)
    
    # 2. Artist Plays Instrument mapping
    logger.info("Creating stage_artist_instrument table...")
    conn.execute("""
    CREATE OR REPLACE TABLE stage_artist_instrument AS
    WITH instrument_records AS (
        SELECT 
            r.artist_mb_id,
            r.artist_name,
            r.target_entity_id as recording_mb_id,
            r.recording_data->>'title' as recording_title,
            UNNEST(r.attributes_array) as instrument_name
        FROM mb_relations_basic_v2 r
        WHERE r.relation_type = 'instrument' 
          AND r.target_type = 'recording'
          AND r.attributes_array IS NOT NULL
          AND ARRAY_LENGTH(r.attributes_array) > 0
    )
    SELECT 
        i.artist_mb_id,
        i.artist_name,
        i.recording_mb_id,
        i.recording_title,
        i.instrument_name,
        a.kb_id as kb_artist_id,
        s.kb_id as kb_song_id,
        instr.kb_id as kb_instrument_id
    FROM instrument_records i
    LEFT JOIN kb_Artist a ON i.artist_mb_id = a.mb_artist_id
    LEFT JOIN kb_Song s ON i.recording_mb_id = s.mb_recording_id
    LEFT JOIN kb_Instrument instr ON i.instrument_name = instr.name;
    """)
    
    # 3. Artist Performs Song mapping
    logger.info("Creating stage_artist_performs_song table...")
    conn.execute("""
    CREATE OR REPLACE TABLE stage_artist_performs_song AS
    SELECT 
        r.artist_mb_id,
        r.artist_name,
        r.target_entity_id as recording_mb_id,
        r.recording_data->>'title' as recording_title,
        a.kb_id as kb_artist_id,
        s.kb_id as kb_song_id
    FROM mb_relations_basic_v2 r
    LEFT JOIN kb_Artist a ON r.artist_mb_id = a.mb_artist_id
    LEFT JOIN kb_Song s ON r.target_entity_id = s.mb_recording_id
    WHERE r.relation_type = 'performer' 
      AND r.target_type = 'recording';
    """)
    
    # 4. Production/Creative Credits mapping
    logger.info("Creating stage_production_credits table...")
    conn.execute("""
    CREATE OR REPLACE TABLE stage_production_credits AS
    SELECT 
        r.artist_mb_id,
        r.artist_name,
        r.relation_type as role_name,
        r.target_type,
        r.target_entity_id,
        CASE 
            WHEN r.target_type = 'recording' THEN r.recording_data->>'title'
            WHEN r.target_type = 'release' THEN r.release_data->>'title'
            ELSE NULL
        END as target_title,
        a.kb_id as kb_person_id,
        CASE
            WHEN r.target_type = 'recording' THEN s.kb_id
            WHEN r.target_type = 'release' THEN rel.kb_id
            ELSE NULL
        END as kb_target_id,
        role.kb_id as kb_role_id
    FROM mb_relations_basic_v2 r
    LEFT JOIN kb_Person p ON r.artist_mb_id = p.mb_id
    LEFT JOIN kb_Artist a ON r.artist_mb_id = a.mb_artist_id
    LEFT JOIN kb_Song s ON r.target_entity_id = s.mb_recording_id AND r.target_type = 'recording'
    LEFT JOIN kb_Release rel ON r.target_entity_id = rel.mb_release_id AND r.target_type = 'release'
    LEFT JOIN kb_Role role ON r.relation_type = LOWER(role.name)
    WHERE r.relation_type IN ('producer', 'composer', 'engineer', 'lyricist', 'writer')
      AND r.target_type IN ('recording', 'release');
    """)
    
    # 5. External Links mapping
    logger.info("Creating stage_external_links table...")
    conn.execute("""
    CREATE OR REPLACE TABLE stage_external_links AS
    SELECT 
        r.artist_mb_id,
        r.artist_name,
        r.relation_type as link_type,
        r.url_data->>'resource' as url,
        a.kb_id as kb_entity_id,
        'ARTIST' as entity_type
    FROM mb_relations_basic_v2 r
    LEFT JOIN kb_Artist a ON r.artist_mb_id = a.mb_artist_id
    WHERE r.target_type = 'url'
      AND r.url_data IS NOT NULL;
    """)
    
    # Get statistics on the created tables
    tables = [
        "stage_member_of_band", 
        "stage_artist_instrument", 
        "stage_artist_performs_song",
        "stage_production_credits",
        "stage_external_links"
    ]
    
    stats = {}
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        matched = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE kb_artist_id IS NOT NULL").fetchone()[0] \
                 if "kb_artist_id" in conn.execute(f"DESCRIBE {table}").fetchdf()['column_name'].values \
                 else "N/A"
        
        stats[table] = {
            "total_rows": count,
            "matched_kb_entities": matched
        }
    
    return stats

def main():
    """Main execution function."""
    logger.info("Starting MusicBrainz relationship analysis...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Analyze relation types
    relation_types_df = analyze_relation_types()
    
    # Create a summary report
    summary_data = []
    
    # Select top relation types to analyze in detail
    top_relations = relation_types_df.head(15).values.tolist()
    for relation_type, target_type, count in top_relations:
        result = analyze_attributes_by_relation(relation_type, target_type)
        if result:
            result['count'] = count
            summary_data.append(result)
    
    # Save summary report
    summary_df = pd.DataFrame(summary_data)
    summary_path = OUTPUT_DIR / "relationship_analysis_summary.csv"
    summary_df.to_csv(summary_path, index=False)
    
    # Create KB mapping tables
    mapping_stats = create_kb_mapping_tables()
    
    # Save mapping stats
    with open(OUTPUT_DIR / "mapping_stats.json", 'w') as f:
        json.dump(mapping_stats, f, indent=2)
    
    logger.info(f"Analysis complete. Reports saved to {OUTPUT_DIR}")
    
    # Print summary
    print("\nRelationship Analysis Summary:")
    print(f"- Total unique relation type/target combinations: {len(relation_types_df)}")
    print(f"- Detailed analysis for top {len(summary_data)} combinations")
    print(f"- Created {len(mapping_stats)} staging tables for KB relationship mapping")
    
    # Print mapping stats
    print("\nStaging Table Statistics:")
    for table, stats in mapping_stats.items():
        print(f"- {table}: {stats['total_rows']} total rows, {stats['matched_kb_entities']} matched KB entities")

if __name__ == "__main__":
    main()