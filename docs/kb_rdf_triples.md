# KEXP Knowledge Base Documentation

A comprehensive music knowledge graph built from KEXP radio play data and enriched with MusicBrainz metadata.

## Project Overview

The KEXP Knowledge Base (KB) transforms raw radio play data into a structured, queryable knowledge graph that connects artists, songs, albums, and their relationships. The system processes over 2.1M play records and 2.6M MusicBrainz artist records to create a unified music knowledge base.

### Key Statistics

- **2.1M** KEXP play records processed
- **177K** artists with **65K** MusicBrainz links
- **1.26M** songs with **140K** MusicBrainz links
- **84K** releases and **73K** albums
- **111K** relationship triples connecting entities
- **43** distinct relationship types

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   KEXP Plays    │    │   MusicBrainz    │    │   Knowledge Base    │
│                 │    │                  │    │                     │
│ • fact_plays    │───▶│ • mb_artists_raw │───▶│ • kb_Artist         │
│ • dim_tracks    │    │ • mb_relations   │    │ • kb_Song           │
│ • dim_artists   │    │                  │    │ • kb_Album          │
│ • dim_releases  │    │                  │    │ • kb_Release        │
└─────────────────┘    └──────────────────┘    │ • kb_Relationship   │
                                               └─────────────────────┘
```

## Database Schema

### Core Knowledge Base Entities

#### `kb_Artist` - Musical Artists/Bands

```sql
kb_id              UUID PRIMARY KEY     -- Internal KB identifier
name               VARCHAR NOT NULL     -- Artist name
mb_artist_id       UUID                 -- MusicBrainz artist ID
kb_artist_type     ENUM                 -- PERSON, GROUP, ORCHESTRA, etc.
kb_person_id       UUID                 -- Link to kb_Person if applicable
country_id         UUID                 -- Geographic origin
disambiguation     VARCHAR              -- Disambiguation text
created_at         TIMESTAMP            -- Record creation
updated_at         TIMESTAMP            -- Last modification
```

#### `kb_Song` - Musical Recordings

```sql
kb_id              UUID PRIMARY KEY     -- Internal KB identifier
title              VARCHAR NOT NULL     -- Song title
type               ENUM                 -- SONG (default)
mb_recording_id    UUID                 -- MusicBrainz recording ID
mb_work_id         UUID                 -- MusicBrainz work ID
created_at         TIMESTAMP            -- Record creation
updated_at         TIMESTAMP            -- Last modification
```

#### `kb_Album` - Release Groups

```sql
kb_id              UUID PRIMARY KEY     -- Internal KB identifier
title              VARCHAR NOT NULL     -- Album title
type               ENUM                 -- ALBUM (default)
mb_release_group_id UUID                -- MusicBrainz release group ID
created_at         TIMESTAMP            -- Record creation
updated_at         TIMESTAMP            -- Last modification
```

#### `kb_Release` - Specific Album Releases

```sql
kb_id              UUID PRIMARY KEY     -- Internal KB identifier
album_id           UUID                 -- Link to kb_Album
title              VARCHAR NOT NULL     -- Release title
mb_release_id      UUID                 -- MusicBrainz release ID
release_date       DATE                 -- Release date
country_id         UUID                 -- Release country
format             VARCHAR              -- Physical format (CD, Vinyl, etc.)
barcode            VARCHAR              -- UPC/EAN barcode
created_at         TIMESTAMP            -- Record creation
updated_at         TIMESTAMP            -- Last modification
```

#### `kb_Person` - Individual People

```sql
kb_id              UUID PRIMARY KEY     -- Internal KB identifier
legal_name         VARCHAR              -- Full legal name
common_name        VARCHAR NOT NULL     -- Commonly used name
mb_person_id       UUID                 -- MusicBrainz person ID
gender             VARCHAR              -- Gender
nationality        VARCHAR              -- Nationality
disambiguation     VARCHAR              -- Disambiguation text
created_at         TIMESTAMP            -- Record creation
updated_at         TIMESTAMP            -- Last modification
```

### Relationship System

#### `kb_Relationship` - RDF-Style Triples

```sql
triple_id          VARCHAR PRIMARY KEY  -- Unique relationship identifier
subject_type       VARCHAR NOT NULL     -- Entity type (kb_Artist, etc.)
subject_id         VARCHAR NOT NULL     -- Subject entity ID
predicate          VARCHAR NOT NULL     -- Relationship type
object_type        VARCHAR NOT NULL     -- Target entity type
object_id          VARCHAR NOT NULL     -- Target entity ID
source_name        VARCHAR              -- Human-readable source name
target_name        VARCHAR              -- Human-readable target name
mb_relation_type   VARCHAR              -- Original MusicBrainz relation
mb_target_type     VARCHAR              -- Original MusicBrainz target type
created_at         TIMESTAMP            -- Relationship creation
```

#### Relationship Types (Top 10 by Volume)

| Predicate             | Subject → Object       | Count  | Description                          |
| --------------------- | ---------------------- | ------ | ------------------------------------ |
| `plays_instrument_on` | kb_Artist → kb_Song    | 22,131 | Artist plays instrument on recording |
| `produces`            | kb_Artist → kb_Song    | 20,993 | Artist produces recording            |
| `provides_vocals_on`  | kb_Artist → kb_Song    | 19,859 | Artist provides vocals on recording  |
| `member_of`           | kb_Artist → kb_Artist  | 16,292 | Artist is member of band/group       |
| `mix_recording`       | kb_Artist → kb_Song    | 3,943  | Artist mixes recording               |
| `instrument_release`  | kb_Artist → kb_Release | 3,408  | Artist plays instrument on release   |
| `arranger_recording`  | kb_Artist → kb_Song    | 2,917  | Artist arranges recording            |
| `producer_release`    | kb_Artist → kb_Release | 2,770  | Artist produces release              |
| `performs_on`         | kb_Artist → kb_Song    | 2,636  | Artist performs on recording         |
| `vocal_release`       | kb_Artist → kb_Release | 2,056  | Artist provides vocals on release    |

### Supporting Entities

#### `kb_Genre` - Musical Genres

```sql
kb_id              UUID PRIMARY KEY
name               VARCHAR NOT NULL
mb_genre_id        UUID
description        VARCHAR
```

#### `kb_Instrument` - Musical Instruments

```sql
kb_id              UUID PRIMARY KEY
name               VARCHAR NOT NULL
mb_instrument_id   UUID
instrument_type    VARCHAR
description        VARCHAR
```

#### `kb_Location` - Geographic Locations

```sql
kb_id              UUID PRIMARY KEY
mb_area_id         UUID
name               VARCHAR NOT NULL
type               VARCHAR
country_code       VARCHAR
latitude           DECIMAL(9,6)
longitude          DECIMAL(9,6)
```

#### `kb_RecordLabel` - Record Labels

```sql
kb_id              UUID PRIMARY KEY
name               VARCHAR NOT NULL
mb_label_id        UUID
country            VARCHAR
```

### Bridge Tables - KB to KEXP Mapping

#### `bridge_kb_artist_to_kexp`

```sql
kb_artist_id       UUID PRIMARY KEY
kexp_artist_id_internal UUID PRIMARY KEY
```

#### `bridge_kb_song_to_kexp`

```sql
kb_song_id         UUID PRIMARY KEY
kexp_track_id_internal UUID PRIMARY KEY
```

## Data Processing Pipeline

### Phase 1: Foundation Data Extraction

**Script:** `entities_phase_1_foundation_extraction.py`

**Purpose:** Extract and normalize core entities from MusicBrainz dumps

**Key Operations:**

- Extract artists, locations, genres, instruments from `mb_artists_raw`
- Create staging tables for validation and deduplication
- Generate enhanced relations table for Phase 3 processing
- Establish MusicBrainz ID mappings

**Outputs:**

- `stage_artist_extraction` - Normalized artist data
- `stage_location_extraction` - Geographic entities
- `stage_genre_extraction` - Genre classifications
- `stage_instrument_extraction` - Instrument types
- `mb_relations_enhanced` - Cleaned relationship data

### Phase 2: Core Entity Population

**Script:** `entities_phase_2_core_extraction.py`

**Purpose:** Populate final KB entity tables from KEXP and MusicBrainz data

**Key Operations:**

- Cross-reference KEXP tracks/artists with MusicBrainz entities
- Create canonical KB entities with internal UUIDs
- Establish bidirectional mapping between KB and KEXP IDs
- Populate `kb_Artist`, `kb_Song`, `kb_Album`, `kb_Release`, `kb_Person`

**Data Flow:**

```
KEXP Data (fact_plays, dim_*)
    ↓
MusicBrainz Matching
    ↓
KB Entity Creation (kb_*)
    ↓
Bridge Table Population
```

**Outputs:**

- **177,517** `kb_Artist` records
- **1,262,818** `kb_Song` records
- **73,238** `kb_Album` records
- **84,454** `kb_Release` records
- **27,955** `kb_Person` records

### Phase 3: Relationship Population

**Script:** `entities_phase_3_relationships.py`

**Purpose:** Extract relationships from MusicBrainz and create RDF-style triples

**Key Operations:**

- Discover viable relationship types with KB entity coverage
- Extract relationships from `mb_relations_basic_v2`
- Validate both relationship endpoints exist in KB
- Create standardized RDF triples with semantic predicates
- Consolidate into final `kb_Relationship` table

**Relationship Discovery Process:**

1. **Coverage Analysis** - Calculate KB entity coverage for each MB relation type
2. **Viability Filtering** - Process only relations with ≥1000 instances and KB coverage
3. **Entity Validation** - Ensure both source and target entities exist in KB
4. **Triple Generation** - Create subject-predicate-object triples with metadata
5. **Consolidation** - Deduplicate and create final relationship table

**Outputs:**

- **111,091** relationship triples across **43** predicate types
- Individual CSV files per relationship type for analysis
- Consolidated `kb_Relationship` table

## Advanced Analytics Capabilities

### Topic Modeling & Semantic Analysis

**Scripts:** `cluster_comments.py`, `rate_topics.py`

- **BERTopic** implementation for DJ comment analysis
- Hyperparameter optimization for topic discovery
- Manual topic evaluation and rating system
- Semantic embeddings for content analysis

### Text Processing Pipeline

**Scripts:** `generate_comment_embeddings.py`, `create_comment_chunks_analysis.py`

- Comment chunking and normalization
- Embedding generation using sentence transformers
- Text deduplication and cleaning
- Multi-language stop word processing

### Knowledge Graph Views

**Scripts:** `create_core_analysis_views.py`

- Pre-computed analytical views for common queries
- Artist collaboration networks
- Genre classification hierarchies
- Play frequency and popularity metrics

## Usage Examples

### Basic Entity Queries

```sql
-- Find all artists from Seattle
SELECT a.name, l.name as location
FROM kb_Artist a
JOIN kb_Location l ON a.country_id = l.kb_id
WHERE l.name ILIKE '%seattle%';

-- Get all songs by a specific artist
SELECT s.title, a.name as artist
FROM kb_Song s
JOIN kb_Relationship r ON s.kb_id = r.object_id
JOIN kb_Artist a ON r.subject_id = a.kb_id
WHERE a.name = 'Nirvana'
  AND r.predicate IN ('provides_vocals_on', 'performs_on');
```

### Relationship Analysis

```sql
-- Find the most collaborative artists (most relationships)
SELECT
    a.name,
    COUNT(*) as relationship_count,
    COUNT(DISTINCT r.predicate) as relationship_types
FROM kb_Artist a
JOIN kb_Relationship r ON a.kb_id = r.subject_id
GROUP BY a.kb_id, a.name
ORDER BY relationship_count DESC
LIMIT 10;

-- Analyze instrument usage patterns
SELECT
    r.predicate,
    COUNT(*) as usage_count,
    COUNT(DISTINCT r.subject_id) as unique_artists
FROM kb_Relationship r
WHERE r.predicate LIKE '%instrument%'
GROUP BY r.predicate
ORDER BY usage_count DESC;
```

### KEXP Play Analysis

```sql
-- Find most played artists on KEXP
SELECT
    a.name,
    COUNT(p.play_id) as play_count
FROM kb_Artist a
JOIN bridge_kb_artist_to_kexp ba ON a.kb_id = ba.kb_artist_id
JOIN fact_plays p ON ba.kexp_artist_id_internal = p.track_id_internal
GROUP BY a.kb_id, a.name
ORDER BY play_count DESC
LIMIT 20;
```

## Setup Instructions

### Prerequisites

- Python 3.12+
- DuckDB 1.3.0+
- 50GB+ available disk space for full dataset

### Installation

```bash
# Clone repository
git clone <repository-url>
cd kexp_data_scripts

# Install dependencies
pip install -r requirements.txt

# Or use uv for faster installation
uv pip install -r requirements.txt
```

### Database Setup

```bash
# Download and normalize KEXP data
python scripts/download.py
python scripts/normalize_kexp.py
python scripts/ingest_kexp_data.py

# Download MusicBrainz artist dump
# Place in data/ directory as mb_artists_raw table

# Build knowledge base
python scripts/entities_phase_1_foundation_extraction.py
python scripts/entities_phase_2_core_extraction.py
python scripts/entities_phase_3_relationships.py
```

### Analytics Setup

```bash
# Generate comment embeddings
python scripts/generate_comment_embeddings.py

# Create analysis views
python scripts/create_core_analysis_views.py

# Run topic modeling
python scripts/cluster_comments.py --optimize
```

## File Structure

```
kexp_data_scripts/
├── scripts/
│   ├── entities_phase_1_foundation_extraction.py    # Phase 1: Extract foundation data
│   ├── entities_phase_2_core_extraction.py          # Phase 2: Populate KB entities
│   ├── entities_phase_3_relationships.py            # Phase 3: Create relationships
│   ├── cluster_comments.py                          # Topic modeling
│   ├── generate_comment_embeddings.py               # Embedding generation
│   ├── download.py                                  # Data acquisition
│   ├── normalize_kexp.py                           # Data normalization
│   └── ingest_kexp_data.py                         # Database ingestion
├── data/                                            # Raw data files
├── outputs/                                         # Processing outputs
│   ├── phase3_output/                              # Relationship processing
│   └── bertopic_results/                           # Topic modeling results
├── pyproject.toml                                  # Dependencies
└── README.md                                       # This file
```

## Data Quality & Validation

### Entity Coverage

- **36.7%** of artists have MusicBrainz links (65K/177K)
- **11.1%** of songs have MusicBrainz links (140K/1.26M)
- **100%** of releases and albums have MusicBrainz links

### Relationship Validation

- All relationships validated for endpoint existence in KB
- **4.6%** average conversion rate from MusicBrainz to KB relationships
- **16.7%** conversion rate for artist-to-artist relationships

### Data Lineage

- Full traceability from KEXP plays to KB entities via bridge tables
- MusicBrainz source IDs preserved in all KB entities
- Relationship provenance tracked in `mb_relation_type` fields

## Performance Considerations

### Query Optimization

- All KB entities indexed on primary keys and MusicBrainz IDs
- Relationship queries optimized with subject/object/predicate indexes
- Bridge tables enable efficient KEXP-to-KB joins

### Storage Requirements

- **Core KB entities:** ~2GB
- **Relationships:** ~500MB
- **KEXP data:** ~1GB
- **MusicBrainz raw:** ~15GB
- **Embeddings:** ~5GB

### Processing Times

- **Phase 1 Foundation:** ~30 minutes
- **Phase 2 Core Extraction:** ~45 minutes
- **Phase 3 Relationships:** ~10 minutes
- **Topic Modeling:** ~2-4 hours (depending on corpus size)

## Contributing

### Code Style

- Follow PEP 8 for Python code
- Use type hints for function signatures
- Comprehensive logging for all processing steps
- Error handling with meaningful messages

### Testing

- Validate all processing phases with data quality checks
- Ensure referential integrity between KB entities
- Test relationship queries for performance

### Documentation

- Update README for schema changes
- Document new relationship types and predicates
- Maintain processing time benchmarks

## Troubleshooting

### Common Issues

**"Table not found" errors:**

- Ensure previous processing phases completed successfully
- Check database connection and permissions

**Memory issues during processing:**

- Increase system memory or process in smaller batches
- Use `--limit` flags for testing with smaller datasets

**Missing MusicBrainz relationships:**

- Verify `mb_relations_basic_v2` table exists and is populated
- Check MB artist coverage in `dim_artists_master`

**Slow relationship queries:**

- Ensure indexes exist on `kb_Relationship` table
- Use EXPLAIN ANALYZE to identify query bottlenecks

### Monitoring & Debugging

```sql
-- Check processing status
SELECT table_name, COUNT(*) as row_count
FROM (
    SELECT 'kb_Artist' as table_name, COUNT(*) FROM kb_Artist
    UNION ALL
    SELECT 'kb_Song', COUNT(*) FROM kb_Song
    UNION ALL
    SELECT 'kb_Relationship', COUNT(*) FROM kb_Relationship
) ORDER BY row_count DESC;

-- Validate relationship integrity
SELECT COUNT(*) as orphaned_relationships
FROM kb_Relationship r
WHERE NOT EXISTS (
    SELECT 1 FROM kb_Artist a WHERE a.kb_id = r.subject_id
);
```

## Roadmap

### Immediate Priorities

- [ ] Add work-to-recording relationship mapping
- [ ] Implement genre hierarchies and classifications
- [ ] Create artist similarity metrics based on collaborations
- [ ] Add temporal analysis for relationship changes

### Future Enhancements

- [ ] Real-time KEXP data ingestion pipeline
- [ ] Graph neural network embeddings for entity similarity
- [ ] Natural language querying interface
- [ ] REST API for knowledge base access
- [ ] Integration with external music services (Spotify, Last.fm)

### Research Applications

- [ ] Music recommendation systems based on KB relationships
- [ ] Artist influence network analysis
- [ ] Genre evolution tracking over time
- [ ] DJ preference and taste profiling

---

**Project Status:** Production Ready  
**Last Updated:** June 2025  
**Database Version:** kexp_data.db v3.0
