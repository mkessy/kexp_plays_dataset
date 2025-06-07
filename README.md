# KEXP Data Scripts

A collection of scripts for processing, analyzing, and extracting insights from KEXP DJ comments and music metadata.

## Overview

This project includes tools for:
1. Downloading and normalizing KEXP data
2. Generating embeddings for comment chunks
3. Topic modeling of DJ comments using BERTopic
4. Creating knowledge graph structures from extracted insights

## Key Components

### Data Processing
- `download.py` - Download KEXP play data
- `normalize_kexp.py` - Normalize and preprocess KEXP data
- `ingest_kexp_data.py` - Import data into database

### Text Analysis
- `generate_comment_embeddings.py` - Create embeddings for DJ comments
- `create_comment_chunks_analysis.py` - Analyze and chunk DJ comments
- `cluster_comments.py` - Topic modeling with BERTopic
- `rate_topics.py` - Interactive CLI for evaluating topic quality

### Knowledge Base
- `create_kb_phase_0_1_2.py` - Setup knowledge graph schema
- `create_core_analysis_views.py` - Create analysis views

## Topic Modeling

The `cluster_comments.py` script implements topic modeling for KEXP DJ comments using BERTopic. Key features include:

- Pre-computed embeddings loaded from DuckDB
- Comprehensive text cleaning (URLs, emails, phone numbers)
- De-duplication of comment chunks
- Extensive stop word lists (English, Spanish, domain-specific)
- Hyperparameter tuning for optimal topic discovery
- MMR (Maximal Marginal Relevance) for diverse topic keywords
- Multiple outlier reduction strategies
- Topic coherence evaluation

### Usage

```bash
# Basic usage with default parameters
python cluster_comments.py

# Run with hyperparameter optimization
python cluster_comments.py --optimize

# Process all documents with random sampling
python cluster_comments.py --limit 0 --sample

# Use a predefined configuration
python cluster_comments.py --config conservative
```

### Configuration Options

- `--limit`: Number of documents to process (0 for all)
- `--sample`: Use random sampling instead of sequential
- `--optimize`: Run hyperparameter optimization
- `--config`: Predefined configuration to use:
  - `granular`: More specific topics (higher count)
  - `balanced`: Balanced approach (default)
  - `conservative`: Fewer, more general topics with higher coherence

### Topic Evaluation

Use the `rate_topics.py` script to manually evaluate topic quality:

```bash
python rate_topics.py --topic-file bertopic_kexp_results/bertopic_results_20250605_123456_topic_summary.json
```

Features include:
- Interactive topic browsing
- Rating topics as good/bad
- Topic search by keyword
- Statistics on rated topics
- Export of rating data for further analysis

## Requirements

See `pyproject.toml` for dependencies. Main requirements:
- Python 3.10+
- DuckDB
- Sentence Transformers
- BERTopic
- UMAP
- HDBSCAN
- Plotly

## License

This project is proprietary and confidential.
