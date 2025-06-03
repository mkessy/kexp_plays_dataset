# KEXP Data Scripts

A data pipeline for downloading, processing, and analyzing KEXP radio station data, with a focus on DJ comment analysis and embedding generation.

## Overview

This project provides tools to:

1. Download KEXP data via their public API
2. Normalize the data into a dimensional model in DuckDB
3. Split DJ comments into meaningful chunks for analysis
4. Generate embeddings for semantic search and analysis

## Project Structure

```
kexp_data_scripts/
├── Core Scripts
│   ├── download.py                    # Downloads KEXP data from API
│   ├── ingest_kexp_data.py           # Ingests raw data into DuckDB
│   ├── normalize_kexp.py             # Normalizes data into dimensional model
│   ├── create_core_analysis_views.py # Creates analysis views
│   ├── create_comment_chunks_analysis.py # Implements comment chunking strategies
│   └── generate_comment_embeddings.py    # Generates embeddings (needs update)
│
├── Data
│   ├── kexp_data.db                  # DuckDB database (2.4GB)
│   ├── data/                         # Raw downloaded data
│   └── normalized_kexp_jsonl/        # Normalized data exports
│
├── Documentation
│   ├── COMMENT_CHUNKING_PROGRESS.md  # Chunking implementation progress
│   └── CORE_ANALYSIS_IMPLEMENTATION.md # Core analysis documentation
│
└── Archive/                          # Evaluation scripts and temporary files
```

## Setup

1. Create a Python virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On macOS/Linux
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt  # or use uv/poetry
   ```

3. Create `.env` file with configuration:

   ```bash
   # Database
   DB_PATH=kexp_data.db

   # Embedding model (for Apple Silicon Macs)
   EMBEDDING_MODEL_NAME=mlx-community/nomicai-modernbert-embed-base-4bit
   EMBEDDING_DIMENSION=768
   EMBEDDING_BATCH_SIZE=64
   EMBEDDING_TABLE_NAME=play_comment_embeddings

   # OpenAI (optional, for evaluation)
   OPENAI_API_KEY=your-key-here
   ```

## Usage

### 1. Download KEXP Data

```bash
python download.py --start-date 2024-01-01 --end-date 2024-12-31
```

### 2. Ingest and Normalize Data

```bash
python ingest_kexp_data.py
python normalize_kexp.py
```

### 3. Create Analysis Views

```bash
python create_core_analysis_views.py
```

### 4. Create Comment Chunks

```bash
python create_comment_chunks_analysis.py
```

### 5. Generate Embeddings (Coming Soon)

```bash
python generate_comment_embeddings.py  # Needs update to use chunking
```

## Key Findings

### Comment Chunking Analysis

- Evaluated 4 splitting strategies: standard, aggressive, conservative, double_newline
- **Conservative strategy** performs best with 4.83/5 average rating
- Dramatic shift in comment formatting around 2020 (from <2% to 40-54% multi-chunk comments)
- 86.4% of chunks pass quality filters with the standard strategy

### Database Schema

The normalized schema includes:

- **Dimension tables**: dim_artists, dim_tracks, dim_shows, dim_hosts, dim_programs
- **Fact table**: fact_plays (1.5M+ records)
- **Analysis tables**: comment_chunks_raw, comment_splitting_strategies
- **Views**: Various analysis views for querying

## Next Steps

1. **Update embedding generation** to use the conservative chunking strategy
2. **Implement chunk post-processing** to handle URL-only chunks
3. **Generate embeddings** for all quality chunks
4. **Build semantic search** interface for the embedded content

## Technologies Used

- **Database**: DuckDB
- **Language**: Python 3.12
- **Embedding Model**: MLX (Apple Silicon optimized)
- **APIs**: KEXP Public API
- **Analysis**: OpenAI GPT-4 (for evaluation)

## License

This project is for educational and research purposes. Please respect KEXP's terms of service when using their data.
