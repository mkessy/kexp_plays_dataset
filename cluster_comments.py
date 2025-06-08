#!/usr/bin/env python3
"""
BERTopic Comment Clustering for KEXP DJ Comments

This script implements topic modeling for KEXP DJ comment chunks using BERTopic.
It leverages pre-computed embeddings from DuckDB and applies focused hyperparameter
optimization for effective topic discovery.
"""

import os
import time
import logging
import numpy as np
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Tuple, List, Dict, Any
import openai
import json
import argparse

# BERTopic dependencies
from bertopic import BERTopic
from bertopic.representation import MaximalMarginalRelevance, PartOfSpeech, KeyBERTInspired, OpenAI
from bertopic.vectorizers import ClassTfidfTransformer
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer
from hdbscan import HDBSCAN
from umap import UMAP

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"bertopic_kexp_results/bertopic_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_PATH = os.getenv("DB_PATH", "kexp_data.db")
CONSERVATIVE_STRATEGY_ID = 3  # Based on analysis in create_comment_chunks_analysis.py
MIN_CHUNK_LENGTH = int(os.getenv("MIN_CHUNK_LENGTH", 75))
MIN_ALPHA_RATIO = float(os.getenv("MIN_ALPHA_RATIO", 0.4))
MIN_ALPHANUM_RATIO = float(os.getenv("MIN_ALPHANUM_RATIO", 0.6))
CHUNK_EMBEDDING_TABLE = os.getenv(
    "CHUNK_EMBEDDING_TABLE_NAME", "chunk_embeddings")
OUTPUT_DIR = Path("bertopic_kexp_results")
OUTPUT_DIR.mkdir(exist_ok=True)


def connect_db() -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB database."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        logger.info(f"Connected to database: {DB_PATH}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def clean_text(text: str) -> str:
    """
    Clean text by removing URLs, phone numbers, and email addresses.

    Args:
        text: Text to clean

    Returns:
        Cleaned text
    """
    import re

    # Replace URLs
    url_pattern = r'https?://\S+|www\.\S+|\S+\.(com|org|net|io|ly|fm|co|us|edu)\S*'
    text = re.sub(url_pattern, ' [URL] ', text)

    # Replace email addresses
    email_pattern = r'\S+@\S+\.\S+'
    text = re.sub(email_pattern, ' [EMAIL] ', text)

    # Replace phone numbers (various formats)
    phone_pattern = r'\b(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b'
    text = re.sub(phone_pattern, ' [PHONE] ', text)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # fix misspelling of "in studio" from "instudio"
    text = re.sub(r'instudio', 'in studio', text)

    return text


def fetch_embeddings_and_chunks(
    conn: duckdb.DuckDBPyConnection,
    limit: Optional[int] = None,
    random_sample: bool = True
) -> Tuple[List[str], np.ndarray, List[int], pd.DataFrame]:
    """
    Fetch quality-filtered chunks and their embeddings from DuckDB.

    Args:
        conn: DuckDB connection
        limit: Optional limit on number of chunks to fetch
        random_sample: Whether to sample randomly or use sequential ordering

    Returns:
        Tuple containing:
        - list of document texts
        - numpy array of embeddings (n_docs × embedding_dim)
        - list of chunk IDs
        - dataframe with full metadata
    """
    logger.info("Fetching chunks and embeddings from database...")

    # Build the ORDER BY clause based on random_sample parameter
    order_clause = "ORDER BY RANDOM()" if random_sample else "ORDER BY c.chunk_id"

    # Build the LIMIT clause if specified
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
    SELECT
        c.chunk_id,
        c.normalized_chunk_text AS text,
        ce.embedding,
        c.play_id,
        fp.original_artist_text,
        fp.original_song_text,
        fp.original_album_text,
        fp.airdate_iso,
        c.chunk_length,
        c.alpha_ratio,
        c.alphanum_ratio
    FROM
        comment_chunks_raw c
    JOIN
        {CHUNK_EMBEDDING_TABLE} ce ON c.chunk_id = ce.chunk_id
    JOIN
        fact_plays fp ON c.play_id = fp.play_id
    WHERE
        c.strategy_id = {CONSERVATIVE_STRATEGY_ID}
        AND c.chunk_length >= {MIN_CHUNK_LENGTH}
        AND NOT c.is_url_only
        AND c.alpha_ratio >= {MIN_ALPHA_RATIO}
        AND c.alphanum_ratio >= {MIN_ALPHANUM_RATIO}
    {order_clause}
    {limit_clause}
    """

    try:
        df = conn.execute(query).fetchdf()
        logger.info(f"Fetched {len(df)} chunks with embeddings")

        if df.empty:
            logger.warning("No data found that matches filtering criteria")
            return [], np.array([]), [], pd.DataFrame()

        # Clean text (remove URLs, phone numbers, emails)
        df['cleaned_text'] = df['text'].apply(clean_text)
        logger.info("Cleaned texts by removing URLs, phone numbers, and emails")

        # De-duplicate data based on original text (before cleaning)
        # This catches exact duplicate comments that might have been extracted multiple times
        pre_dedup_count = len(df)
        df = df.drop_duplicates(subset=['text'])
        dedup_count = pre_dedup_count - len(df)
        if dedup_count > 0:
            logger.info(
                f"Removed {dedup_count} duplicate comments ({dedup_count/pre_dedup_count:.1%} of total)")

        # Extract components - use cleaned text for topic modeling
        documents = df['cleaned_text'].tolist()
        chunk_ids = df['chunk_id'].tolist()

        # Convert embeddings from lists to numpy array
        embeddings_list = df['embedding'].tolist()
        embeddings_array = np.array(embeddings_list, dtype=np.float32)

        logger.info(f"Embeddings shape: {embeddings_array.shape}")
        return documents, embeddings_array, chunk_ids, df

    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise


def create_custom_stop_words() -> List[str]:
    """Create custom stop words for KEXP domain, including English and Spanish."""
    # Base stop words from sklearn (English)
    from sklearn.feature_extraction import text
    stop_words = list(text.ENGLISH_STOP_WORDS)

    # Load host names from JSONL file
    host_names = []
    try:
        with open('normalized_kexp_jsonl/dim_hosts.jsonl', 'r') as f:
            for line in f:
                host = json.loads(line)
                name = host.get("primary_name", "")
                if name:
                    # Add full name only
                    host_names.append(name.lower())
        logger.info(f"Loaded {len(host_names)} host names as stop words.")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(
            f"Could not load host names from dim_hosts.jsonl. Proceeding without them. Error: {e}")

    # Spanish stop words
    spanish_stop_words = [
        # Common Spanish words (pronouns, articles, prepositions, etc.)
        "el", "la", "los", "las", "un", "una", "unos", "unas", "y", "o", "pero", "si",
        "de", "del", "a", "al", "en", "por", "para", "con", "sin", "sobre", "entre",
        "yo", "tu", "él", "ella", "nosotros", "nosotras", "vosotros", "vosotras",
        "ellos", "ellas", "mi", "tu", "su", "nuestro", "nuestra", "vuestro", "vuestra",
        "este", "esta", "estos", "estas", "ese", "esa", "esos", "esas", "aquel",
        "aquella", "aquellos", "aquellas", "que", "quien", "quienes", "cual", "cuales",
        "cuyo", "cuya", "cuyos", "cuyas", "donde", "cuando", "como", "ser", "estar",
        "haber", "tener", "hacer", "ir", "venir", "decir", "ver", "dar", "saber",
        "poder", "querer", "deber", "poner", "parecer", "gustar", "es", "son", "está",
        "están", "ha", "han", "fue", "fueron", "era", "eran", "será", "serán", "sería",
        "serían", "hay", "había", "hubo", "habría", "he", "has", "hemos", "habéis",
        "tienen", "tuvo", "tendrá", "más", "menos", "muy", "mucho", "mucha", "muchos",
        "muchas", "poco", "poca", "pocos", "pocas", "grande", "grandes", "pequeño",
        "pequeña", "pequeños", "pequeñas", "bueno", "buena", "buenos", "buenas", "malo",
        "mala", "malos", "malas", "todo", "toda", "todos", "todas", "alguno", "alguna",
        "algunos", "algunas", "ninguno", "ninguna", "ningunos", "ningunas", "otro",
        "otra", "otros", "otras", "mismo", "misma", "mismos", "mismas", "tan", "tanto",
        "tanta", "tantos", "tantas", "uno", "dos", "tres", "cuatro", "cinco", "seis",
        "siete", "ocho", "nueve", "diez", "primero", "segundo", "tercero", "cuarto",
        "quinto", "siguiente", "último", "penúltimo", "mejor", "peor", "mayor", "menor",
        "igual", "diferente", "parecido", "distinto", "bien", "mal", "así", "también",
        "solo", "solamente", "además", "ya", "aún", "todavía", "siempre", "nunca",
        "jamás", "ahora", "después", "luego", "antes", "durante", "mientras", "aquí",
        "allí", "allá", "acá", "arriba", "abajo", "dentro", "fuera", "adelante", "atrás"
    ]

    # KEXP-specific stop words (domain-specific terms that don't help define topics)
    kexp_stop_words = [
        # Radio/DJ related
        "kexp", "seattle", "dj", "radio", "station", "show", "playlist", "broadcast",
        "feature", "featured", "featuring", "listener", "listeners", "request", "requests",

        # Music metadata
        "song", "track", "album", "artist", "band", "release", "released", "single", "lyrics", "lyric", "lyrical"
        "record", "records", "music", "sound", "sounds", "version", "ep", "lp", "single", "musical", "musician", "musicians",


        # Spanish music terminology
        "canción", "canciones", "música", "músico", "músicos", "artista", "artistas",
        "banda", "bandas", "grupo", "grupos", "álbum", "álbumes", "disco", "discos",
        "sencillo", "sencillos", "sonido", "sonidos", "versión", "lanzamiento",

        # Common music descriptors
        "new", "live", "latest", "debut", "original", "classic", "hit", "said",

        # Time related
        "today", "tonight", "tomorrow", "yesterday", "week", "weekend", "day", "night",
        "morning", "afternoon", "evening", "upcoming", "recent", "just", "now",

        # Common actions
        "play", "playing", "played", "hear", "heard", "listen", "listening", "check",
        "enjoy", "like", "love", "favorite", "presents", "present", "bring",
        "brings", "bringing", "brought", "coming", "back", "get", "got", "getting", "thanks",

        # days
        "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
        "january", "february", "march", "april", "may", "june", "july", "august",
        "september", "october", "november", "december",

        # Common modifiers
        "great", "good", "best", "nice", "amazing", "awesome", "excellent", "fantastic",
        "wonderful", "beautiful", "lovely", "cool", "hot", "brilliant",

        # Web/link related terms
        "http", "https", "www", "com", "org", "io", "fm", "net", "link", "click",
        "website", "stream", "online", "video", "download", "follow", "page", "site",
        "url", "email", "phone", "spotify", "twitter", "facebook", "instagram",

        # Explicit placeholders (as they appear in the text after cleaning)
        "[url]", "[email]", "[phone]",

        # Variants without brackets that might appear
        "url", "email", "phone",

        "bandcamp", "spotify", "apple", "youtube", "applemusic", "kexp", "kexps", "soundcloud", "podcasts", "podcast",

        "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th",

        "11th", "12th", "13th", "14th", "15th", "16th", "17th", "18th", "19th", "20th",

        "21st", "22nd", "23rd", "24th", "25th", "26th", "27th", "28th", "29th", "30th", "31st",

        "12pm", "7pm", "10pm", "11pm", "12am", "1am", "2am", "3am", "4am", "5am", "6am", "7am", "8am", "9am", "10am", "11am",


        # Commonly used but uninformative topic words
        "official", "watch", "heres", "check", "im", "dont", "ive", "youre", "theres",
        "cant", "didnt", "wont", "isnt", "arent", "wasnt", "werent", "doesnt", "dont",
        "theyre", "theyll", "youd", "wed", "hed", "shed", "id", "theyd",

        # Other filler words
        "etc", "one", "two", "three", "first", "second", "third", "fourth", "fifth",
        "sixth", "seventh", "eighth", "ninth", "tenth", "also", "well", "really",
        "very", "quite", "just", "even", "much", "many", "lot", "lots", "bit", "little",
        "thing", "things", "something", "anything", "nothing", "everything", "someone",
        "anyone", "everyone", "somebody", "anybody", "everybody", "none", "all",
        "every", "each", "few", "several", "some", "any", "both", "either", "neither",
        "other", "another", "else", "such", "same", "different"
    ]

    # Combine all stop words, including host names
    combined_stop_words = list(
        set(stop_words + spanish_stop_words + kexp_stop_words + host_names)
    )

    # Convert to lowercase
    combined_stop_words = [word.lower() for word in combined_stop_words]

    logger.info(
        f"Created custom stop word list with {len(combined_stop_words)} words: {len(stop_words)} English, {len(spanish_stop_words)} Spanish, {len(kexp_stop_words)} domain-specific, and {len(host_names)} host names.")

    return combined_stop_words


def configure_bertopic_components(
    n_neighbors: int = 15,
    n_components: int = 5,
    min_cluster_size: int = 30,
    min_samples: int = 10,
    random_state: int = 42,
    umap_metric: str = 'cosine',
    hdbscan_metric: str = 'euclidean',
    n_documents: Optional[int] = None
) -> Tuple[UMAP, HDBSCAN, CountVectorizer, Dict[str, Any]]:
    """
    Configure BERTopic components with safe vectorizer defaults
    """
    # Validate min_cluster_size
    if min_cluster_size <= 1:
        logger.warning(
            f"Invalid min_cluster_size {min_cluster_size}, using 2 instead")
        min_cluster_size = 2

    # 1. UMAP with configurable metric
    umap_model = UMAP(
        n_neighbors=n_neighbors,
        n_components=n_components,
        min_dist=0.1,
        metric='cosine',
        random_state=random_state,
        low_memory=True
    )

    # 2. HDBSCAN with configurable metric
    hdbscan_model = HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        metric='euclidean',
        cluster_selection_method='eom',
        prediction_data=True,
    )

    # 3. Create vectorizer with dynamic df ranges
    custom_stop_words = create_custom_stop_words()

    vectorizer_model = CountVectorizer(
        stop_words=custom_stop_words,
        ngram_range=(1, 2),
        min_df=10,
        max_df=0.7
    )

    # Define representation models for diverse topic representations
    # "Main" uses the default c-TF-IDF representation
    representation_model = {
        "Main": KeyBERTInspired(top_n_words=15, nr_repr_docs=10),
        "MMR": MaximalMarginalRelevance(diversity=0.5, top_n_words=15),
    }

    # Add POS only if spacy is available to prevent crashes
    try:
        import spacy
        spacy.load('en_core_web_sm')

        representation_model["POS"] = PartOfSpeech(
            "en_core_web_sm", pos_patterns=get_improved_pos_patterns())
        logger.info(
            "Added 'POS' to representation models. Using improved POS patterns.")
    except (ImportError, OSError):
        logger.warning(
            "spaCy or 'en_core_web_sm' not found. Skipping 'POS' representation.")

    logger.info(
        "Configured BERTopic components with Main, MMR, and POS representations.")
    return umap_model, hdbscan_model, vectorizer_model, representation_model


def get_improved_pos_patterns() -> List[List[Dict[str, str]]]:
    """
    Defines more sophisticated POS patterns to extract meaningful keyphrases
    from KEXP DJ comments. These go beyond simple nouns and adjectives.
    """
    return [
        # Noun Phrases (existing and improved)
        # e.g., "alternative rock", "new single"
        [{'POS': 'ADJ'}, {'POS': 'NOUN'}],
        # e.g., "singer songwriter", "dream pop"
        [{'POS': 'NOUN'}, {'POS': 'NOUN'}],
        # e.g., "english rock band"
        [{'POS': 'ADJ'}, {'POS': 'NOUN'}, {'POS': 'NOUN'}],

        # Proper Nouns (artists, places)
        [{'POS': 'PROPN'}],  # Catches single-word names like "Prince", "Björk"
        # e.g., "David Bowie", "New York"
        [{'POS': 'PROPN'}, {'POS': 'PROPN'}],


        # Action-oriented phrases
        # e.g., "play show", "release album"
        [{'POS': 'VERB'}, {'POS': 'NOUN'}],
        [{'POS': 'VERB'}, {'POS': 'ADV'}],  # e.g., "died unexpectedly"
        [{'POS': 'VERB'}, {'POS': 'PART'}, {'POS': 'VERB'}],  # e.g., "has to be"
    ]


def run_bertopic_analysis(
    documents: List[str],
    embeddings: np.ndarray,
    umap_model: UMAP,
    hdbscan_model: HDBSCAN,
    vectorizer_model: CountVectorizer,
    representation_model: Optional[Dict[str, Any]] = None
) -> Tuple[BERTopic, List[int]]:
    """
    Run BERTopic analysis with the provided components.

    Args:
        documents: List of text documents
        embeddings: Pre-computed embeddings matrix
        umap_model: Configured UMAP model
        hdbscan_model: Configured HDBSCAN model
        vectorizer_model: Configured CountVectorizer
        representation_model: Optional dictionary of representation models

    Returns:
        Fitted BERTopic model and topic assignments for each document.
    """
    start_time = time.time()
    logger.info("Running BERTopic analysis...")

    embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

    ctfidf_model = ClassTfidfTransformer(
        reduce_frequent_words=True,
        bm25_weighting=True,
    )

    # Configure BERTopic model
    topic_model = BERTopic(
        ctfidf_model=ctfidf_model,
        umap_model=umap_model,
        embedding_model=embedding_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        calculate_probabilities=False,
        verbose=True
    ).fit(documents, embeddings=embeddings)

    # Get document-level topic assignments (List[int])
    topics = topic_model.topics_

    topic_info = topic_model.get_topic_info()
    n_topics = len(topic_info[topic_info['Topic'] != -1])
    outlier_row = topic_info.loc[topic_info['Topic'] == -1]

    if not outlier_row.empty:
        outlier_count = outlier_row['Count'].iloc[0]
        outlier_pct = outlier_count / len(documents) * 100
    else:
        outlier_count = 0
        outlier_pct = 0.0

    duration = time.time() - start_time
    logger.info(f"BERTopic analysis completed in {duration:.2f} seconds")
    logger.info(
        f"Found {n_topics} topics and {outlier_count} outliers ({outlier_pct:.1f}%)")

    return topic_model, topics


def analyze_and_save_results(
    topic_model: BERTopic,
    topics: List[int],
    documents: List[str],
    chunk_ids: List[int],
    metadata_df: pd.DataFrame,
    output_dir: Path,
    embeddings: np.ndarray[np.float32, Any],
    results_prefix: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze topic model results and save artifacts.

    Args:
        topic_model: Fitted BERTopic model
        topics: List of topic assignments for each document
        documents: Original text documents
        chunk_ids: List of chunk IDs
        metadata_df: DataFrame with full metadata
        output_dir: Directory to save outputs
        embeddings: Pre-computed embeddings matrix for visualization
        results_prefix: Optional prefix for result files. If None, a new timestamp is generated.

    Returns:
        Dictionary with analysis results
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not results_prefix:
        results_prefix = f"bertopic_results_{timestamp}"

    # 1. Get and save topic information
    topic_info = topic_model.get_topic_info()
    topic_info.to_csv(
        output_dir / f"{results_prefix}_topic_info.csv", index=False)
    logger.info(f"Saved topic info to {results_prefix}_topic_info.csv")

    # 2. Get topic assignments for each document
    # topics are now passed directly to the function

    # 3. Combine into a results dataframe
    results_df = pd.DataFrame({
        'chunk_id': chunk_ids,
        'text': documents,
        'topic': topics
    })

    # Join with metadata
    full_results = pd.merge(
        results_df,
        metadata_df,
        on='chunk_id',
        how='left'
    )

    # Save results
    full_results.to_csv(
        output_dir / f"{results_prefix}_document_topics.csv", index=False)
    logger.info(
        f"Saved document topic assignments to {results_prefix}_document_topics.csv")

    # 4. Extract representative documents for each topic
    try:
        representative_docs = topic_model.get_representative_docs()
    except Exception:
        logger.warning(
            "Could not extract representative documents. Using manual method.")
        topic_assignments = {topic: []
                             for topic in topic_info['Topic'].unique() if topic != -1}
        doc_mapping = {i: doc for i, doc in enumerate(documents)}
        for i, topic in enumerate(topics):
            if topic != -1:
                topic_assignments[topic].append(i)

        representative_docs = {}
        for topic_id, doc_indices in topic_assignments.items():
            if len(doc_indices) > 5:
                import random
                random.seed(42)
                sampled_indices = random.sample(doc_indices, 5)
            else:
                sampled_indices = doc_indices
            representative_docs[topic_id] = [doc_mapping[idx]
                                             for idx in sampled_indices]

    # 5. Generate comprehensive topic summary with multiple representations
    topic_summary = []

    # Get all representation aspects
    representation_aspects = list(topic_model.topic_aspects_.keys())

    # Get representative docs for outliers
    outlier_indices = [i for i, topic in enumerate(topics) if topic == -1]
    outlier_docs = [documents[i]
                    for i in outlier_indices[:5]]  # Top 5 outlier docs

    for _, row in topic_info.iterrows():
        topic_id = row['Topic']
        if topic_id == -1:
            topic_summary.append({
                'topic_id': -1,
                'name': 'Outliers',
                'count': row['Count'],
                'representations': {aspect: [] for aspect in representation_aspects},
                'representative_docs': outlier_docs
            })
        else:
            # Get all representations for this topic from different aspects
            representations = {}
            for aspect in representation_aspects:
                aspect_repr = topic_model.topic_aspects_[
                    aspect].get(topic_id, [])
                representations[aspect] = [(word, score)
                                           for word, score in aspect_repr]

            # Get representative documents
            try:
                rep_docs = topic_model.get_representative_docs(topic_id)
            except Exception:
                rep_docs = representative_docs.get(topic_id, [])

            topic_summary.append({
                'topic_id': topic_id,
                'name': row['Name'],
                'count': row['Count'],
                'representations': representations,
                'representative_docs': rep_docs
            })

    # Save summary to file
    import json
    with open(output_dir / f"{results_prefix}_topic_summary.json", 'w') as f:
        json.dump(topic_summary, f, indent=2)

    logger.info(f"Saved topic summary to {results_prefix}_topic_summary.json")

    # 6. Generate and save all visualizations, wrapped in individual error handlers

    # First, generate hierarchical topics as it is needed for several visualizations
    hierarchical_topics = None
    try:
        hierarchical_topics = topic_model.hierarchical_topics(documents)
        hierarchical_topics.to_csv(
            output_dir / f"{results_prefix}_hierarchical_topic_info.csv", index=False
        )
        logger.info(
            f"Saved hierarchical topic info to {results_prefix}_hierarchical_topic_info.csv")
    except Exception as e:
        logger.warning(f"Could not generate or save hierarchical topics: {e}")

    if hierarchical_topics is not None:

        try:
            # Topic tree text representation
            topic_tree = topic_model.get_topic_tree(hierarchical_topics)
            with open(output_dir / f"{results_prefix}_topic_tree.txt", "w", encoding="utf-8") as f:
                f.write(topic_tree)
            logger.info(f"Saved topic tree to {results_prefix}_topic_tree.txt")
        except Exception as e:
            logger.warning(f"Could not generate topic tree: {e}")

    # Return summary for further analysis
    outlier_count = topic_info.loc[topic_info['Topic'] == -1,
                                   'Count'].iloc[0] if -1 in topic_info.Topic.values else 0
    analysis_results = {
        'n_documents': len(documents),
        'n_topics': len(topic_info[topic_info['Topic'] != -1]),
        'outlier_count': outlier_count,
        'outlier_percentage': outlier_count / len(documents) * 100 if len(documents) > 0 else 0,
        'topic_summary': topic_summary,
        'timestamp': timestamp,
        'file_prefix': results_prefix
    }

    return analysis_results


def reduce_and_save_model(
    topic_model: BERTopic,
    documents: List[str],
    topics: List[int],
    embeddings: np.ndarray,
    chunk_ids: List[int],
    metadata_df: pd.DataFrame,
    output_dir: Path,
    original_file_prefix: str,
    use_llm: bool = False
):
    """Reduces outliers, optionally applies LLM, and saves results."""
    logger.info(
        f"--- Starting outlier reduction for model with prefix: {original_file_prefix} ---")

    # 1. Reduce outliers
    logger.info("Reducing outliers with 'embeddings' strategy...")
    new_topics = topic_model.reduce_outliers(
        documents,
        topics=topics,
        embeddings=embeddings,
        strategy="embeddings",
        threshold=0.5,
    )

    # 2. Update topics
    logger.info("Updating topics to reflect outlier reduction...")
    topic_model.update_topics(documents, topics=new_topics)

    # 3. Handle LLM representation
    if use_llm:
        logger.info(
            "--- Proceeding with LLM-based topic representation generation ---")
        representation_model = topic_model.representation_model
        if representation_model is None:
            logger.warning(
                "No representation model found. Initializing an empty one.")
            representation_model = {}

        openai_api_key = os.getenv("OPENAI_API_KEY")
        if openai_api_key:
            logger.info(
                "OpenAI API key found. Preparing LLM-based topic summaries with GPT-4o-mini...")
            try:
                llm_prompt = """I am a topic modeler analyzing DJ comments from KEXP, a public radio station.
I have a topic that is described by the following keywords: [KEYWORDS]
The following are a few representative comments from this topic:
[DOCUMENTS]
Based on the keywords and comments, provide a concise, expert-level summary (10-15 words) of this topic.
The summary should capture the essence of what the comment is communicating about the music or artist. Focus on qualitative descriptions of the music or artist like genre, style, mood, etc.
Consider the keywords and try and relate them to the comments. Use the keywords to help guide your summary. At the end of the summary include 3 - 4 tags that describe the topic.
Your response MUST be in the format:
topic: <summary> <tags>
"""
                client = openai.OpenAI(api_key=openai_api_key)
                representation_model_llm = OpenAI(
                    client,
                    model="gpt-4o-mini",
                    prompt=llm_prompt,
                    diversity=0.1,
                    delay_in_seconds=4,
                    nr_docs=10,
                    doc_length=400,
                    tokenizer='char'
                )
                if hasattr(representation_model, '__setitem__'):
                    representation_model["LLM"] = representation_model_llm
                    logger.info(
                        "Successfully added 'LLM' to representation models for topic update.")
                    # Update topics again to generate LLM representations
                    logger.info("Updating topics with LLM representations...")
                    topic_model.update_topics(
                        documents,
                        topics=new_topics,
                        representation_model=representation_model
                    )
                else:
                    logger.warning(
                        "Cannot add LLM representation to the existing representation model. Skipping LLM update.")

            except Exception as e:
                logger.warning(
                    f"Failed to initialize or run OpenAI representation model. Error: {e}")
        else:
            logger.warning(
                "OPENAI_API_KEY not found. Cannot generate LLM representations.")

        # Analyze and save LLM results
        reduced_prefix = f"{original_file_prefix}_reduced_llm"
        logger.info(
            f"Analyzing and saving LLM-enhanced results with prefix: {reduced_prefix}")
        analyze_and_save_results(
            topic_model=topic_model,
            topics=new_topics,
            documents=documents,
            chunk_ids=chunk_ids,
            metadata_df=metadata_df,
            output_dir=output_dir,
            embeddings=embeddings,
            results_prefix=reduced_prefix
        )
        # Save the final LLM-enhanced model
        reduced_model_path = output_dir / f"{reduced_prefix}_model"
        logger.info(f"Saving LLM-enhanced model to {reduced_model_path}")
        topic_model.save(str(reduced_model_path),
                         serialization="safetensors", save_ctfidf=True, save_embedding_model="all-MiniLM-L6-v2")
        logger.info(
            f"--- Successfully saved LLM-enhanced model: {reduced_prefix} ---")

    else:
        # Analyze and save standard reduced results
        reduced_prefix = f"{original_file_prefix}_reduced"
        logger.info(
            f"Analyzing and saving reduced results with prefix: {reduced_prefix}")
        analyze_and_save_results(
            topic_model=topic_model,
            topics=new_topics,
            documents=documents,
            chunk_ids=chunk_ids,
            metadata_df=metadata_df,
            output_dir=output_dir,
            embeddings=embeddings,
            results_prefix=reduced_prefix
        )

        # Save the final reduced model
        reduced_model_path = output_dir / f"{reduced_prefix}_model"
        logger.info(f"Saving reduced model to {reduced_model_path}")
        topic_model.save(str(reduced_model_path),
                         serialization="safetensors", save_ctfidf=True, save_embedding_model=False)
        logger.info(
            f"--- Successfully reduced and saved model: {reduced_prefix} ---")


def optimize_hyperparameters(
    documents: List[str],
    embeddings: np.ndarray,
    chunk_ids: List[int],
    metadata_df: pd.DataFrame,
    output_dir: Path,
    reduce_outliers: bool = False,
    use_llm_for_reduction: bool = False
) -> None:
    logger.info("Starting hyperparameter optimization for BERTopic")

    # Expanded configuration grid based on BERTopic docs
    configs = [
        # Format: (n_neighbors, n_components, min_cluster_size, min_samples, umap_metric, hdbscan_metric)

        # Varying cluster sizes with different metrics
        # (15, 5, 25, 5, 'cosine', 'euclidean'),
        # (20, 5, 50, 10, 'cosine', 'euclidean'),
        # (30, 8, 75, 15, 'cosine', 'euclidean'),
        # (50, 10, 100, 20, 'cosine', 'euclidean'),
        # (100, 15, 150, 30, 'cosine', 'euclidean'),

        # Varying neighborhood sizes
        # (5, 5, 50, 10, 'cosine', 'euclidean'),   # Very local
        (10, 5, 35, 35, 'cosine', 'euclidean'),   # Very local
        # (50, 10, 100, 20, 'cosine', 'euclidean'),  # More global
        # (100, 15, 150, 30, 'cosine', 'euclidean'),  # Most global

        # Different dimensionality reductions
        # (15, 3, 50, 10, 'cosine', 'euclidean'),  # Low dim
        # (20, 10, 75, 15, 'cosine', 'euclidean'),  # Medium dim
        # (25, 15, 100, 20, 'cosine', 'euclidean'),  # High dim

        # Cluster size ratios
        # (20, 5, int(len(documents)*0.0005), 5,
        #  'cosine', 'euclidean'),  # 0.05% of dataset
        # (30, 8, int(len(documents)*0.001), 10,
        #  'cosine', 'euclidean'),  # 0.1% of dataset
        # (40, 10, int(len(documents)*0.002), 15,
        #  'cosine', 'euclidean'),  # 0.2% of dataset
    ]

    results = []
    random_state = 42

    for i, (n_neighbors, n_components, min_cluster_size,
            min_samples, umap_metric, hdbscan_metric) in enumerate(configs):
        logger.info(f"Running configuration {i+1}/{len(configs)}: "
                    f"UMAP(n={n_neighbors}, d={n_components}, metric={umap_metric}), "
                    f"HDBSCAN(min_size={min_cluster_size}, min_samples={min_samples}, metric={hdbscan_metric})")

        # Pass document count to component configuration
        umap_model, hdbscan_model, vectorizer_model, representation_model = configure_bertopic_components(
            n_neighbors=n_neighbors,
            n_components=n_components,
            min_cluster_size=min_cluster_size,
            min_samples=min_samples,
            random_state=random_state,
            umap_metric=umap_metric,
            hdbscan_metric=hdbscan_metric,
            n_documents=len(documents)  # Add document count
        )

        # Run BERTopic
        topic_model, topics = run_bertopic_analysis(
            documents=documents,
            embeddings=embeddings,
            umap_model=umap_model,
            hdbscan_model=hdbscan_model,
            vectorizer_model=vectorizer_model,
            representation_model=representation_model
        )

        # Analyze and save original results
        analysis = analyze_and_save_results(
            topic_model=topic_model,
            topics=topics,
            documents=documents,
            chunk_ids=chunk_ids,
            metadata_df=metadata_df,
            output_dir=output_dir,
            embeddings=embeddings
        )

        # Save the original model for each configuration
        topic_model.save(str(output_dir / f"{analysis['file_prefix']}_model"),
                         serialization="safetensors",
                         save_ctfidf=True,
                         save_embedding_model=False)
        logger.info(f"Saved model to {analysis['file_prefix']}_model")

        # Save all data used for this model run so it can be re-processed independently
        try:
            import pickle
            data_save_prefix = output_dir / analysis['file_prefix']
            logger.info(
                f"Saving model data for independent reduction to {data_save_prefix}_*")
            np.save(f"{data_save_prefix}_embeddings.npy", embeddings)
            metadata_df.to_pickle(f"{data_save_prefix}_metadata.pkl")
            with open(f"{data_save_prefix}_documents.pkl", "wb") as f:
                pickle.dump(documents, f)
            with open(f"{data_save_prefix}_chunk_ids.pkl", "wb") as f:
                pickle.dump(chunk_ids, f)
            with open(f"{data_save_prefix}_representation.pkl", "wb") as f:
                pickle.dump(representation_model, f)
        except Exception as e:
            logger.error(f"Failed to save model data for reprocessing: {e}")

        # Add configuration parameters to the analysis for comparison
        analysis['config'] = {
            'n_neighbors': n_neighbors,
            'n_components': n_components,
            'min_cluster_size': min_cluster_size,
            'min_samples': min_samples,
            'random_state': random_state,
            'umap_metric': umap_metric,
            'hdbscan_metric': hdbscan_metric
        }
        results.append(analysis)

        # Reduce outliers if requested
        if reduce_outliers:
            reduce_and_save_model(
                topic_model=topic_model,
                documents=documents,
                topics=topics,
                embeddings=embeddings,
                chunk_ids=chunk_ids,
                metadata_df=metadata_df,
                output_dir=output_dir,
                original_file_prefix=analysis['file_prefix'],
                use_llm=use_llm_for_reduction
            )

    # Save comparison of configurations
    configs_df = pd.DataFrame([
        {
            'config_id': i,
            'n_neighbors': r['config']['n_neighbors'],
            'n_components': r['config']['n_components'],
            'min_cluster_size': r['config']['min_cluster_size'],
            'min_samples': r['config']['min_samples'],
            'random_state': r['config']['random_state'],
            'umap_metric': r['config']['umap_metric'],
            'hdbscan_metric': r['config']['hdbscan_metric'],
            'n_topics': r['n_topics'],
            'outlier_percentage': r['outlier_percentage'],
            'timestamp': r['timestamp'],
            'file_prefix': r['file_prefix']
        }
        for i, r in enumerate(results)
    ])

    configs_df.to_csv(
        output_dir / "hyperparameter_comparison.csv", index=False)
    logger.info(
        "Saved hyperparameter comparison to hyperparameter_comparison.csv")


def main():
    """Main function to run BERTopic analysis on KEXP comment chunks."""
    parser = argparse.ArgumentParser(
        description="Run BERTopic analysis on KEXP comment chunks.")
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Run hyperparameter optimization."
    )
    parser.add_argument(
        "--reduce",
        action="store_true",
        help="Reduce outliers for the generated model(s). Can be used with or without --optimize."
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        default=False,
        help="Use LLM for topic representation during outlier reduction. Requires --reduce."
    )
    args = parser.parse_args()

    if args.llm and not args.reduce:
        parser.error("--llm requires --reduce.")

    logger.info("Starting BERTopic analysis for KEXP comment chunks")

    # Dependency check for POS representation
    try:
        import spacy
        spacy.load('en_core_web_sm')
    except (ImportError, OSError):
        logger.warning("spaCy or 'en_core_web_sm' model not found.")
        logger.warning("POS representation will not be available.")
        logger.info(
            "To install, run: pip install spacy && python -m spacy download en_core_web_sm")

    # Connect to database
    conn = connect_db()

    try:
        # Fetch data
        documents, embeddings, chunk_ids, metadata_df = fetch_embeddings_and_chunks(
            conn=conn,
            # limit=25000,
            random_sample=True
        )

        if not documents:
            logger.error("No documents found. Exiting.")
            return

        logger.info(
            f"Processing {len(documents)} documents with {embeddings.shape[1]}-dimensional embeddings")

        # Either run optimization or a single model
        use_optimization = args.optimize

        if use_optimization:
            # Run hyperparameter optimization
            optimize_hyperparameters(
                documents=documents,
                embeddings=embeddings,
                chunk_ids=chunk_ids,
                metadata_df=metadata_df,
                output_dir=OUTPUT_DIR,
                reduce_outliers=args.reduce,
                use_llm_for_reduction=args.llm
            )
            logger.info(
                "Optimization complete. Results saved for comparison.")

        else:
            # Run a single model with more conservative parameters for fewer, more coherent topics
            umap_model, hdbscan_model, vectorizer_model, representation_model = configure_bertopic_components(
                n_neighbors=40,
                n_components=10,
                min_cluster_size=100,
                min_samples=35,
                random_state=42
            )

            topic_model, topics = run_bertopic_analysis(
                documents=documents,
                embeddings=embeddings,
                umap_model=umap_model,
                hdbscan_model=hdbscan_model,
                vectorizer_model=vectorizer_model,
                representation_model=representation_model
            )

            # Analyze and save results
            analysis = analyze_and_save_results(
                topic_model=topic_model,
                topics=topics,
                documents=documents,
                chunk_ids=chunk_ids,
                metadata_df=metadata_df,
                output_dir=OUTPUT_DIR,
                embeddings=embeddings
            )

            # Save the model
            model_path = str(OUTPUT_DIR / f"{analysis['file_prefix']}_model")
            topic_model.save(
                model_path,
                serialization="safetensors",
                save_ctfidf=True,
                save_embedding_model=False
            )
            logger.info(f"Saved model to {model_path}")

            # Reduce outliers if requested
            if args.reduce:
                reduce_and_save_model(
                    topic_model=topic_model,
                    documents=documents,
                    topics=topics,
                    embeddings=embeddings,
                    chunk_ids=chunk_ids,
                    metadata_df=metadata_df,
                    output_dir=OUTPUT_DIR,
                    original_file_prefix=analysis['file_prefix'],
                    use_llm=args.llm
                )

    except Exception as e:
        logger.error(f"Error in main execution: {e}", exc_info=True)

    finally:
        conn.close()
        logger.info("Database connection closed")
        logger.info("BERTopic analysis complete")


if __name__ == "__main__":
    main()
