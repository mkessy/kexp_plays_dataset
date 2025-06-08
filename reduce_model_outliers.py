#!/usr/bin/env python3
"""
Standalone utility to load a pre-trained, non-reduced BERTopic model and its
full data context, reduce its outliers, and generate a new, complete set of
analysis artifacts including LLM-based summaries.
"""

import os
import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import openai
from dotenv import load_dotenv

from bertopic import BERTopic
from bertopic.representation import OpenAI

# It's necessary to re-import this function from the main script.
# In a larger project, this would live in a shared utils file.
from cluster_comments import analyze_and_save_results

# Load environment variables from .env file
load_dotenv()

# --- Logging Setup ---
log_dir = Path("bertopic_kexp_results")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "reduce_model_outliers.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main(model_dir: str, use_llm: bool):
    """
    Loads, reduces, and re-analyzes a BERTopic model.

    Args:
        model_dir: The path to the directory containing the saved,
                   non-reduced BERTopic model.
        use_llm: Whether to generate LLM-based topic representations.
    """
    # 1. Define paths from input directory
    model_path = Path(model_dir)
    if not model_path.exists() or not model_path.is_dir():
        logger.error(f"Model directory not found: {model_path}")
        return

    output_dir = model_path.parent
    file_prefix = model_path.name.replace("_model", "")
    csv_path = output_dir / f"{file_prefix}_document_topics.csv"

    # 2. Load the BERTopic model
    logger.info(f"Loading BERTopic model from: {model_path}")
    topic_model = BERTopic.load(
        str(model_path),
        embedding_model="all-MiniLM-L6-v2"
    )

    # 3. Load associated data from CSV
    logger.info(f"Loading documents and embeddings from {csv_path}...")
    try:
        # Load main data from CSV
        df = pd.read_csv(csv_path, verbose=True)

        # Use the cleaned text, which the model was trained on
        documents = df['cleaned_text'].astype(str).tolist()
        chunk_ids = df['chunk_id'].tolist()

        # The embedding is stored as a string representation of a numpy array
        # (e.g., '[0.1 0.2 0.3]'), which is not valid JSON. We need to parse it.
        logger.info("Parsing embeddings from string representation...")
        # Check if 'embedding' column exists and handle potential errors
        if 'embedding' not in df.columns:
            logger.error(
                f"'embedding' column not found in {csv_path}. Cannot proceed.")
            return
        embeddings = np.array(
            df['embedding'].apply(
                lambda s: [float(x) for x in s.strip('[] \n').split()]
            ).tolist()
        )

        # The full dataframe can serve as the metadata
        metadata_df = df

        logger.info(
            f"Successfully loaded {len(documents)} documents and "
            f"{embeddings.shape[0]} embeddings."
        )

    except FileNotFoundError:
        logger.error(f"Could not find required data file: {csv_path}")
        logger.error(
            "Ensure that the '{prefix}_document_topics.csv' from the "
            "original run exists in the same directory."
        )
        return
    except Exception as e:
        logger.error(
            f"Failed to load or parse data from CSV: {e}", exc_info=True)
        return

    # 4. Reduce outliers
    logger.info("Reducing outliers with 'embeddings' strategy...")
    topics = topic_model.topics_

    new_topics = topic_model.reduce_outliers(
        documents,
        topics=topics,
        embeddings=embeddings,
        strategy="embeddings",
        threshold=0.5,
    )

    # 5. Update topics with the new assignments after outlier reduction
    logger.info("Updating topics to reflect outlier reduction...")
    topic_model.update_topics(documents, topics=new_topics)

    # 6. Handle final representation, analysis, and saving
    if use_llm:
        logger.info(
            "--- Proceeding with LLM-based topic representation generation ---")
        # Get representation model and add LLM to it
        representation_model = topic_model.representation_model
        if representation_model is None:
            logger.warning(
                "No representation model found in the loaded BERTopic model. "
                "Initializing an empty one."
            )
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

            except Exception as e:
                logger.warning(
                    f"Failed to initialize or run OpenAI representation model. Error: {e}")
        else:
            logger.warning(
                "OPENAI_API_KEY not found. Cannot generate LLM representations.")

        # Analyze and save LLM results
        llm_prefix = f"{file_prefix}_reduced_llm"
        logger.info(
            f"Analyzing and saving LLM-enhanced results with prefix: {llm_prefix}")
        analyze_and_save_results(
            topic_model=topic_model,
            topics=new_topics,
            documents=documents,
            chunk_ids=chunk_ids,
            metadata_df=metadata_df,
            output_dir=output_dir,
            embeddings=embeddings,
            results_prefix=llm_prefix
        )
        # Save the final LLM-enhanced model
        llm_model_path = output_dir / f"{llm_prefix}_model"
        logger.info(f"Saving LLM-enhanced model to {llm_model_path}")
        topic_model.save(str(llm_model_path),
                         serialization="safetensors", save_ctfidf=True)
        logger.info(
            f"--- Successfully saved LLM-enhanced model: {llm_prefix} ---")

    else:
        # Analyze and save standard reduced results
        reduced_prefix = f"{file_prefix}_reduced"
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
                         serialization="safetensors", save_ctfidf=True)

        logger.info(
            f"--- Successfully reduced and saved model: {reduced_prefix} ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reduce outliers for a pre-trained, non-reduced BERTopic model.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "model_dir",
        type=str,
        help="Path to the directory containing the non-reduced BERTopic model.\n"
             "Example: bertopic_kexp_results/bertopic_results_20250606_203045_model"
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        default=False,
        help="Generate LLM-based topic representations after reduction."
    )
    args = parser.parse_args()
    main(args.model_dir, args.llm)
