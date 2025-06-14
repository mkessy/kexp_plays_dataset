#!/usr/bin/env python3
"""
KEXP Knowledge Base - Data Normalization
======================================
Normalizes raw KEXP JSON data into a structured format for ingestion.

This script is part of the KEXP Knowledge Base Pipeline and should be run 
after download.py has fetched the raw KEXP data.

The script:
1. Reads raw KEXP data from the data/ directory
2. Normalizes and transforms the data into a structured format
3. Writes normalized data as JSONL files to normalized_kexp_jsonl/ directory
4. Creates dimension tables and bridge tables for the KEXP data model
"""

import json
import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, IO, Dict, List, Optional, Set, Tuple
from pathlib import Path
from tqdm import tqdm

# --- Configuration ---
KEXP_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'kexp.org')
RAW_DIR = os.getenv("RAW_DIR", "data/")
NORMALIZED_DIR = os.getenv("NORMALIZED_DIR", "normalized_kexp_jsonl/")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- Helper Functions ---


def count_lines_in_file(file_path: str) -> int:
    """Count total lines in a file for progress tracking."""
    if not os.path.exists(file_path):
        return 0

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except Exception as e:
        logger.warning(f"Could not count lines in {file_path}: {e}")
        return 0


def to_utc_iso(date_str: str | None) -> str | None:
    if not date_str:
        return None
    try:
        # Attempt to parse with timezone info
        dt = datetime.fromisoformat(date_str)
        # If naive, assume UTC (though KEXP data usually has tz)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        # Fallback for different formats if necessary, or log error
        logger.warning(f"Could not parse date string: {date_str}")
        return None

# Helper to get date as YYYY-MM-DD string or None


def format_date_to_iso_str(date_input: Any) -> str | None:
    if isinstance(date_input, str):
        try:
            # Validate if it's a parseable date, could be YYYY-MM-DD, YYYY-MM, YYYY
            # For simplicity, this example assumes if it's a string, it's somewhat valid.
            # A more robust solution might parse and reformat.
            # If it's already YYYY-MM-DD, strptime will validate.
            if len(date_input) == 10:  # YYYY-MM-DD
                datetime.strptime(date_input, '%Y-%m-%d')
            # Add other common KEXP date formats if necessary
            return date_input
        except ValueError:
            logger.warning(
                f"Date string {date_input} is not in expected YYYY-MM-DD format.")
            return None  # Or handle as per requirements
    return None

# Helper to get time as HH:MM:SS string or None


def format_time_to_str(time_input: Any) -> str | None:
    if isinstance(time_input, str):
        try:
            datetime.strptime(time_input, '%H:%M:%S')  # Validates HH:MM:SS
            return time_input
        except ValueError:
            logger.warning(
                f"Time string {time_input} is not in HH:MM:SS format.")
            return None
    return None


def get_safe(data: dict[str, Any] | None, key: str, default: Any | None = None) -> Any | None:
    return data.get(key, default) if data else default


def generate_internal_id(entity_type: str, identifier_parts: list[str | None], mb_id: str | None = None) -> str:
    if mb_id:
        return str(mb_id)

    # Ensure all parts are strings and handle None values gracefully
    stringified_parts = [
        str(part) if part is not None else "None" for part in identifier_parts]
    identifier_string = "_".join(stringified_parts)

    if not identifier_string or all(part == "None" for part in stringified_parts):
        if entity_type == "label":
            # Deterministic ID for labels with missing names
            return str(uuid.uuid5(KEXP_NAMESPACE, "label:__MISSING_LABEL_NAME__"))
        else:
            logger.warning(
                f"Generating random UUID for entity `{entity_type}` due to missing identifier parts: {identifier_parts}")
            return str(uuid.uuid4())

    return str(uuid.uuid5(KEXP_NAMESPACE, f"{entity_type}:{identifier_string}"))


def validate_raw_files() -> bool:
    """Validate that required raw files exist before attempting normalization."""
    raw_dir = Path(RAW_DIR)

    if not raw_dir.exists():
        logger.error(f"Raw data directory '{RAW_DIR}' not found.")
        logger.error("Please run the download script (download.py) first.")
        return False

    # Check for essential files
    missing_files = []
    essential_files = ["kexp_plays.jsonl"]

    for file_name in essential_files:
        file_path = raw_dir / file_name
        if not file_path.exists():
            missing_files.append(file_name)

    if missing_files:
        logger.error(
            f"Essential files are missing: {', '.join(missing_files)}")
        logger.error("Please run the download script (download.py) first.")
        return False

    return True


def normalize_data() -> bool:
    """
    Main function to normalize KEXP data.
    Returns True if successful, False otherwise.
    """
    # Validate input files first
    if not validate_raw_files():
        return False

    logger.info(
        f"Starting normalization. Raw data from: {RAW_DIR}, Normalized data to: {NORMALIZED_DIR}")

    if not os.path.exists(NORMALIZED_DIR):
        os.makedirs(NORMALIZED_DIR)
        logger.info(f"Created directory: {NORMALIZED_DIR}")

    # --- Initialize all in-memory 'written_...' sets ---
    written_host_ids: Set[int] = set()
    written_program_ids: Set[int] = set()
    written_show_ids: Set[int] = set()
    written_artist_ids_internal: Set[str] = set()
    written_label_ids_internal: Set[str] = set()
    written_release_ids_internal: Set[str] = set()
    written_track_ids_internal: Set[str] = set()
    written_artist_id_name_pairs: Set[Tuple[str, str]] = set()
    written_release_id_name_pairs: Set[Tuple[str, str]] = set()
    written_label_id_name_pairs: Set[Tuple[str, str]] = set()
    written_timeslot_ids: Set[int] = set()

    # Using a dictionary to manage file handles
    output_files: Dict[str, IO[Any]] = {}
    file_names: List[str] = [
        "dim_hosts.jsonl", "dim_programs.jsonl", "dim_shows.jsonl",
        "dim_artists_master.jsonl", "dim_labels_master.jsonl", "dim_releases_master.jsonl",
        "dim_tracks.jsonl", "bridge_artist_id_to_names.jsonl",
        "bridge_release_id_to_names.jsonl", "bridge_label_id_to_names.jsonl",
        "fact_plays.jsonl", "bridge_show_hosts.jsonl",
        "bridge_play_to_artist.jsonl", "bridge_play_to_label.jsonl",
        "bridge_timeslot_hosts.jsonl", "dim_timeslots.jsonl"
    ]

    try:
        # Open all output files
        print("📁 Opening output files...")
        for fname in tqdm(file_names, desc="Opening files", unit="file"):
            output_files[fname] = open(os.path.join(
                NORMALIZED_DIR, fname), 'w', encoding='utf-8')
            logger.debug(f"Opened {fname} for writing.")

        # --- STAGE 1: Process Dimension-Rich Standalone Files ---
        # 1. Process kexp_hosts.jsonl -> dim_hosts.jsonl
        host_file_path = os.path.join(RAW_DIR, 'kexp_hosts.jsonl')
        if os.path.exists(host_file_path):
            host_count = count_lines_in_file(host_file_path)
            logger.info(f"Processing {host_count:,} hosts...")

            with open(host_file_path, 'r', encoding='utf-8') as f:
                pbar = tqdm(f, total=host_count,
                            desc="🎙️  Processing hosts", unit="host")
                for line in pbar:
                    raw: dict[str, Any] = json.loads(line)
                    host_id = get_safe(raw, 'id')
                    if host_id is not None and host_id not in written_host_ids:
                        dim_host: dict[str, Any] = {
                            "host_id": host_id,
                            "primary_name": get_safe(raw, 'name'),
                            "host_uri": get_safe(raw, 'uri')
                        }
                        output_files['dim_hosts.jsonl'].write(
                            json.dumps(dim_host) + '\n')
                        written_host_ids.add(host_id)
                    pbar.set_postfix({"processed": len(written_host_ids)})
        else:
            logger.warning(f"File {host_file_path} not found.")

        # 2. Process kexp_programs.jsonl -> dim_programs.jsonl
        program_file_path = os.path.join(RAW_DIR, 'kexp_programs.jsonl')
        if os.path.exists(program_file_path):
            program_count = count_lines_in_file(program_file_path)
            logger.info(f"Processing {program_count:,} programs...")

            with open(program_file_path, 'r', encoding='utf-8') as f:
                pbar = tqdm(f, total=program_count,
                            desc="📻 Processing programs", unit="program")
                for line in pbar:
                    raw_program = json.loads(line)
                    program_id = get_safe(raw_program, 'id')
                    if program_id is not None and program_id not in written_program_ids:
                        dim_program: dict[str, Any] = {
                            "program_id": program_id,
                            "primary_name": get_safe(raw_program, 'name'),
                            "program_uri": get_safe(raw_program, 'uri'),
                            "description": get_safe(raw_program, 'description'),
                            # Expect list
                            "tags": get_safe(raw_program, 'tags', []),
                            "image_uri": get_safe(raw_program, 'image_uri')
                        }
                        output_files['dim_programs.jsonl'].write(
                            json.dumps(dim_program) + '\n')
                        written_program_ids.add(program_id)
                    pbar.set_postfix({"processed": len(written_program_ids)})
        else:
            logger.warning(f"File {program_file_path} not found.")

        # 3. Process kexp_timeslots.jsonl -> dim_timeslots.jsonl, bridge_timeslot_hosts.jsonl
        timeslot_file_path = os.path.join(RAW_DIR, 'kexp_timeslots.jsonl')
        if os.path.exists(timeslot_file_path):
            timeslot_count = count_lines_in_file(timeslot_file_path)
            logger.info(f"Processing {timeslot_count:,} timeslots...")

            with open(timeslot_file_path, 'r', encoding='utf-8') as f:
                pbar = tqdm(f, total=timeslot_count,
                            desc="⏰ Processing timeslots", unit="timeslot")
                for line in pbar:
                    raw_timeslot: dict[str, Any] = json.loads(line)
                    timeslot_id = get_safe(raw_timeslot, 'id')

                    if timeslot_id is not None and timeslot_id not in written_timeslot_ids:
                        dim_timeslot: dict[str, Any] = {
                            "timeslot_id": timeslot_id,
                            # KEXP API uses 'program' for ID
                            "program_id": get_safe(raw_timeslot, 'program'),
                            "weekday": get_safe(raw_timeslot, 'weekday'),
                            "start_date_iso": format_date_to_iso_str(get_safe(raw_timeslot, 'start_date')),
                            "end_date_iso": format_date_to_iso_str(get_safe(raw_timeslot, 'end_date')),
                            "start_time_str": format_time_to_str(get_safe(raw_timeslot, 'start_time')),
                            "end_time_str": format_time_to_str(get_safe(raw_timeslot, 'end_time')),
                            # Typically string like "02:00:00"
                            "duration_str": get_safe(raw_timeslot, 'duration')
                        }
                        output_files['dim_timeslots.jsonl'].write(
                            json.dumps(dim_timeslot) + '\n')
                        written_timeslot_ids.add(timeslot_id)

                    _host_ids_raw = get_safe(raw_timeslot, 'hosts', [])
                    host_ids_for_timeslot: list[int] = _host_ids_raw if isinstance(
                        _host_ids_raw, list) else []
                    _host_names_raw = get_safe(raw_timeslot, 'host_names', [])
                    host_names_for_timeslot: list[str] = _host_names_raw if isinstance(
                        _host_names_raw, list) else []

                    for i, host_id in enumerate(host_ids_for_timeslot):

                        if timeslot_id is not None:  # Ensure timeslot_id is valid for bridge
                            bridge_record: dict[str, Any] = {
                                "timeslot_id": timeslot_id, "host_id": host_id}
                            output_files['bridge_timeslot_hosts.jsonl'].write(
                                json.dumps(bridge_record) + '\n')

                        if host_id not in written_host_ids:
                            host_name = host_names_for_timeslot[i] if i < len(
                                host_names_for_timeslot) else f"Unknown Host {host_id}"
                            new_dim_host: dict[str, Any] = {
                                "host_id": host_id,
                                "primary_name": host_name,
                                "host_uri": None  # URI not typically available in timeslot host list
                            }
                            output_files['dim_hosts.jsonl'].write(
                                json.dumps(new_dim_host) + '\n')
                            written_host_ids.add(host_id)

                    pbar.set_postfix(
                        {"processed": len(written_timeslot_ids), "hosts": len(written_host_ids)})
        else:
            logger.warning(f"File {timeslot_file_path} not found.")

        # --- STAGE 2: Process Shows (depends on Hosts, Programs) ---
        show_file_path = os.path.join(RAW_DIR, 'kexp_shows.jsonl')
        if os.path.exists(show_file_path):
            show_count = count_lines_in_file(show_file_path)
            logger.info(f"Processing {show_count:,} shows...")

            with open(show_file_path, 'r', encoding='utf-8') as f:
                pbar = tqdm(f, total=show_count,
                            desc="📺 Processing shows", unit="show")
                for line in pbar:
                    raw_show: dict[str, Any] = json.loads(line)
                    show_id = get_safe(raw_show, 'id')
                    if show_id is not None and show_id not in written_show_ids:
                        dim_show: dict[str, Any] = {
                            "show_id": show_id,
                            "show_uri": get_safe(raw_show, 'uri'),
                            "program_id": get_safe(raw_show, 'program_id'),
                            # Use 'start_time'
                            "start_time_iso": to_utc_iso(get_safe(raw_show, 'start_time')),
                            # Keep if sometimes present
                            "tagline_at_show_time": get_safe(raw_show, 'tagline'),
                            # Keep if sometimes present
                            "title_at_show_time": get_safe(raw_show, 'title'),
                            "program_name_at_show_time": get_safe(raw_show, 'program_name'),
                            # Expect list
                            "program_tags_at_show_time": get_safe(raw_show, 'program_tags', []),
                            # Use 'hosts'
                            "host_ids_at_show_time": get_safe(raw_show, 'hosts', [])
                        }
                        output_files['dim_shows.jsonl'].write(
                            json.dumps(dim_show) + '\n')
                        written_show_ids.add(show_id)

                    _show_host_ids_raw = get_safe(raw_show, 'hosts', [])
                    host_ids_for_show: list[int] = _show_host_ids_raw if isinstance(
                        _show_host_ids_raw, list) else []
                    _show_host_names_raw = get_safe(raw_show, 'host_names', [])
                    host_names_for_show: list[str] = _show_host_names_raw if isinstance(
                        _show_host_names_raw, list) else []

                    for i, host_id in enumerate(host_ids_for_show):
                        if show_id is not None:  # Ensure show_id is valid for bridge
                            bridge_record = {
                                "show_id": show_id, "host_id": host_id}
                            output_files['bridge_show_hosts.jsonl'].write(
                                json.dumps(bridge_record) + '\n')

                        if host_id not in written_host_ids:
                            host_name = host_names_for_show[i] if i < len(
                                host_names_for_show) else f"Unknown Host {host_id}"
                            new_dim_host: dict[str, Any] = {
                                "host_id": host_id,
                                "primary_name": host_name,
                                "host_uri": None  # URI not typically available in show host list
                            }
                            output_files['dim_hosts.jsonl'].write(
                                json.dumps(new_dim_host) + '\n')
                            written_host_ids.add(host_id)

                    pbar.set_postfix(
                        {"processed": len(written_show_ids), "hosts": len(written_host_ids)})
        else:
            logger.warning(f"File {show_file_path} not found.")

        # --- STAGE 3: Process Plays ---
        play_file_path = os.path.join(RAW_DIR, 'kexp_plays.jsonl')
        if os.path.exists(play_file_path):
            # Count total lines first for accurate progress tracking
            print("📊 Counting plays for progress tracking...")
            play_count = count_lines_in_file(play_file_path)
            logger.info(f"Processing {play_count:,} plays...")

            with open(play_file_path, 'r', encoding='utf-8') as f:
                pbar = tqdm(f, total=play_count,
                            desc="🎵 Processing plays", unit="play")

                tracks_created = 0
                releases_created = 0
                artists_created = 0
                labels_created = 0
                skipped_airbreaks = 0

                for line_num, line in enumerate(pbar):
                    try:
                        raw_play: dict[str, Any] = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            f"Skipping malformed JSON line in plays file: {line_num+1} - {e}")
                        continue

                    play_id = get_safe(raw_play, 'id')
                    if play_id is None:
                        logger.warning(
                            f"Play without ID found at line {line_num+1}, skipping.")
                        continue

                    # CRITICAL: Filter Out "Airbreak" Plays
                    if get_safe(raw_play, 'play_type') != 'trackplay':
                        skipped_airbreaks += 1
                        pbar.set_postfix({
                            "tracks": tracks_created,
                            "artists": artists_created,
                            "skipped": skipped_airbreaks
                        })
                        continue  # Skip airbreaks for fact_plays and related dimensions

                    original_artist_text: str | None = get_safe(
                        raw_play, 'artist')
                    original_album_text: str | None = get_safe(
                        raw_play, 'album')
                    original_song_text: str | None = get_safe(
                        raw_play, 'song')

                    mb_track_id: str | None = get_safe(raw_play, 'track_id')
                    mb_recording_id: str | None = get_safe(
                        raw_play, 'recording_id')

                    internal_track_id: str = generate_internal_id(
                        "track",
                        [original_song_text, original_artist_text],
                        mb_track_id
                    )

                    mb_release_id_on_play: str | None = get_safe(
                        raw_play, 'release_id')
                    internal_release_id_for_track: str | None = None
                    if mb_release_id_on_play:
                        internal_release_id_for_track = str(
                            mb_release_id_on_play)
                    elif original_album_text:
                        internal_release_id_for_track = generate_internal_id(
                            "release",
                            [original_album_text, original_artist_text]
                        )

                    if internal_track_id not in written_track_ids_internal:
                        dim_track: dict[str, Any] = {
                            "track_id_internal": internal_track_id,
                            "primary_song_title_observed": original_song_text,
                            "mb_track_id": mb_track_id,
                            "mb_recording_id": mb_recording_id,
                            "release_id_internal_on_track": internal_release_id_for_track
                        }
                        output_files['dim_tracks.jsonl'].write(
                            json.dumps(dim_track) + '\n')
                        written_track_ids_internal.add(internal_track_id)
                        tracks_created += 1

                    internal_release_id: str | None = None
                    if mb_release_id_on_play:
                        internal_release_id = str(mb_release_id_on_play)
                    elif original_album_text:
                        internal_release_id = generate_internal_id(
                            "release",
                            [original_album_text, original_artist_text]
                        )

                    if internal_release_id and internal_release_id not in written_release_ids_internal:
                        dim_release: dict[str, Any] = {
                            "release_id_internal": internal_release_id,
                            "primary_album_name_observed": original_album_text,
                            "mb_release_id": mb_release_id_on_play,
                            "mb_release_group_id": get_safe(raw_play, 'release_group_id'),
                            "release_date_iso": format_date_to_iso_str(get_safe(raw_play, 'release_date'))
                        }
                        output_files['dim_releases_master.jsonl'].write(
                            json.dumps(dim_release) + '\n')
                        written_release_ids_internal.add(internal_release_id)
                        releases_created += 1

                    if internal_release_id and original_album_text and (internal_release_id, original_album_text) not in written_release_id_name_pairs:
                        bridge_release_name: dict[str, Any] = {
                            "release_id_internal": internal_release_id,
                            "observed_album_name_string": original_album_text
                        }
                        output_files['bridge_release_id_to_names.jsonl'].write(
                            json.dumps(bridge_release_name) + '\n')
                        written_release_id_name_pairs.add(
                            (internal_release_id, original_album_text))

                    _artist_ids_raw = get_safe(raw_play, 'artist_ids', [])
                    mb_artist_ids_from_play: list[str] = _artist_ids_raw if isinstance(
                        _artist_ids_raw, list) else []
                    processed_artist_internals_for_this_play: list[str] = []

                    if mb_artist_ids_from_play:
                        for mb_artist_id_val in mb_artist_ids_from_play:
                            internal_artist_id = str(mb_artist_id_val)
                            if internal_artist_id not in written_artist_ids_internal:
                                dim_artist: dict[str, Any] = {
                                    "artist_id_internal": internal_artist_id,
                                    "primary_name_observed": original_artist_text,
                                    "mb_id": internal_artist_id
                                }
                                output_files['dim_artists_master.jsonl'].write(
                                    json.dumps(dim_artist) + '\n')
                                written_artist_ids_internal.add(
                                    internal_artist_id)
                                artists_created += 1

                            if original_artist_text and (internal_artist_id, original_artist_text) not in written_artist_id_name_pairs:
                                bridge_artist_name: dict[str, Any] = {
                                    "artist_id_internal": internal_artist_id,
                                    "observed_name_string": original_artist_text
                                }
                                output_files['bridge_artist_id_to_names.jsonl'].write(
                                    json.dumps(bridge_artist_name) + '\n')
                                written_artist_id_name_pairs.add(
                                    (internal_artist_id, original_artist_text))
                            processed_artist_internals_for_this_play.append(
                                internal_artist_id)
                    elif original_artist_text:
                        internal_artist_id = generate_internal_id(
                            "artist", [original_artist_text])
                        if internal_artist_id not in written_artist_ids_internal:
                            dim_artist = {
                                "artist_id_internal": internal_artist_id,
                                "primary_name_observed": original_artist_text,
                                "mb_id": None
                            }
                            output_files['dim_artists_master.jsonl'].write(
                                json.dumps(dim_artist) + '\n')
                            written_artist_ids_internal.add(internal_artist_id)
                            artists_created += 1

                        # Check name pair before adding
                        if (internal_artist_id, original_artist_text) not in written_artist_id_name_pairs:
                            bridge_artist_name = {
                                "artist_id_internal": internal_artist_id,
                                "observed_name_string": original_artist_text
                            }
                            output_files['bridge_artist_id_to_names.jsonl'].write(
                                json.dumps(bridge_artist_name) + '\n')
                            written_artist_id_name_pairs.add(
                                (internal_artist_id, original_artist_text))
                        processed_artist_internals_for_this_play.append(
                            internal_artist_id)

                    _label_ids_raw = get_safe(raw_play, 'label_ids', [])
                    mb_label_ids_from_play: list[str] = _label_ids_raw if isinstance(
                        _label_ids_raw, list) else []

                    _label_names_raw = get_safe(raw_play, 'labels', [])
                    original_label_names_from_play: list[str] = _label_names_raw if isinstance(
                        _label_names_raw, list) else []
                    processed_label_internals_for_this_play: list[str] = []

                    if mb_label_ids_from_play:
                        for i, mb_label_id_val in enumerate(mb_label_ids_from_play):
                            internal_label_id = str(mb_label_id_val)
                            label_name: str | None = original_label_names_from_play[i] if i < len(
                                original_label_names_from_play) else "N/A"  # Ensure index exists

                            if internal_label_id not in written_label_ids_internal:
                                dim_label: dict[str, Any] = {
                                    "label_id_internal": internal_label_id,
                                    "primary_name_observed": label_name,
                                    "mb_id": internal_label_id
                                }
                                output_files['dim_labels_master.jsonl'].write(
                                    json.dumps(dim_label) + '\n')
                                written_label_ids_internal.add(
                                    internal_label_id)
                                labels_created += 1

                            if label_name and label_name != "N/A" and (internal_label_id, label_name) not in written_label_id_name_pairs:
                                bridge_label_name: dict[str, Any] = {
                                    "label_id_internal": internal_label_id,
                                    "observed_label_name_string": label_name
                                }
                                output_files['bridge_label_id_to_names.jsonl'].write(
                                    json.dumps(bridge_label_name) + '\n')
                                written_label_id_name_pairs.add(
                                    (internal_label_id, label_name))
                            processed_label_internals_for_this_play.append(
                                internal_label_id)

                    # Process remaining names if labels array was longer or no MBIDs
                    start_index_for_name_only_labels = len(
                        mb_label_ids_from_play)
                    if original_label_names_from_play:
                        for i in range(start_index_for_name_only_labels, len(original_label_names_from_play)):
                            label_name = original_label_names_from_play[i]
                            if not label_name:
                                continue

                            internal_label_id = generate_internal_id(
                                "label", [label_name])
                            if internal_label_id not in written_label_ids_internal:
                                dim_label = {
                                    "label_id_internal": internal_label_id,
                                    "primary_name_observed": label_name,
                                    "mb_id": None
                                }
                                output_files['dim_labels_master.jsonl'].write(
                                    json.dumps(dim_label) + '\n')
                                written_label_ids_internal.add(
                                    internal_label_id)
                                labels_created += 1

                            # Check name pair before adding
                            if (internal_label_id, label_name) not in written_label_id_name_pairs:
                                bridge_label_name = {
                                    "label_id_internal": internal_label_id,
                                    "observed_label_name_string": label_name
                                }
                                output_files['bridge_label_id_to_names.jsonl'].write(
                                    json.dumps(bridge_label_name) + '\n')
                                written_label_id_name_pairs.add(
                                    (internal_label_id, label_name))
                            if internal_label_id not in processed_label_internals_for_this_play:
                                processed_label_internals_for_this_play.append(
                                    internal_label_id)

                    fact_play: dict[str, Any] = {
                        "play_id": play_id,
                        "airdate_iso": to_utc_iso(get_safe(raw_play, 'airdate')),
                        "show_id": get_safe(raw_play, 'show'),
                        "track_id_internal": internal_track_id,
                        "comment": get_safe(raw_play, 'comment'),
                        "rotation_status": get_safe(raw_play, 'rotation_status'),
                        "is_local": get_safe(raw_play, 'is_local'),
                        "is_request": get_safe(raw_play, 'is_request'),
                        "is_live": get_safe(raw_play, 'is_live'),
                        # Storing original play_type
                        "play_type": get_safe(raw_play, 'play_type'),
                        "original_artist_text": original_artist_text,
                        "original_album_text": original_album_text,
                        "original_song_text": original_song_text
                    }
                    output_files['fact_plays.jsonl'].write(
                        json.dumps(fact_play) + '\n')

                    for art_id_internal in processed_artist_internals_for_this_play:
                        bridge_play_artist: dict[str, Any] = {
                            "play_id": play_id, "artist_id_internal": art_id_internal}
                        output_files['bridge_play_to_artist.jsonl'].write(
                            json.dumps(bridge_play_artist) + '\n')

                    for lbl_id_internal in processed_label_internals_for_this_play:
                        bridge_play_label: dict[str, Any] = {
                            "play_id": play_id, "label_id_internal": lbl_id_internal}
                        output_files['bridge_play_to_label.jsonl'].write(
                            json.dumps(bridge_play_label) + '\n')

                    # Update progress bar with current stats
                    pbar.set_postfix({
                        "tracks": tracks_created,
                        "artists": artists_created,
                        "releases": releases_created,
                        "labels": labels_created,
                        "skipped": skipped_airbreaks
                    })

                logger.info(
                    f"✅ Completed processing plays: {tracks_created:,} tracks, {artists_created:,} artists, {releases_created:,} releases, {labels_created:,} labels")
        else:
            logger.error(
                f"File {play_file_path} not found. This is a critical file.")

    except Exception as e:
        logger.error(f"An error occurred during normalization: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        print("📁 Closing output files...")
        for fname, fhandle in tqdm(output_files.items(), desc="Closing files", unit="file"):
            if fhandle and not fhandle.closed:
                fhandle.close()
                logger.debug(f"Closed {fname}.")
        logger.info("Normalization process finished.")

    # Validate output files
    print("🔍 Validating output files...")
    output_dir = Path(NORMALIZED_DIR)
    required_output_files = ["fact_plays.jsonl",
                             "dim_artists_master.jsonl", "dim_tracks.jsonl"]
    missing_outputs = []

    validation_progress = tqdm(
        required_output_files, desc="Validating files", unit="file")
    for file_name in validation_progress:
        file_path = output_dir / file_name
        if not file_path.exists() or os.path.getsize(file_path) == 0:
            missing_outputs.append(file_name)
            validation_progress.set_postfix(
                {"status": f"❌ {file_name} missing"})
        else:
            file_size = os.path.getsize(file_path)
            validation_progress.set_postfix(
                {"status": f"✅ {file_name} ({file_size:,} bytes)"})

    if missing_outputs:
        logger.error(
            f"Required output files are missing or empty: {', '.join(missing_outputs)}")
        return False

    logger.info("✅ Normalization completed successfully.")
    return True


if __name__ == '__main__':
    success = normalize_data()
    exit(0 if success else 1)
