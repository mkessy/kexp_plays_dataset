import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, IO

# --- Configuration ---
KEXP_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'kexp.org')
RAW_DIR = "data/"
NORMALIZED_DIR = "normalized_kexp_jsonl/"

# --- Helper Functions ---


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
        print(f"Warning: Could not parse date string: {date_str}")
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
            print(
                f"Warning: Date string {date_input} is not in expected YYYY-MM-DD format.")
            return None  # Or handle as per requirements
    return None

# Helper to get time as HH:MM:SS string or None


def format_time_to_str(time_input: Any) -> str | None:
    if isinstance(time_input, str):
        try:
            datetime.strptime(time_input, '%H:%M:%S')  # Validates HH:MM:SS
            return time_input
        except ValueError:
            print(
                f"Warning: Time string {time_input} is not in HH:MM:SS format.")
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
            print(
                f"Warning: Generating random UUID for entity `{entity_type}` due to missing identifier parts: {identifier_parts}")
            return str(uuid.uuid4())

    return str(uuid.uuid5(KEXP_NAMESPACE, f"{entity_type}:{identifier_string}"))


def normalize_data() -> None:
    print(
        f"Starting normalization. Raw data from: {RAW_DIR}, Normalized data to: {NORMALIZED_DIR}")

    if not os.path.exists(NORMALIZED_DIR):
        os.makedirs(NORMALIZED_DIR)
        print(f"Created directory: {NORMALIZED_DIR}")

    # --- Initialize all in-memory 'written_...' sets ---
    written_host_ids: set[int] = set()
    written_program_ids: set[int] = set()
    written_show_ids: set[int] = set()
    written_artist_ids_internal: set[str] = set()
    written_label_ids_internal: set[str] = set()
    written_release_ids_internal: set[str] = set()
    written_track_ids_internal: set[str] = set()
    written_artist_id_name_pairs: set[tuple[str, str]] = set()
    written_release_id_name_pairs: set[tuple[str, str]] = set()
    written_label_id_name_pairs: set[tuple[str, str]] = set()
    written_timeslot_ids: set[int] = set()

    # Using a dictionary to manage file handles
    output_files: dict[str, IO[Any]] = {}
    file_names: list[str] = [
        "dim_hosts.jsonl", "dim_programs.jsonl", "dim_shows.jsonl",
        "dim_artists_master.jsonl", "dim_labels_master.jsonl", "dim_releases_master.jsonl",
        "dim_tracks.jsonl", "bridge_artist_id_to_names.jsonl",
        "bridge_release_id_to_names.jsonl", "bridge_label_id_to_names.jsonl",
        "fact_plays.jsonl", "bridge_show_hosts.jsonl",
        "bridge_play_to_artist.jsonl", "bridge_play_to_label.jsonl",
        "bridge_timeslot_hosts.jsonl", "dim_timeslots.jsonl"
    ]

    try:
        for fname in file_names:
            output_files[fname] = open(os.path.join(
                NORMALIZED_DIR, fname), 'w', encoding='utf-8')
            print(f"Opened {fname} for writing.")

        # --- STAGE 1: Process Dimension-Rich Standalone Files ---
        # 1. Process kexp_hosts.jsonl -> dim_hosts.jsonl
        print("Processing hosts...")
        host_file_path = os.path.join(RAW_DIR, 'kexp_hosts.jsonl')
        if os.path.exists(host_file_path):
            with open(host_file_path, 'r', encoding='utf-8') as f:
                for line in f:
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
        else:
            print(f"Warning: {host_file_path} not found.")

        # 2. Process kexp_programs.jsonl -> dim_programs.jsonl
        print("Processing programs...")
        program_file_path = os.path.join(RAW_DIR, 'kexp_programs.jsonl')
        if os.path.exists(program_file_path):
            with open(program_file_path, 'r', encoding='utf-8') as f:
                for line in f:
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
        else:
            print(f"Warning: {program_file_path} not found.")

        # 3. Process kexp_timeslots.jsonl -> dim_timeslots.jsonl, bridge_timeslot_hosts.jsonl
        print("Processing timeslots...")
        timeslot_file_path = os.path.join(RAW_DIR, 'kexp_timeslots.jsonl')
        if os.path.exists(timeslot_file_path):
            with open(timeslot_file_path, 'r', encoding='utf-8') as f:
                for line in f:
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
                            # 'default_show_id' was in previous version, but not in typical KEXP timeslot. Removing for now.
                            # 'title' was in previous version, KEXP timeslots usually don't have own title, program's title is used.
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
        else:
            print(f"Warning: {timeslot_file_path} not found.")

        # --- STAGE 2: Process Shows (depends on Hosts, Programs) ---
        print("Processing shows...")
        show_file_path = os.path.join(RAW_DIR, 'kexp_shows.jsonl')
        if os.path.exists(show_file_path):
            with open(show_file_path, 'r', encoding='utf-8') as f:
                for line in f:
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
        else:
            print(f"Warning: {show_file_path} not found.")

        # --- STAGE 3: Process Plays ---
        print("Processing plays...")
        play_file_path = os.path.join(RAW_DIR, 'kexp_plays.jsonl')
        if os.path.exists(play_file_path):
            with open(play_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f):
                    if (line_num + 1) % 100000 == 0:
                        print(f"  Processed {line_num + 1} plays...")
                    try:
                        raw_play: dict[str, Any] = json.loads(line)
                    except json.JSONDecodeError as e:
                        print(
                            f"Warning: Skipping malformed JSON line in plays file: {line_num+1} - {e}")
                        continue

                    play_id = get_safe(raw_play, 'id')
                    if play_id is None:
                        print(
                            f"Warning: Play without ID found at line {line_num+1}, skipping.")
                        continue

                    # CRITICAL: Filter Out "Airbreak" Plays
                    if get_safe(raw_play, 'play_type') != 'trackplay':
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
        else:
            print(
                f"Warning: {play_file_path} not found. This is a critical file.")

    except Exception as e:
        print(f"An error occurred during normalization: {e}")
        import traceback
        traceback.print_exc()
    finally:
        for fname, fhandle in output_files.items():
            if fhandle and not fhandle.closed:
                fhandle.close()
                print(f"Closed {fname}.")
        print("Normalization process finished.")


if __name__ == '__main__':
    normalize_data()
    print("Script execution complete. Check the " +
          NORMALIZED_DIR + " directory.")
