import requests
import json
import time
import os
import logging
import threading
import sys
import typing  # Retained for typing.Any

# --- Type Aliases ---
EndpointName = str
EndpointConfig = dict[str, str | int | None]
ApiItem = dict[str, typing.Any]
ItemId = str | int

# --- Global state for progress ---
progress_lock = threading.Lock()
total_items_saved_globally: int = 0
endpoint_statuses: dict[EndpointName, str] = {}
ALL_ENDPOINTS_CONFIG: list[EndpointConfig] = []

# Class to hold state for redraw_progress_display


class ProgressDisplayState:
    lines_printed_count: int = 0


progress_display_state = ProgressDisplayState()

# --- Configuration ---
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
REQUEST_DELAY_SECONDS = 0.15


def count_lines_in_file(filepath: str) -> int:
    """Counts the number of lines in a file."""
    if not os.path.exists(filepath):
        return 0
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except Exception as e:
        logging.warning(
            f"Error reading line count from {filepath}: {e}. Assuming 0 lines.")
        return 0


def redraw_progress_display():
    """Redraws the multi-line progress display for all endpoints."""
    with progress_lock:
        if progress_display_state.lines_printed_count > 0:
            sys.stdout.write(
                f"\033[{progress_display_state.lines_printed_count}A")

        current_lines_written_this_call = 0
        for cfg in ALL_ENDPOINTS_CONFIG:
            endpoint_name_ordered = str(cfg["name"])
            display_str = endpoint_statuses.get(
                endpoint_name_ordered, f"{endpoint_name_ordered}: Error - status not found")
            sys.stdout.write(f"\033[K{display_str}\n")
            current_lines_written_this_call += 1

        progress_display_state.lines_printed_count = current_lines_written_this_call
        sys.stdout.flush()


def get_item_id(item: ApiItem) -> ItemId | None:
    """
    Extracts a unique ID from an API item.
    Primarily looks for 'id'. Can be extended to look for 'uri' or other fields if necessary.
    """
    if 'id' in item and item['id'] is not None:
        return item['id']
    return None


def download_endpoint_data(config: EndpointConfig):
    """
    Downloads new data for a single configured API endpoint until an existing item is found.
    New items are prepended to the existing data file.
    """
    base_url = str(config["base_url"])
    output_file = str(config["output_file"])
    endpoint_name = str(config["name"])

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    existing_ids: set[ItemId] = set()
    original_items: list[ApiItem] = []
    file_existed_and_was_readable = False

    if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
        try:
            with open(output_file, 'r', encoding='utf-8') as f_in:
                for line_number, line in enumerate(f_in, 1):
                    try:
                        item: ApiItem = json.loads(line)
                        original_items.append(item)
                        item_id = get_item_id(item)
                        if item_id is not None:
                            existing_ids.add(item_id)
                        else:
                            logging.debug(
                                f"'{endpoint_name}': No ID for item in {output_file} line {line_number}. Item: {str(line)[:100]}")
                    except json.JSONDecodeError:
                        logging.warning(
                            f"'{endpoint_name}': Skipping corrupt JSON line in {output_file} at line {line_number}: {line.strip()}")
            file_existed_and_was_readable = True
            status_msg = f"{endpoint_name}: Found {len(existing_ids)} existing unique IDs ({len(original_items)} items) in {output_file}."
        except IOError as e:
            status_msg = f"{endpoint_name}: IOError reading {output_file}: {e}. Will fetch all."
            logging.error(status_msg)
            existing_ids.clear()
            original_items.clear()
        except Exception as e:
            status_msg = f"{endpoint_name}: Error reading {output_file}: {e}. Will fetch all."
            logging.error(status_msg)
            existing_ids.clear()
            original_items.clear()
    else:
        status_msg = f"{endpoint_name}: No existing data or file empty. Starting fresh."

    with progress_lock:
        endpoint_statuses[endpoint_name] = status_msg
    redraw_progress_display()

    initial_limit = 5000 if endpoint_name == "plays" else 200
    initial_request_params: dict[str, str | int | None] = {
        'limit': initial_limit, 'format': 'json', 'offset': None}

    current_url: str | None = base_url
    active_params_for_current_request: dict[str, str |
                                            int | None] | None = initial_request_params
    is_first_request_attempt = True

    session_new_items: list[ApiItem] = []
    stop_fetching_for_this_endpoint = False

    try:
        while current_url and not stop_fetching_for_this_endpoint:
            with progress_lock:
                endpoint_statuses[endpoint_name] = (
                    f"{endpoint_name}: Fetching page. "
                    f"New items this session: {len(session_new_items)} (Known existing: {len(existing_ids)})."
                )
            redraw_progress_display()

            retries = 0
            response = None
            current_request_url_for_log = current_url if not is_first_request_attempt else base_url
            current_params_for_log = active_params_for_current_request if is_first_request_attempt else None

            while retries < MAX_RETRIES:
                try:
                    response = requests.get(
                        current_url, params=active_params_for_current_request, timeout=30)  # type: ignore
                    response.raise_for_status()
                    break
                except requests.exceptions.Timeout:
                    retries += 1
                    logging.warning(
                        f"Timeout for '{endpoint_name}'. URL: {current_request_url_for_log}, Params: {current_params_for_log}. Retry {retries}/{MAX_RETRIES}...")
                except requests.exceptions.RequestException as e:
                    retries += 1
                    logging.warning(
                        f"Error for '{endpoint_name}'. URL: {current_request_url_for_log}, Params: {current_params_for_log}: {e}. Retry {retries}/{MAX_RETRIES}...")

                if retries >= MAX_RETRIES:
                    logging.error(
                        f"Failed for '{endpoint_name}' after {MAX_RETRIES} retries. URL: {current_request_url_for_log}, Params: {current_params_for_log}. Skipping.")
                    current_url = None
                    break
                time.sleep(RETRY_DELAY_SECONDS)

            if is_first_request_attempt:
                is_first_request_attempt = False
                active_params_for_current_request = None

            if response is None or response.status_code != 200:
                if current_url:
                    logging.error(
                        f"Stopping for '{endpoint_name}' due to fetch error or non-200 response on {current_request_url_for_log}.")
                break

            try:
                data: ApiItem = response.json()
            except json.JSONDecodeError as e:
                preview = response.text[:200] if response else "No response object"
                logging.error(
                    f"Error decoding JSON for '{endpoint_name}' from {current_request_url_for_log}: {e}. Preview: '{preview}...'. Skipping page.")
                with progress_lock:
                    endpoint_statuses[endpoint_name] = f"{endpoint_name}: JSON Decode Error - Skipping page."
                redraw_progress_display()
                current_url = None
                break

            results: list[ApiItem] | None = data.get('results', [])
            if not results and data.get('next') is None and not session_new_items and not existing_ids:
                logging.info(
                    f"'{endpoint_name}': No results on first page and no 'next' page. Endpoint may be empty.")

            page_new_items_count = 0
            # type: ignore
            # type: ignore
            for item_idx, item_from_results in enumerate(results):
                item: ApiItem = item_from_results  # type: ignore # Explicitly type item
                item_id = get_item_id(item)  # type: ignore

                if item_id is None:
                    logging.warning(
                        f"'{endpoint_name}': Fetched item #{item_idx} on page from {current_request_url_for_log} has no ID. Skipping. Item: {str(item)[:100]}")
                    continue

                if item_id in existing_ids:
                    logging.info(
                        f"'{endpoint_name}': Encountered known item ID {item_id} (from {output_file}). Stopping fetch for new items.")
                    stop_fetching_for_this_endpoint = True
                    break

                session_new_items.append(item)  # type: ignore
                page_new_items_count += 1

            if page_new_items_count > 0:
                with progress_lock:
                    endpoint_statuses[endpoint_name] = (
                        f"{endpoint_name}: Page fetched. New this page: {page_new_items_count}. "
                        f"Total new this session: {len(session_new_items)}."
                    )
                redraw_progress_display()

            if stop_fetching_for_this_endpoint:
                break

            current_url = data.get('next')
            if not current_url:
                logging.info(
                    f"'{endpoint_name}': No 'next' URL. Reached end of API after processing this page.")
                break
            time.sleep(REQUEST_DELAY_SECONDS)

    except Exception as e:
        logging.critical(
            f"An unexpected critical error occurred during download phase for '{endpoint_name}': {e}", exc_info=True)
        with progress_lock:
            endpoint_statuses[endpoint_name] = f"{endpoint_name}: CRITICAL ERROR during download - {e}"
        redraw_progress_display()
        session_new_items.clear()

    if session_new_items:
        logging.info(
            f"'{endpoint_name}': Found {len(session_new_items)} new items. Updating {output_file}.")
        combined_items = session_new_items + original_items
        temp_output_file = output_file + ".tmp"

        try:
            with open(temp_output_file, 'w', encoding='utf-8') as f_out:
                for item_to_write in combined_items:
                    f_out.write(json.dumps(item_to_write) + '\n')

            os.replace(temp_output_file, output_file)

            logging.info(
                f"'{endpoint_name}': Successfully wrote {len(combined_items)} items ({len(session_new_items)} new) to {output_file}.")
            with progress_lock:
                global total_items_saved_globally
                total_items_saved_globally += len(session_new_items)
                endpoint_statuses[endpoint_name] = (
                    f"{endpoint_name}: Done. Added {len(session_new_items)} new. "
                    f"File Total: {len(combined_items)}."
                )
        except IOError as e:
            logging.error(
                f"'{endpoint_name}': IOError during file update ({temp_output_file} or {output_file}): {e}. New data for this session may be lost.")
            with progress_lock:
                endpoint_statuses[endpoint_name] = f"{endpoint_name}: IOError on write - {e}"
            if os.path.exists(temp_output_file):
                try:
                    os.remove(temp_output_file)
                    logging.info(
                        f"'{endpoint_name}': Cleaned up temporary file {temp_output_file}.")
                except OSError as rm_e:
                    logging.error(
                        f"'{endpoint_name}': Failed to clean up temporary file {temp_output_file}: {rm_e}.")
        except Exception as e:
            logging.critical(
                f"'{endpoint_name}': Unexpected error during file update ({temp_output_file} to {output_file}): {e}", exc_info=True)
            with progress_lock:
                endpoint_statuses[endpoint_name] = f"{endpoint_name}: CRITICAL error on write - {e}"
            if os.path.exists(temp_output_file):
                try:
                    os.remove(temp_output_file)
                    logging.info(
                        f"'{endpoint_name}': Cleaned up temporary file {temp_output_file} after critical error.")
                except OSError as rm_e:
                    logging.error(
                        f"'{endpoint_name}': Failed to clean up temporary file {temp_output_file} after critical error: {rm_e}.")
    else:
        final_total_in_file = len(
            original_items) if file_existed_and_was_readable else count_lines_in_file(output_file)
        logging.info(
            f"'{endpoint_name}': No new items found for this session. Existing items in file: {final_total_in_file}.")
        with progress_lock:
            endpoint_statuses[endpoint_name] = (
                f"{endpoint_name}: Done. No new items. "
                f"File Total: {final_total_in_file}."
            )
    redraw_progress_display()


# --- Main Execution ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    data_directory = "data"
    os.makedirs(data_directory, exist_ok=True)

    ALL_ENDPOINTS_CONFIG.extend([
        {
            "name": "plays",
            "base_url": "https://api.kexp.org/v2/plays/",
            "output_file": os.path.join(data_directory, "kexp_plays.jsonl"),
            "offset": None
        },
        {
            "name": "shows",
            "base_url": "https://api.kexp.org/v2/shows/",
            "output_file": os.path.join(data_directory, "kexp_shows.jsonl"),
            "offset": None
        },
        {
            "name": "hosts",
            "base_url": "https://api.kexp.org/v2/hosts/",
            "output_file": os.path.join(data_directory, "kexp_hosts.jsonl"),
            "offset": None
        },
        {
            "name": "programs",
            "base_url": "https://api.kexp.org/v2/programs/",
            "output_file": os.path.join(data_directory, "kexp_programs.jsonl"),
            "offset": None
        },
        {
            "name": "timeslots",
            "base_url": "https://api.kexp.org/v2/timeslots/",
            "output_file": os.path.join(data_directory, "kexp_timeslots.jsonl"),
            "offset": None
        }
    ])

    progress_display_state.lines_printed_count = 0
    with progress_lock:
        for config_item in ALL_ENDPOINTS_CONFIG:
            endpoint_statuses[str(config_item["name"])
                              ] = f"{str(config_item['name'])}: Queued"
    redraw_progress_display()

    threads: list[threading.Thread] = []
    for config_item in ALL_ENDPOINTS_CONFIG:
        thread = threading.Thread(target=download_endpoint_data, args=(
            config_item,), name=f"{str(config_item['name'])}-downloader")
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    if progress_display_state.lines_printed_count > 0:
        pass

    sys.stdout.write('\n' * (progress_display_state.lines_printed_count + 1))
    sys.stdout.flush()

    logging.info(
        "All configured KEXP endpoint download attempts are complete.")
    logging.info(
        f"Final total new items saved across all endpoints this session: {total_items_saved_globally}")
