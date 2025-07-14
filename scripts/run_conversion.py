import json
import csv
import os
import multiprocessing
import time

# --- Define Our Project Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")


def jsonl_to_csv(task_details):
    """
    Worker function that processes a single file.
    It now accepts a single tuple with all its arguments.
    """
    jsonl_filename, csv_filename, fields = task_details

    # Construct the full paths to the input and output files
    input_path = os.path.join(RAW_DATA_DIR, jsonl_filename)
    output_path = os.path.join(PROCESSED_DATA_DIR, csv_filename)

    # --- Check if the input file exists ---
    if not os.path.exists(input_path):
        print(f"!!! [{jsonl_filename}] SKIPPING: Cannot find file -> {input_path}")
        return f"SKIPPED: {jsonl_filename}"

    print(f"--- [{jsonl_filename}] STARTING conversion ---")
    start_time = time.time()

    # Ensure the output directory exists
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    line_count = 0
    with open(input_path, "r", encoding="utf-8") as f_in, open(
        output_path, "w", encoding="utf-8", newline=""
    ) as f_out:

        writer = csv.DictWriter(f_out, fieldnames=fields)
        writer.writeheader()

        for line in f_in:
            line_count += 1
            try:
                record = json.loads(line)
                row = {field: record.get(field, "") for field in fields}
                writer.writerow(row)
            except json.JSONDecodeError:
                continue

    end_time = time.time()
    duration = end_time - start_time
    print(
        f"--- [{jsonl_filename}] DONE. Processed {line_count:,} lines in {duration:.2f} seconds. ---"
    )
    return f"COMPLETED: {jsonl_filename}"


# This is a standard Python guard for multiprocessing.
# It ensures the main code runs only once, not in the child processes.
if __name__ == "__main__":

    print(">>> Preparing to process 4 files in parallel... <<< \n")

    # --- Fields we care about ---
    comment_fields = [
        "id",
        "author",
        "body",
        "score",
        "created_utc",
        "subreddit",
        "link_id",
        "parent_id",
    ]
    post_fields = [
        "id",
        "author",
        "title",
        "selftext",
        "score",
        "created_utc",
        "subreddit",
        "num_comments",
        "url",
        "permalink",
    ]

    # --- List of all tasks to be done ---
    # Each item is a tuple containing the arguments for our worker function.
    tasks = [
        ("politics_comments.jsonl", "politics_comments_cleaned.csv", comment_fields),
        (
            "conservative_comments.jsonl",
            "conservative_comments_cleaned.csv",
            comment_fields,
        ),
        ("politics_submissions.jsonl", "politics_submissions_cleaned.csv", post_fields),
        (
            "conservative_submissions.jsonl",
            "conservative_submissions_cleaned.csv",
            post_fields,
        ),
    ]

    # --- Create a Pool of worker processes ---
    # This will create as many processes as you have CPU cores, up to the number of tasks.
    with multiprocessing.Pool() as pool:
        # The 'starmap' function takes our worker function and the list of tasks,
        # and runs them all in parallel across the process pool.
        results = pool.starmap(jsonl_to_csv, tasks)

    print("\n>>> ALL TASKS FINISHED. <<<")
    print("Results:", results)
    print("The cleaned CSV files are in the 'data/processed' folder.")
