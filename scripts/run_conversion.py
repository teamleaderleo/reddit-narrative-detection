import json
import csv
import os
import multiprocessing
import time

# --- Define Our Project Paths ---
# This automatically finds the root of our project.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")


# The function now accepts three separate arguments instead of one tuple.
def jsonl_to_csv(jsonl_filename, csv_filename, fields):
    """
    Worker function that processes a single file.
    """
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

    print(">>> Preparing to process files in parallel... <<< \n")

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

    tasks = [
        ("r_politics_comments.jsonl", "politics_comments_cleaned.csv", comment_fields),
        (
            "r_conservative_comments.jsonl",
            "conservative_comments_cleaned.csv",
            comment_fields,
        ),
        ("r_politics_posts.jsonl", "politics_posts_cleaned.csv", post_fields),
        ("r_conservative_posts.jsonl", "conservative_posts_cleaned.csv", post_fields),
    ]

    # --- Create a Pool of worker processes ---
    # This will create as many processes as we have CPU cores, up to the number of tasks.
    with multiprocessing.Pool() as pool:
        # starmap will unpack each task tuple into the 3 arguments
        # that our redesigned correctly accepts.
        results = pool.starmap(jsonl_to_csv, tasks)

    print("\n>>> ALL TASKS FINISHED. <<<")
    print("Results:", results)
    print("The cleaned CSV files are in the 'data/processed' folder.")
