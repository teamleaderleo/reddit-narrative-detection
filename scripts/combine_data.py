import pandas as pd
import os
import time

# --- Define Our Project Paths ---
# This automatically finds the root of our project.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
COMBINED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'combined')

def combine_csv_files(file1_name, file2_name, output_filename, file_type):
    """
    Reads two CSV files, combines them, cleans the link_id if it's a comments file,
    and saves the result.
    """
    input_path1 = os.path.join(PROCESSED_DATA_DIR, file1_name)
    input_path2 = os.path.join(PROCESSED_DATA_DIR, file2_name)
    output_path = os.path.join(COMBINED_DATA_DIR, output_filename)

    # --- Check if input files exist ---
    if not os.path.exists(input_path1) or not os.path.exists(input_path2):
        print(f"!!! ERROR: Could not find one or both input files: {file1_name}, {file2_name}. Skipping.")
        return

    print(f"--- Combining {file1_name} and {file2_name} ---")
    start_time = time.time()

    # Ensure the output directory exists
    os.makedirs(COMBINED_DATA_DIR, exist_ok=True)

    # Read the two CSV files into pandas DataFrames
    df1 = pd.read_csv(input_path1, low_memory=False)
    df2 = pd.read_csv(input_path2, low_memory=False)

    # Concatenate (union/stack) the two DataFrames
    combined_df = pd.concat([df1, df2], ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    # index=False is important to prevent pandas from writing an extra column

    # If this is a comments file, strip the 't3_' prefix from the link_id.
    # If it has a parent, strip the t1_
    if file_type == "comments":
        print("    -> Detected comments file. Cleaning ID prefixes...")

        # Clean link_id (e.g., 't3_123' -> '123')
        combined_df["link_id"] = (
            combined_df["link_id"].astype(str).str.replace("t3_", "", n=1)
        )

        # Clean parent_id (e.g., 't1_456' -> '456' OR 't3_789' -> '789')
        # This handles replies to comments AND replies directly to posts.
        combined_df["parent_id"] = (
            combined_df["parent_id"].astype(str).str.replace(r"t[13]_", "", regex=True)
        )

        print("    -> 'link_id' and 'parent_id' columns cleaned.")

    combined_df.to_csv(output_path, index=False)

    duration = time.time() - start_time
    print(f"--- DONE. Created {output_filename} with {len(combined_df):,} total rows in {duration:.2f} seconds. ---\n")


if __name__ == "__main__":
    print(">>> Starting data combination process... <<<\n")

    # Combine the two comment files
    combine_csv_files(
        "politics_comments_cleaned.csv",
        "conservative_comments_cleaned.csv",
        "all_comments.csv",
        "comments",  # We pass a 'type' so the function knows to clean it
    )

    # Combine the two post files
    combine_csv_files(
        "politics_posts_cleaned.csv",
        "conservative_posts_cleaned.csv",
        "all_posts.csv",
        "posts",  # This is not 'comments', so it will be skipped
    )

    print(">>> ALL FILES COMBINED. <<<")
    print("The final 'all_comments.csv' and 'all_posts.csv' are in the 'data/combined' folder.")
