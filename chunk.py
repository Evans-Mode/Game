#!/usr/bin/env python3
from dataclasses import dataclass
from datetime import datetime
import argparse
import csv
import os
import sys
import sqlite3
from typing import List

SELECT_COLUMNS = [
    "AppID",
    "Name",
    "Release date",
    "Estimated owners",
    "Price",
    "User score",
    "Score rank",
    "Genres",
]
@dataclass
class Game:
    app_id: int
    name: str
    release_date: str
    estimated_owners: str
    price: float
    user_score: int
    score_rank: int
    genres: str
def split_csv(
    input_path: str = "data/games.csv",
    output_prefix: str = "games_chunk",
    chunk_size: int = 5000,
    selected_columns: List[str] = SELECT_COLUMNS,
) -> int:
    """Read file and verify columns, then split into chunks of specified size with selected columns. Returns chunk count."""
    csvCounter = 0
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Always write chunks into the `data/` folder unless an explicit directory is provided.
    if not os.path.dirname(output_prefix):
        output_prefix = os.path.join("data", output_prefix)

    # ensure output directory exists
    output_dir = os.path.dirname(output_prefix)
    if output_dir and not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    with open(input_path, newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        # verify selected columns exist in source
        missing = [c for c in selected_columns if c not in reader.fieldnames]
        if missing:
            raise ValueError(
                f"The following requested columns are not present in the input: {missing}"
            )

        chunk_index = 1
        output_file = None
        writer = None
        rows_in_chunk = 0

        def open_new_chunk():
            nonlocal output_file, writer, rows_in_chunk, chunk_index
            if output_file:
                output_file.close()
            filename = f"{output_prefix}_{chunk_index}.csv"
            output_file = open(filename, "w", newline='', encoding='utf-8')
            writer = csv.DictWriter(output_file, fieldnames=selected_columns)
            writer.writeheader()
            rows_in_chunk = 0
            chunk_index += 1
            print(f"Creating {filename}")
            return writer

        # initialize first chunk
        writer = open_new_chunk()

        for row in reader:
            filtered = {col: row.get(col, '') for col in selected_columns}
            writer.writerow(filtered)
            rows_in_chunk += 1
            if rows_in_chunk >= chunk_size:
                writer = open_new_chunk()
                csvCounter += 1
        # close last chunk if open
        if output_file:
            output_file.close()
        
        return csvCounter + 1

def main():
    split_csv()


if __name__ == "__main__":
    main()
