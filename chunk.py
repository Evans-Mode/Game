#!/usr/bin/env python3

import argparse
import csv
import os
import sys
import sqlite3
from typing import List


def connect(db_path):
    return sqlite3.connect(db_path)


SELECT_COLUMNS = [
    "AppID",
    "Release date",
    "Estimated owners",
    "Price",
    "User score",
    "Score rank",
    "Genres",
]


def split_csv(
    input_path: str = "data/games.csv",
    output_prefix: str = "games_chunk",
    chunk_size: int = 5000,
    selected_columns: List[str] = SELECT_COLUMNS,
) -> None:
    """Read file and verify coulmns, then split into chunks of specified size with selected columns."""
    csvCounter = 0
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

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


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Extract selected columns from a games CSV and split into chunks."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default=os.path.join("data", "games.csv"),
    )
    parser.add_argument(
        "--output-prefix",
        "-o",
        default=os.path.join("data", "games_chunk"),
    )
    parser.add_argument(
        "--chunk-size",
        "-n",
        type=int,
        default=5000,
    )
    args = parser.parse_args(argv)

    split_csv(args.input, args.output_prefix, args.chunk_size)


if __name__ == "__main__":
    main()
