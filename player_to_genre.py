#!/usr/bin/env python3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import chunk
import sqlite3

logger = logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("genre_logger")

BASEPATH = Path(__file__).resolve().parent
OUTDIR = BASEPATH / "out"
OUTDIR.mkdir(exist_ok=True)

conn = sqlite3.connect("gamedb.db")
cursor = conn.cursor()

@dataclass
class GenreOwner:
    genre: str
    estimated_owners: int

def parse_estimated_owners(owners_str: str) -> int:
    """Parse estimated owners string like '10000 - 20000' to average"""
    if not owners_str or owners_str.strip() == "":
        return 0
    parts = owners_str.split('-')
    if len(parts) == 2:
        try:
            low = int(parts[0].strip())
            high = int(parts[1].strip())
            return (low + high) // 2
        except ValueError:
            return 0
    try:
        return int(owners_str.strip())
    except ValueError:
        return 0

def load_data() -> pd.DataFrame:
    """Load data from database"""
    cursor.execute("SELECT Name, Genres, `Estimated_owners` FROM games")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process data to explode genres and parse owners"""
    df = df.copy()
    df['Genres'] = df['Genres'].fillna('')
    df['genres_list'] = df['Genres'].str.split(',')
    df = df.explode('genres_list')
    df['genres_list'] = df['genres_list'].str.strip()
    df['estimated_owners'] = df['Estimated_owners'].apply(parse_estimated_owners)
    df = df[df['genres_list'] != '']
    return df

def aggregate_by_genre(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate estimated owners by genre"""
    agg_df = df.groupby('genres_list')['estimated_owners'].sum().reset_index()
    agg_df = agg_df.sort_values('estimated_owners', ascending=False)
    return agg_df

def plot_genre_owners(agg_df: pd.DataFrame, output_path: Path):
    """Plot bar chart of genres vs estimated owners"""
    plt.figure(figsize=(12, 8))
    plt.bar(agg_df['genres_list'], agg_df['estimated_owners'])
    plt.xlabel('Genre')
    plt.ylabel('Total Estimated Owners (millions)')
    plt.title('Estimated Owners by Genre')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_path)
    plt.show()

def main():
    df = load_data()
    processed_df = process_data(df)
    agg_df = aggregate_by_genre(processed_df)
    output_path = OUTDIR / "genre_owners.png"
    plot_genre_owners(agg_df, output_path)
    print(f"Graph saved to {output_path}")

if __name__ == "__main__":
    main()