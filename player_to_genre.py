#!/usr/bin/env python3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3


BASEPATH = Path(__file__).resolve().parent
OUTDIR = BASEPATH / "out"
OUTDIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("genre_logger")
file_handler = logging.FileHandler(BASEPATH / "pipeline.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)


conn = sqlite3.connect("gamedb.db")
cursor = conn.cursor()

@dataclass
class PlayerGenreData:
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
    """Process data to genres and parse owners"""
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
    plt.title('Estimated Owners by Genre/Mode options')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.show()

@dataclass
class LineageEvent:
    extract_source: str = ""
    extract_rows: int = 0
    checks_performed: List[str] = field(default_factory=list)
    duplicates_found: int = 0
    analytics_summary: str = ""
    visualization_type: str = ""

    def extract(self) -> pd.DataFrame:
        """Extract data from database"""
        start_time = time.time()
        logger.info("Starting data extraction from database gamedb.db")
        self.extract_source = "gamedb.db"
        df = load_data()
        self.extract_rows = len(df)
        duration = time.time() - start_time
        logger.info(f"Completed data extraction: loaded {len(df)} rows in {duration:.2f} seconds")
        return df

    def checks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform checks and initial processing"""
        start_time = time.time()
        logger.info("Starting data checks and processing: filling NaN genres, parsing owners, filtering empty genres")
        self.checks_performed = ["fillna_genres", "parse_owners", "filter_empty_genres"]
        processed_df = process_data(df)
        duration = time.time() - start_time
        logger.info(f"Completed data checks: processed {len(processed_df)} rows in {duration:.2f} seconds")
        return processed_df

    def duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle duplicates based on Name and genres_list"""
        start_time = time.time()
        logger.info("Starting duplicate handling: removing duplicates based on Name and genres_list")
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['Name', 'genres_list'])
        self.duplicates_found = initial_rows - len(df)
        duration = time.time() - start_time
        logger.info(f"Completed duplicate handling: removed {self.duplicates_found} duplicates, {len(df)} rows remaining in {duration:.2f} seconds")
        return df

    def analytics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform analytics aggregation"""
        start_time = time.time()
        logger.info("Starting analytics: aggregating estimated owners by genre")
        agg_df = aggregate_by_genre(df)
        self.analytics_summary = f"Aggregated {len(agg_df)} genres by total estimated owners"
        duration = time.time() - start_time
        logger.info(f"Completed analytics: {self.analytics_summary} in {duration:.2f} seconds")
        return agg_df

    def visualization(self, agg_df: pd.DataFrame):
        """Create visualization"""
        start_time = time.time()
        logger.info("Starting visualization: creating bar chart of genres vs estimated owners")
        self.visualization_type = "bar_chart"
        output_path = OUTDIR / "genre_owners.png"
        plot_genre_owners(agg_df, output_path)
        duration = time.time() - start_time
        logger.info(f"Completed visualization: saved chart to {output_path} in {duration:.2f} seconds")
        print(f"Graph saved to {output_path}")
        


def main():
    start_time = time.time()
    logger.info("Starting player-to-genre data processing pipeline")
    lineage = LineageEvent()
    df = lineage.extract()
    processed_df = lineage.checks(df)
    deduped_df = lineage.duplicates(processed_df)
    agg_df = lineage.analytics(deduped_df)
    lineage.visualization(agg_df)
    total_duration = time.time() - start_time
    logger.info(f"Completed entire pipeline in {total_duration:.2f} seconds")

if __name__ == "__main__":
    main()