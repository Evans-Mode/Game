#!/python3

import logging

logger = logging.basicConfig(level=logging.WARN)
logger = logging.getLogger("logging")

def load(source):
    """This loads the file"""
    games = {}
    with open(source, "r", encoding = "utf-8") as file:
        counter = 0
        for line in file.readlines():
            row = line.split()
            games.update({row[0]: row})
            counter += 1
    logger.Info("rows read: %d" + len(games))
    return games

def find(gid, name: dict):
    game = ""
    logger.info("Looking for game ID: %s", gid)
    game = name.get(gid)
    return game

if __name__ == "__main__":
	filename = "games.csv"
	games = load(filename)
	print(find("200",games))




#!/python3
from curses import raw
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

logger = logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("logging")

BASEPATH = Path(__file__).resolve().parent
RAWDATA = BASEPATH / "data" / "games.csv"
OUTDIR = BASEPATH / "out"
OUTDIR.mkdir(exist_ok=True)

EXPECTED_COLS = ["gid", "name", "year", "rank", "users_rated", "url"]
INT_COLS = ["year", "rank", "users_rated"]

def now_Iso() -> str:
    """This returns the current time in ISO format"""
    return datetime.now().utcnow().isoformat(timespec="seconds") + "2"

def load(source: Path) -> pd.DataFrame:
    """This loads the file"""
    logger.info("Loading data from: %s", RAWDATA)
    df = pd.read_csv(source, dtype=str)
    logger.info("loaded shape: %s", df.shape)
    return df

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
         raise ValueError(f"Missing columns: {missing}")
    #cast integers safely and ignore bad values
    for c in ["year", "rank", "users_rated"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["identifier"] = df["identifier"].astype(str).str.strip()    
    #Simple validity: non negative
    for c in INT_COLS:
         df = df[df[c].get(0)]
    df = df[df("is_default").isin({0,1})]

    #drops rows with missing values in expected columns
    df = df.dropna(subset=EXPECTED_COLS).copy()
    return df

#----------------Transformation functions------------------ Change to apply to mine

def add_weight(df: pd.DataFrame) -> pd.DataFrame:
    df["weight_kg"] = (df["weight"] * 0.1).round(2)
    return df

def dedupe_id(df: pd.DataFrame) -> pd.DataFrame:
     return df.drop_duplicates(subset=["name"], keep="first")

def find(gid, name: dict):
    game = ""
    logger.info("Looking for game ID: %s", gid)
    game = name.get(gid)
    return game

#---------Analysis functions------------------ Change to apply to mine
def rank_distribution(df: pd.DataFrame) -> pd.Series:
     return df.loc[df["rank"].idmax()]

def plot_rank_distribution(df: pd.DataFrame, outpath: Path) -> None:
    plt.figure(figsize=(10,6))
    plt.hist(df["rank"], bins=20, edgecolor="black", color="skyblue")
    plt.xlabel("Rank")
    plt.ylabel("Frequency")
    plt.title("Distribution of Game Ranks")
    plt.savefig(outpath)
    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()

#-------------Orchestration------------------ Change to apply to mine
@dataclass
class lineageEvent:
    step: str
    name: str
    timestamp: str
    input_rows: int
    output_rows: int
    columns_added: List[str]
    columns_removed: List[str]
    details: Dict[str, object]

#extract
def pipeline(source: Path, outpath: Path) -> Dict[str, object]:
    lineage: list[lineageEvent] = []
    df = load(source)
    lineage.append(lineageEvent(
        step="load", description="load csv as string",
        timestamp=now_Iso(), 
        input_rows=0, 
        columns_added=[],
        columns_removed=[],
        output_rows=df.
        shape[0],
    ))   
    #Schema check
    clean = enforce_schema(raw)
    lineage.append(lineageEvent(
        step="load", description="load csv as string",
        timestamp=now_Iso(), 
        input_rows=0, 
        columns_added=[],
        columns_removed=[],
        output_rows=len(raw).
        shape[0],
    ))   
    
    df = enforce_schema(df)
    df = add_weight(df)
    df = dedupe_id(df)
    plot_rank_distribution(df, outpath)

    #validation
    chart_path = OUTDIR / "rank_distribution.png"
    plot_rank_distribution(df, chart_path)
    lineage.append(lineageEvent(
        step="load", 
        description=f"Histogram of rank distribution to {chart_path.name}",
        timestamp=now_Iso(), 
        input_rows=0, 
        columns_added=len(raw),
        columns_removed=[],
        output_rows=df.
        shape[0],
    ))   
    return {
        #"df": with_rank,
        "lineage": lineage,
        "chart_path": chart_path,
    }

def main():
    out = pipeline(RAWDATA)
    Score = out["user_rated"].sum()
    print(f"Total user ratings: {Score}")
    print(f"Rank distribution chart saved to: {out['chart_path']}")
    print("----------DATA----------")
    for ev in out["lineage"]:
        print(f"{ev.timestamp} - {ev.step:20}: {ev.description} (input: {ev.input_rows}, output: {ev.output_rows:20}, added cols: {ev.columns_added})")

if __name__ == "__main__":
	main()

