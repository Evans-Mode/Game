#!/python3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

logger = logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("games_logger")

BASEPATH = Path(__file__).resolve().parent
RAWDATA = BASEPATH / "data" / "games.csv"
OUTDIR = BASEPATH / "out"
OUTDIR.mkdir(exist_ok=True)

EXPECTED_COLS = ["Appid", "Release date", "rank", "Estimated owners", "Price", "User score", "Score rank" "Genres"]
INT_COLS = ["Appid", "Release date", "rank", "User score", "Estimated owners", "Price", "Score rank"]

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
    for c in ["Appid", "year", "rank", "User score"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["identifier"] = df["identifier"].astype(str).str.strip()    
    #Simple validity: non negative
    # for c in INT_COLS:
    #      df = df[df[c].get(0)]
    # df = df[df("is_default").isin({0,1})]

    #drops rows with missing values in expected columns
    df = df.dropna(subset=EXPECTED_COLS).copy()
    return df

#----------------Transformation functions------------------ Change to apply to mine

def avg_score(df: pd.DataFrame) -> pd.DataFrame:
    df["User score"] = (df["User score"] * 0.1).round(2)
    return df

def dedupe_id(df: pd.DataFrame) -> pd.DataFrame:
     return df.drop_duplicates(subset=["name"], keep="first")

def find(Appid, name: dict):
    game = ""
    logger.info("Looking for game ID: %s", Appid)
    game = name.get(Appid)
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
    description: str
    timestamp: str
    input_rows: int
    output_rows: int
    columns_added: List[str]
    columns_removed: List[str]


def pipeline(source: Path, outpath: Path) -> Dict[str, object]:
    lineage: list[lineageEvent] = []

    # Extract
    raw = load(source)
    lineage.append(lineageEvent(
        step="extract",
        description="Loading CSV as string",
        timestamp=now_Iso(),
        input_rows=0,
        output_rows=len(raw),
        columns_added=[],
        columns_removed=[]
    )) 
     # Schema checks
    clean = enforce_schema(raw)
    lineage.append(lineageEvent(
        step="validate",
        description="Enforce schema",
        timestamp=now_Iso(),
        input_rows=len(raw),
        output_rows=len(clean),
        columns_added=[],
        columns_removed=[]
    ))

    # Dedupe
    deduped = dedupe_id(clean)
    lineage.append(lineageEvent(
        step="dedupe",
        description="Dropped duplicates by name",
        timestamp=now_Iso(),
        input_rows=len(clean),
        output_rows=len(deduped),
        columns_added=[],
        columns_removed=[]
    ))

    # Derived Column
    score = avg_score(deduped)
    lineage.append(lineageEvent(
        step="derive avg_score",
        description="Added avg_score (avg_score).round(2)",
        timestamp=now_Iso(),
        input_rows=len(deduped),
        output_rows=len(score),
        columns_added=["avg_score"],
        columns_removed=[]
    ))

    # Analytics
    distribution = rank_distribution(score)
    lineage.append(lineageEvent(
        step="analytics",
        description="Calculated rank distribution",
        timestamp=now_Iso(),
        input_rows=len(score),
        output_rows=1,
        columns_added=[],
        columns_removed=[]
    ))

    #visualization
    chart_path = OUTDIR / "rank_distribution.png"
    plot_rank_distribution(raw, chart_path)
    lineage.append(lineageEvent(
        step="viz",
        description="Histogram of avg_score saved to {chart_path.name}",
        timestamp=now_Iso(),
        input_rows=len(score),
        output_rows=len(score),
        columns_added=[],
        columns_removed=[]
    ))
    return {
        #"df": with_rank,
        "lineage": lineage,
        "chart_path": chart_path,
    }

def main():
    out = pipeline(RAWDATA, OUTDIR)
    Score = out["user_rated"].sum()
    print(f"Total user ratings: {Score}")
    print(f"Rank distribution chart saved to: {out['chart_path']}")
    print("----------DATA----------")
    for ev in out["lineage"]:
        print(f"{ev.timestamp} - {ev.step:20}: {ev.description} (input: {ev.input_rows}, output: {ev.output_rows:20}, added cols: {ev.columns_added})")

if __name__ == "__main__":
	main()

