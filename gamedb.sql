import sqlite3
import csv
from chunk import split_csv


conn = sqlite3.connect("gamedb.db")
cursor = conn.cursor()
cursor.execute("drop table if exists games")
cursor.execute("""create table if not exists games (
    id integer primary key autoincrement,
    AppID integer,
    Name text,
    Release text,
    Estimated_owners text,
    Price integer,
    User_score integer,
    Score_rank integer,
    Genres text
)""");

for i in range(1, 26):
    with open(f"data/games_chunk_{i}.csv", newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            cursor.execute("""insert into games (AppID, Name, Release, Estimated_owners, Price, User_score, Score_rank, Genres) 
            values (?, ?, ?, ?, ?, ?, ?, ?)""", row);
conn.commit()
conn.close()
