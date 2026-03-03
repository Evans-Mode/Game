.mode csv
.header on
.import ../data/gamss_chunked.csv players

DROP TABLE IF EXISTS game_data;
CREATE TABLE game_data (
    AppID INTEGER PRIMARY KEY,
    Name TEXT,
    ReleaseDate TEXT,
    Owner INTEGER,
    Price REAL,
    UserScore REAL,
    ScoreRank INTEGER,
    Genre TEXT,
);

.import --csv --skip1 games.csv games_raw
select * from games_raw limit 5;