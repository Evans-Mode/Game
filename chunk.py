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
