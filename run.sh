#!/bin/bash
echo "Running vChunk..."
echo "...RUNNINsG GAMEDB.SQL..."
python gamedb.sql
echo "...RUNNING PLAYER_TO_GENRE.PY..."
python player_to_genre.py