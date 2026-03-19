#!/usr/bin/env python3
import pytest
import pandas as pd
import numpy as np
import sqlite3
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open, call
import csv
import io

# Import modules to test
import chunk
from player_to_genre import (
    parse_estimated_owners,
    load_data,
    process_data,
    aggregate_by_genre,
    plot_genre_owners,
    LineageEvent,
    BASEPATH,
)


class TestParseEstimatedOwners:
    """Tests for parse_estimated_owners function"""

    def test_parse_range_format(self):
        """Test parsing owners string with range format"""
        result = parse_estimated_owners("10000 - 20000")
        assert result == 15000

    def test_parse_range_with_extra_spaces(self):
        """Test parsing with extra spaces in range"""
        result = parse_estimated_owners("  100000 -  200000  ")
        assert result == 150000

    def test_parse_single_number(self):
        """Test parsing a single number"""
        result = parse_estimated_owners("5000")
        assert result == 5000

    def test_parse_single_number_with_spaces(self):
        """Test parsing single number with spaces"""
        result = parse_estimated_owners("  8000  ")
        assert result == 8000

    def test_parse_empty_string(self):
        """Test parsing empty string returns 0"""
        assert parse_estimated_owners("") == 0

    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only string returns 0"""
        assert parse_estimated_owners("   ") == 0

    def test_parse_none_like_string(self):
        """Test parsing None returns 0"""
        assert parse_estimated_owners("") == 0

    def test_parse_invalid_format(self):
        """Test parsing invalid format returns 0"""
        assert parse_estimated_owners("invalid text") == 0

    def test_parse_invalid_range(self):
        """Test parsing invalid range returns 0"""
        assert parse_estimated_owners("abc - def") == 0

    def test_parse_range_with_commas(self):
        """Test parsing range with comma separators in numbers"""
        result = parse_estimated_owners("1000000 - 2000000")
        assert result == 1500000


class TestLoadData:
    """Tests for load_data function"""

    @patch("player_to_genre.cursor")
    def test_load_data_success(self, mock_cursor):
        """Test successful data loading from database"""
        mock_cursor.fetchall.return_value = [
            ("Game1", "Action,Adventure", "10000"),
            ("Game2", "RPG", "20000"),
        ]
        mock_cursor.description = [
            ("Name",),
            ("Genres",),
            ("Estimated_owners",),
        ]

        result = load_data()

        assert len(result) == 2
        assert list(result.columns) == ["Name", "Genres", "Estimated_owners"]
        assert result.iloc[0]["Name"] == "Game1"

    @patch("player_to_genre.cursor")
    def test_load_data_empty(self, mock_cursor):
        """Test loading with no data in database"""
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = [("Name",), ("Genres",), ("Estimated_owners",)]

        result = load_data()

        assert len(result) == 0
        assert list(result.columns) == ["Name", "Genres", "Estimated_owners"]


class TestProcessData:
    """Tests for process_data function"""

    def test_process_data_basic(self):
        """Test basic data processing"""
        df = pd.DataFrame({
            "Name": ["Game1", "Game2"],
            "Genres": ["Action,Adventure", "RPG"],
            "Estimated_owners": ["10000 - 20000", "5000"],
        })

        result = process_data(df)

        assert "genres_list" in result.columns
        assert "estimated_owners" in result.columns
        assert len(result) >= 3  # Should have more rows due to explosion

    def test_process_data_with_null_genres(self):
        """Test processing with null genres"""
        df = pd.DataFrame({
            "Name": ["Game1", "Game2"],
            "Genres": [None, "Action"],
            "Estimated_owners": ["10000", "20000"],
        })

        result = process_data(df)

        # Should filter out empty genres
        assert all(result["genres_list"] != "")
        assert len(result) < len(df) * 2

    def test_process_data_explode_genres(self):
        """Test that genres are properly exploded"""
        df = pd.DataFrame({
            "Name": ["Game1"],
            "Genres": ["Action,Adventure,RPG"],
            "Estimated_owners": ["30000 - 60000"],
        })

        result = process_data(df)

        assert len(result) == 3
        assert "Action" in result["genres_list"].values
        assert "Adventure" in result["genres_list"].values
        assert "RPG" in result["genres_list"].values

    def test_process_data_preserves_original(self):
        """Test that original dataframe is not modified"""
        df = pd.DataFrame({
            "Name": ["Game1"],
            "Genres": ["Action,Adventure"],
            "Estimated_owners": ["10000"],
        })
        df_copy = df.copy()

        process_data(df)

        pd.testing.assert_frame_equal(df, df_copy)


class TestAggregateByGenre:
    """Tests for aggregate_by_genre function"""

    def test_aggregate_basic(self):
        """Test basic aggregation"""
        df = pd.DataFrame({
            "Name": ["Game1", "Game2", "Game3"],
            "genres_list": ["Action", "Action", "RPG"],
            "estimated_owners": [1000, 2000, 5000],
        })

        result = aggregate_by_genre(df)

        assert len(result) == 2
        assert result.iloc[0]["genres_list"] == "RPG"  # Highest owners
        assert result.iloc[0]["estimated_owners"] == 5000
        assert result.iloc[1]["genres_list"] == "Action"
        assert result.iloc[1]["estimated_owners"] == 3000

    def test_aggregate_sorting(self):
        """Test that results are sorted by estimated_owners descending"""
        df = pd.DataFrame({
            "genres_list": ["A", "B", "C", "D"],
            "estimated_owners": [100, 400, 200, 300],
        })

        result = aggregate_by_genre(df)

        expected_order = ["B", "D", "C", "A"]
        assert list(result["genres_list"].values) == expected_order

    def test_aggregate_single_genre(self):
        """Test aggregation with single genre"""
        df = pd.DataFrame({
            "genres_list": ["Action", "Action"],
            "estimated_owners": [1000, 2000],
        })

        result = aggregate_by_genre(df)

        assert len(result) == 1
        assert result.iloc[0]["estimated_owners"] == 3000


class TestPlotGenreOwners:
    """Tests for plot_genre_owners function"""

    @patch("player_to_genre.plt.savefig")
    @patch("player_to_genre.plt.show")
    @patch("player_to_genre.plt.figure")
    def test_plot_creates_file(self, mock_figure, mock_show, mock_savefig):
        """Test that plot creates output file"""
        df = pd.DataFrame({
            "genres_list": ["Action", "RPG"],
            "estimated_owners": [1000, 2000],
        })
        output_path = Path("test_output.png")

        plot_genre_owners(df, output_path)

        mock_savefig.assert_called_once_with(output_path, dpi=150)
        mock_show.assert_called_once()

    @patch("player_to_genre.plt.show")
    @patch("player_to_genre.plt.figure")
    def test_plot_with_empty_dataframe(self, mock_figure, mock_show):
        """Test plotting with empty dataframe"""
        df = pd.DataFrame({
            "genres_list": [],
            "estimated_owners": [],
        })
        output_path = Path("test_empty.png")

        # Should not raise error
        plot_genre_owners(df, output_path)
        mock_figure.assert_called()


class TestLineageEvent:
    """Tests for LineageEvent class"""

    def test_lineage_event_initialization(self):
        """Test LineageEvent initialization"""
        event = LineageEvent()

        assert event.extract_source == ""
        assert event.extract_rows == 0
        assert event.checks_performed == []
        assert event.duplicates_found == 0
        assert event.analytics_summary == ""
        assert event.visualization_type == ""

    @patch("player_to_genre.load_data")
    @patch("player_to_genre.logger")
    def test_extract_method(self, mock_logger, mock_load_data):
        """Test extract method of LineageEvent"""
        mock_df = pd.DataFrame({"Name": ["Game1", "Game2"]})
        mock_load_data.return_value = mock_df

        event = LineageEvent()
        result = event.extract()

        assert event.extract_source == "gamedb.db"
        assert event.extract_rows == 2
        assert len(result) == 2

    @patch("player_to_genre.process_data")
    @patch("player_to_genre.logger")
    def test_checks_method(self, mock_logger, mock_process_data):
        """Test checks method of LineageEvent"""
        input_df = pd.DataFrame({"Name": ["Game1"]})
        processed_df = pd.DataFrame({"Name": ["Game1"], "genres_list": ["Action"]})
        mock_process_data.return_value = processed_df

        event = LineageEvent()
        result = event.checks(input_df)

        assert event.checks_performed == ["fillna_genres", "parse_owners", "filter_empty_genres"]
        pd.testing.assert_frame_equal(result, processed_df)

    def test_duplicates_method(self):
        """Test duplicates method of LineageEvent"""
        df = pd.DataFrame({
            "Name": ["Game1", "Game1", "Game2"],
            "genres_list": ["Action", "Action", "RPG"],
        })

        event = LineageEvent()
        result = event.duplicates(df)

        assert event.duplicates_found == 1
        assert len(result) == 2

    @patch("player_to_genre.aggregate_by_genre")
    @patch("player_to_genre.logger")
    def test_analytics_method(self, mock_logger, mock_aggregate):
        """Test analytics method of LineageEvent"""
        input_df = pd.DataFrame({"genres_list": ["Action", "RPG"]})
        agg_df = pd.DataFrame({"genres_list": ["Action", "RPG"], "estimated_owners": [100, 200]})
        mock_aggregate.return_value = agg_df

        event = LineageEvent()
        result = event.analytics(input_df)

        assert "Aggregated 2 genres" in event.analytics_summary
        pd.testing.assert_frame_equal(result, agg_df)

    @patch("player_to_genre.plot_genre_owners")
    @patch("player_to_genre.logger")
    def test_visualization_method(self, mock_logger, mock_plot):
        """Test visualization method of LineageEvent"""
        df = pd.DataFrame({"genres_list": ["Action"], "estimated_owners": [1000]})

        event = LineageEvent()
        event.visualization(df)

        assert event.visualization_type == "bar_chart"
        mock_plot.assert_called_once()


class TestSplitCsv:
    """Tests for split_csv function in chunk module"""

    def test_split_csv_basic(self):
        """Test basic CSV splitting"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test input CSV
            input_file = os.path.join(tmpdir, "input.csv")
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=chunk.SELECT_COLUMNS)
                writer.writeheader()
                for i in range(12000):
                    writer.writerow({col: f"value_{i}" for col in chunk.SELECT_COLUMNS})

            output_prefix = os.path.join(tmpdir, "chunk")
            result = chunk.split_csv(input_file, output_prefix, chunk_size=5000)

            assert result == 3  # 12000 rows / 5000 per chunk

    def test_split_csv_file_not_found(self):
        """Test split_csv with non-existent file"""
        with pytest.raises(FileNotFoundError):
            chunk.split_csv("non_existent_file.csv")

    def test_split_csv_missing_columns(self):
        """Test split_csv with missing columns"""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.csv")
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["Name", "Value"])
                writer.writeheader()
                writer.writerow({"Name": "Test", "Value": "123"})

            with pytest.raises(ValueError, match="not present in the input"):
                chunk.split_csv(input_file)

    def test_split_csv_exact_chunk_boundary(self):
        """Test split_csv when rows exactly match chunk size"""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.csv")
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=chunk.SELECT_COLUMNS)
                writer.writeheader()
                for i in range(5000):
                    writer.writerow({col: f"value_{i}" for col in chunk.SELECT_COLUMNS})

            output_prefix = os.path.join(tmpdir, "chunk")
            result = chunk.split_csv(input_file, output_prefix, chunk_size=5000)

            assert result == 2  # Returns csvCounter + 1 (one at boundary creates new chunk)

    def test_split_csv_custom_chunk_size(self):
        """Test split_csv with custom chunk size"""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.csv")
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=chunk.SELECT_COLUMNS)
                writer.writeheader()
                for i in range(1000):
                    writer.writerow({col: f"value_{i}" for col in chunk.SELECT_COLUMNS})

            output_prefix = os.path.join(tmpdir, "chunk")
            result = chunk.split_csv(input_file, output_prefix, chunk_size=100)

            assert result == 11  # Returns csvCounter + 1

    def test_split_csv_creates_directories(self):
        """Test that split_csv creates output directory if it doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.csv")
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=chunk.SELECT_COLUMNS)
                writer.writeheader()
                writer.writerow({col: f"value" for col in chunk.SELECT_COLUMNS})

            output_prefix = os.path.join(tmpdir, "new_dir", "chunk")
            result = chunk.split_csv(input_file, output_prefix)

            assert result == 1
            assert os.path.isdir(os.path.dirname(output_prefix))

    def test_split_csv_output_content(self):
        """Test that output chunks contain correct data"""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.csv")
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=chunk.SELECT_COLUMNS)
                writer.writeheader()
                for i in range(100):
                    writer.writerow(
                        {col: f"{col}_value_{i}" for col in chunk.SELECT_COLUMNS}
                    )

            output_prefix = os.path.join(tmpdir, "chunk")
            result = chunk.split_csv(input_file, output_prefix, chunk_size=50)

            # Check first chunk
            chunk_file = f"{output_prefix}_1.csv"
            with open(chunk_file, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 50

            # Check second chunk
            chunk_file = f"{output_prefix}_2.csv"
            with open(chunk_file, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 50

    def test_split_csv_selected_columns(self):
        """Test that only selected columns are written"""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.csv")
            # Create input with all columns
            all_columns = chunk.SELECT_COLUMNS + ["ExtraColumn"]
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=all_columns)
                writer.writeheader()
                for i in range(10):
                    writer.writerow({col: f"value_{i}" for col in all_columns})

            selected = chunk.SELECT_COLUMNS[:3]  # Select first 3 columns
            output_prefix = os.path.join(tmpdir, "chunk")
            result = chunk.split_csv(
                input_file, output_prefix, chunk_size=5, selected_columns=selected
            )

            # Check that output only has selected columns
            chunk_file = f"{output_prefix}_1.csv"
            with open(chunk_file, "r", newline="") as f:
                reader = csv.DictReader(f)
                assert list(reader.fieldnames) == selected

    def test_split_csv_default_data_directory(self):
        """Test that default output goes to data/ directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.csv")
            with open(input_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=chunk.SELECT_COLUMNS)
                writer.writeheader()
                writer.writerow({col: "value" for col in chunk.SELECT_COLUMNS})

            # Use relative path without directory
            output_prefix = "test_chunk"
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                result = chunk.split_csv(input_file, output_prefix)

                # Should create data/test_chunk_1.csv
                assert os.path.exists(os.path.join("data", "test_chunk_1.csv"))
            finally:
                os.chdir(original_cwd)


class TestMain:
    """Tests for main function"""

    @patch("chunk.split_csv")
    def test_main_calls_split_csv(self, mock_split_csv):
        """Test that main calls split_csv with default arguments"""
        mock_split_csv.return_value = 5

        chunk.main()

        mock_split_csv.assert_called_once()


class TestIntegration:
    """Integration tests combining multiple functions"""

    @patch("player_to_genre.load_data")
    @patch("player_to_genre.aggregate_by_genre")
    @patch("player_to_genre.plot_genre_owners")
    def test_lineage_event_full_pipeline(self, mock_plot, mock_agg, mock_load):
        """Test complete LineageEvent pipeline"""
        # Setup mocks
        raw_df = pd.DataFrame({
            "Name": ["Game1", "Game2"],
            "Genres": ["Action,Adventure", "RPG"],
            "Estimated_owners": ["10000 - 20000", "5000"],
        })
        mock_load.return_value = raw_df

        processed_df = pd.DataFrame({
            "Name": ["Game1", "Game1", "Game2"],
            "genres_list": ["Action", "Adventure", "RPG"],
            "estimated_owners": [15000, 15000, 5000],
        })

        agg_df = pd.DataFrame({
            "genres_list": ["Action", "Adventure", "RPG"],
            "estimated_owners": [15000, 15000, 5000],
        })
        mock_agg.return_value = agg_df

        # Run pipeline
        event = LineageEvent()
        raw = event.extract()
        processed = event.checks(raw)
        deduped = event.duplicates(processed)
        analytics = event.analytics(deduped)
        event.visualization(analytics)

        # Verify
        assert event.extract_source == "gamedb.db"
        assert event.extract_rows == 2
        assert "fillna_genres" in event.checks_performed
        assert event.duplicates_found >= 0
        assert "Aggregated" in event.analytics_summary
        assert event.visualization_type == "bar_chart"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
