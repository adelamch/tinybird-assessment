import os
from datetime import date

import duckdb
import pytest

from src.helper import BASE_URL, build_yellow_taxi_url, latest_allowed_year_month, validate_year_month
from src.task import Etl, OUTPUT_COLUMNS

@pytest.fixture
def local_parquet(tmp_path):
    """Create a small parquet file with known trip_distance values for testing."""
    path = str(tmp_path / "test_trips.parquet")
    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (VALUES
                (1, 100, 200, 1.0),
                (1, 101, 201, 2.0),
                (1, 102, 202, 3.0),
                (1, 103, 203, 4.0),
                (1, 104, 204, 5.0),
                (2, 105, 205, 6.0),
                (2, 106, 206, 7.0),
                (2, 107, 207, 8.0),
                (2, 108, 208, 9.0),
                (2, 109, 209, 10.0)
            ) AS t(VendorID, PULocationID, DOLocationID, trip_distance)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()
    return path

@pytest.fixture
def duckdb_con():
    """Provide a fresh DuckDB connection (no httpfs needed for local files)."""
    con = duckdb.connect()
    yield con
    con.close()

class TestBuildUrl:
    def test_url_format(self):
        url = build_yellow_taxi_url(2024, 1)
        assert url == f"{BASE_URL}/yellow_tripdata_2024-01.parquet"

    def test_invalid_year(self):
        with pytest.raises(ValueError, match="year must be >= 2009"):
            validate_year_month(2008, 12)

    def test_invalid_month_low(self):
        with pytest.raises(ValueError, match="month must be between 1 and 12"):
            validate_year_month(2024, 0)

    def test_invalid_month_high(self):
        with pytest.raises(ValueError, match="month must be between 1 and 12"):
            validate_year_month(2024, 13)

    def test_latest_allowed_example_feb_2026(self):
        # If today is 2026-02-11, latest allowed should be 2025-11
        assert latest_allowed_year_month(today=date(2026, 2, 11)) == (2025, 11)
        validate_year_month(2025, 11, today=date(2026, 2, 11))  # should pass
        with pytest.raises(ValueError, match="too recent"):
            validate_year_month(2025, 12, today=date(2026, 2, 11))

    def test_latest_allowed_example_may_2028(self):
        # If today is 2028-05-23, latest allowed should be 2028-02
        assert latest_allowed_year_month(today=date(2028, 5, 23)) == (2028, 2)
        validate_year_month(2028, 2, today=date(2028, 5, 23))  # should pass
        with pytest.raises(ValueError, match="too recent"):
            validate_year_month(2028, 3, today=date(2028, 5, 23))

class TestComputePercentile:
    def test_percentile_value(self, duckdb_con, local_parquet):
        """With values 1-10, the 0.9 percentile should be 9.0."""
        etl = Etl(year=2024, month=1)
        etl.con = duckdb_con
        etl.percentile = 0.9
        threshold = etl.compute_percentile(local_parquet)
        assert threshold == pytest.approx(9.0, abs=0.1)

    def test_median(self, duckdb_con, local_parquet):
        """With values 1-10, the median (0.5 percentile) should be 5.5."""
        etl = Etl(year=2024, month=1)
        etl.con = duckdb_con
        etl.percentile = 0.5
        threshold = etl.compute_percentile(local_parquet)
        assert threshold == pytest.approx(5.5, abs=0.01)

    def test_fails_when_file_cannot_be_read(self, duckdb_con):
        bad_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/this-file-does-not-exist.parquet"
        etl = Etl(year=2024, month=1)
        etl.con = duckdb_con
        etl.percentile = 0.9
        with pytest.raises(RuntimeError, match="Failed to read/query file"):
            etl.compute_percentile(bad_url)

    def test_fails_when_all_distances_are_null(self, duckdb_con, tmp_path):
        null_parquet = str(tmp_path / "null_trips.parquet")
        duckdb_con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    (1, 100, 200, NULL),
                    (2, 101, 201, NULL)
                ) AS t(VendorID, PULocationID, DOLocationID, trip_distance)
            ) TO '{null_parquet}' (FORMAT PARQUET)
            """
        )
        etl = Etl(year=2024, month=1)
        etl.con = duckdb_con
        with pytest.raises(RuntimeError, match="Percentile computation returned no result"):
            etl.compute_percentile(null_parquet)

class TestFilterAndExport:
    def test_filters_correctly(self, duckdb_con, local_parquet, tmp_path):
        output_path = str(tmp_path / "output.parquet")
        threshold = 8.0
        etl = Etl(year=2024, month=1)
        etl.con = duckdb_con
        etl.output_columns = OUTPUT_COLUMNS
        count = etl.filter_and_export(local_parquet, threshold, output_path)

        # Values 9.0 and 10.0 are above 8.0
        assert count == 2
        assert os.path.exists(output_path)

    def test_output_columns(self, duckdb_con, local_parquet, tmp_path):
        output_path = str(tmp_path / "output.parquet")
        etl = Etl(year=2024, month=1)
        etl.con = duckdb_con
        etl.output_columns = OUTPUT_COLUMNS
        etl.filter_and_export(local_parquet, 5.0, output_path)

        result = duckdb_con.execute(f"SELECT * FROM read_parquet('{output_path}') LIMIT 1").description
        column_names = [col[0] for col in result]
        assert column_names == OUTPUT_COLUMNS

    def test_no_results_above_threshold(self, duckdb_con, local_parquet, tmp_path):
        output_path = str(tmp_path / "output.parquet")
        etl = Etl(year=2024, month=1)
        etl.con = duckdb_con
        etl.output_columns = OUTPUT_COLUMNS
        count = etl.filter_and_export(local_parquet, 100.0, output_path)
        assert count == 0

class TestRun:
    def test_run_clears_output_folder_before_writing_new_file(self, tmp_path):
        etl = Etl(year=2024, month=1)
        etl.output_dir = str(tmp_path)

        old_file_1 = tmp_path / "old_1.parquet"
        old_file_2 = tmp_path / "old_2.txt"
        old_file_1.write_text("old")
        old_file_2.write_text("old")

        def fake_compute_percentile(_file_url: str) -> float:
            return 9.9

        def fake_filter_and_export(_file_url: str, _threshold: float, output_path: str) -> int:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("new")
            return 1

        etl.compute_percentile = fake_compute_percentile
        etl.filter_and_export = fake_filter_and_export

        result = etl.run()

        files_after_run = [p.name for p in tmp_path.iterdir()]
        assert len(files_after_run) == 1
        assert files_after_run[0] == f"trips_above_p90_{etl.year}-{etl.month:02d}.parquet"
        assert result["output_path"].endswith(files_after_run[0])

    def test_run_clears_nested_directories_in_output(self, tmp_path):
        etl = Etl(year=2024, month=1)
        etl.output_dir = str(tmp_path)

        nested_dir = tmp_path / "old_folder"
        nested_dir.mkdir()
        (nested_dir / "old_file.txt").write_text("old")

        def fake_compute_percentile(_file_url: str) -> float:
            return 9.9

        def fake_filter_and_export(_file_url: str, _threshold: float, output_path: str) -> int:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("new")
            return 1

        etl.compute_percentile = fake_compute_percentile
        etl.filter_and_export = fake_filter_and_export
        etl.run()

        files_after_run = [p.name for p in tmp_path.iterdir()]
        assert len(files_after_run) == 1
        assert files_after_run[0] == f"trips_above_p90_{etl.year}-{etl.month:02d}.parquet"

    def test_run_happy_path_wires_compute_and_filter(self, tmp_path):
        etl = Etl(year=2024, month=1)
        etl.output_dir = str(tmp_path)

        calls = {}

        def fake_compute_percentile(file_url: str) -> float:
            calls["file_url_from_compute"] = file_url
            return 7.5

        def fake_filter_and_export(file_url: str, threshold: float, output_path: str) -> int:
            calls["file_url_from_filter"] = file_url
            calls["threshold"] = threshold
            calls["output_path"] = output_path
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("new")
            return 123

        etl.compute_percentile = fake_compute_percentile
        etl.filter_and_export = fake_filter_and_export

        result = etl.run()

        expected_url = f"{BASE_URL}/yellow_tripdata_2024-01.parquet"
        assert calls["file_url_from_compute"] == expected_url
        assert calls["file_url_from_filter"] == expected_url
        assert calls["threshold"] == 7.5
        assert result["count"] == 123
        assert result["threshold"] == 7.5
        assert result["output_path"] == calls["output_path"]
