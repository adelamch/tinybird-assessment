import logging
import os
import argparse
import shutil

import duckdb

from src.helper import MIN_YEAR, build_yellow_taxi_url, validate_year_month

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = ["VendorID", "PULocationID", "DOLocationID", "trip_distance"]
PERCENTILE = 0.9


class Etl:
    def __init__(
        self,
        year: int | None = None,
        month: int | None = None,
    ) -> None:
        if year is None or month is None:
            args = self._parse_args()
            year = args.year
            month = args.month

        self.year = year
        self.month = month
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "output")
        self.output_columns = OUTPUT_COLUMNS
        self.percentile = PERCENTILE
        self.con = self._setup_duckdb()
        validate_year_month(self.year, self.month)

    def _parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--year",
            type=int,
            required=True,
            help=f"Year of the file to process (must be >= {MIN_YEAR}, e.g. 2024)",
        )
        parser.add_argument("--month", type=int, required=True, help="Month of the file to process (1-12)")
        return parser.parse_args()

    def _setup_duckdb(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        return con

    def _clear_output_dir(self) -> None:
        """Remove all existing files/folders inside output directory."""
        if not os.path.isdir(self.output_dir):
            return
        for entry in os.scandir(self.output_dir):
            path = entry.path
            if entry.is_dir():
                shutil.rmtree(path)
            else:
                os.remove(path)

    def compute_percentile(self, file_url: str) -> float:
        """Compute configured percentile of trip_distance for one parquet file."""
        logger.info("Computing %.0fth percentile for %s ...", self.percentile * 100, file_url)
        try:
            result = self.con.execute(
                """
                SELECT percentile_cont(?) WITHIN GROUP (ORDER BY trip_distance) AS p
                FROM read_parquet(?, union_by_name=true)
                WHERE trip_distance IS NOT NULL
                """,
                [self.percentile, file_url],
            ).fetchone()
        except duckdb.Error as exc:
            raise RuntimeError(f"Failed to read/query file: {file_url}") from exc

        if result is None or result[0] is None:
            raise RuntimeError("Percentile computation returned no result.")

        threshold = float(result[0])
        logger.info("%.1f percentile threshold: %.4f miles", self.percentile, threshold)
        return threshold

    def filter_and_export(self, file_url: str, threshold: float, output_path: str) -> int:
        """Export trips above threshold and return exported row count."""
        columns = ", ".join(self.output_columns)
        logger.info("Filtering trips with trip_distance > %.4f and exporting to %s ...", threshold, output_path)
        try:
            # DuckDB does not support parameterized filenames in COPY TO.
            self.con.execute(
                f"""
                COPY (
                    SELECT {columns}
                    FROM read_parquet(?, union_by_name=true)
                    WHERE trip_distance > ?
                ) TO '{output_path}' (FORMAT PARQUET)
                """,
                [file_url, threshold],
            )
        except duckdb.Error as exc:
            raise RuntimeError(f"Failed to read/query file: {file_url}") from exc

        count = self.con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')",
        ).fetchone()[0]
        return count

    def run(self) -> dict[str, str | float | int]:
        file_url = build_yellow_taxi_url(self.year, self.month)

        os.makedirs(self.output_dir, exist_ok=True)
        self._clear_output_dir()
        output_path = os.path.join(self.output_dir, f"trips_above_p90_{self.year}-{self.month:02d}.parquet")

        threshold = self.compute_percentile(file_url)
        count = self.filter_and_export(file_url, threshold, output_path)

        result = {
            "file_url": file_url,
            "threshold": threshold,
            "count": count,
            "output_path": output_path,
        }
        logger.warning("Done | nyc_taxi_trip_percentile | %s | Return: %s", f"{self.year}-{self.month:02d}", result)
        print(f"\n{'='*60}")
        print(f"  Input file               : {result['file_url']}")
        print(f"  0.9 percentile threshold : {result['threshold']:.4f} miles")
        print(f"  Trips above threshold    : {result['count']:,}")
        print(f"  Output file              : {result['output_path']}")
        print(f"{'='*60}\n")
        return result
