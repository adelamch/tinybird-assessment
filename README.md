# NYC Yellow Taxi - Trips Over 0.9 Percentile in Distance

This project identifies all NYC Yellow Taxi trips that exceed the 90th percentile in trip distance, querying the official [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) parquet files directly from their public URLs — no manual downloads required.

**Disclaimer**: AI tools were used as coding assistants for implementation support.

## Steps to reproduce the test

### Requirements

- **Python 3.13+** (or [pyenv](https://github.com/pyenv/pyenv#installation))
- **Make** ([install guide](https://leangaurav.medium.com/how-to-setup-install-gnu-make-on-windows-324480f1da69))
- **Docker** ([install Docker](https://docs.docker.com/engine/install/))

Docker is recommended so that anyone reviewing this can run it with a single command without worrying about Python versions or virtual environments. Results will be the same independently of where the test is run.

1. **Clone the repo**
   ```bash
   git clone <repo-url>
   cd tinybird-assessment
   ```

2. **Run with Docker** (recommended)
   ```bash
   make run YEAR=2024 MONTH=1
   ```
   This builds the Docker image, installs all dependencies, and executes the script. Results are written to `output/`.

3. **Or run locally** (without Docker)
   ```bash
   make setup
   make execute YEAR=2024 MONTH=1
   ```

4. **Run tests**
   ```bash
   make test
   ```

5. **View results**

   The output is saved as a parquet file in the `output/` directory. A summary is printed to stdout showing the computed percentile threshold and the number of qualifying trips.

   **Important**: before writing a new result, the script automatically clears the `output/` folder, so there is only one output file at a time (the latest execution). Previous output files are deleted automatically.

   If you want to keep previous outputs, copy or move the generated parquet file to another folder before running the script again.


## Questions that I would ask the client to structure the problem

1. The request is ambiguous: "Using NYC “Yellow Taxi” Trips Data, give me all the trips over 0.9 percentile in distance travelled for any of the parquet files you can find there.". **Requirement clarification**: Do we need to always use all files available and compute the 0.9 percentile in distance for all yearly trips together, or do we need to calculate the 0.9 percentile in distance for all trips in a given, single file?
2. Is this a one-off or will we need to run the analysis periodically and automatically include newly published datasets? 
3. How big are the parquet files? If we need to compute the 0.9 percentile for all of them together, how many files do we have to query?
4. Is the 0.9 percentile fixed, or will the client want to change it to compare results using different percentiles?
5. Do all files have the same schema?
6. What columns form the unique trip identifier?
7. Is the distance travelled measured in the same unit across all files?
8. How should the missing values in the distance field be handled? How are outliers with implausible trip distances handled?
9. How should errors when reading/querying a file be handled? If all files are used every time, should the whole process break or should the failing files be skipped? What percentage of files skipped would be acceptable?
10. What is the expected output format, how will the results be consumed and by whom? Should results be exported or stored somewhere else? Which trip information about the 0.9 percentile rows is relevant for the client?

## Assumptions

The following assumptions have been made for the scope of this test, but in a real setting I would confirm with the client that they are correct.

These assumptions can also be challenged during the implementation phase, and they should be re-evaluated.

1. All "yellow taxi" trips data is contained in files named "Yellow Taxi Trip Records", and thus we don't need to use any other files (green/for hire/high volume).
2. All URLs for yellow taxi trips data follow the same structure.
3. The client expects to get the 0.9 percentile in distance travelled for one file at a time, given a year and a month as inputs.
4. The code needs to automatically accept newly published files each month, so the client will always be able to ask for files from two months ago (because the TLC page mentions that "Trip data is published monthly on this website, typically with a two-month delay to allow time for full vendor submissions.").
5. The 0.9 percentile is fixed.
6. The data contract does not change over the years: all files follow the same schema and the distance is measured in the same unit (there is only one Data Dictionary file for all parquets).
7. Null values can be safely removed, as they don't provide any value for a percentile calculation.
8. The TLC notes that data is provided by technology vendors and may contain inaccuracies, including `trip_distance = 0` or implausibly large values. However, defining how to treat outliers falls out of the scope of this test, so raw distance data will be used as it comes.
9. As we'll query just one file per execution, if there is an error when reading it we'll just raise an error message.
10. The client expects the resulting rows in a single parquet file, which will be generated in the `output/` folder, containing the trip identifier columns and the distance travelled in each trip.

A quick exploration of some of the datasets was done to check that these assumptions hold.

## Data notes

- **Source**: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Dataset**: Yellow Taxi Trip Records (2009–2025)
- **Distance field**: `trip_distance` (in miles, as reported by the taximeter)
- **Unique trip identifier**: According to the [Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf), there is no combination of fields that provide a unique identifier for a trip. Therefore, the columns `VendorID`, `PULocationID` and `DOLocationID` will be used in the output file. 

## Technical implementation

We process one file per execution. The client provides `year` and `month`, the program builds the corresponding URL, and DuckDB queries that file directly (without downloading it first).

For each execution, the process is:

1. **Step 1: Build file URL from input**: The script creates the URL from the input year/month.
2. **Step 2: Compute the 0.9 percentile**: Query that file and calculate `PERCENTILE_CONT(0.9)` over `trip_distance` (ignoring nulls).
3. **Step 3: Filter and export**: Query the same file again and keep only rows with `trip_distance > p90_threshold`, exporting the result to one parquet file.

If the file cannot be read (non-existing month, network issue, etc.), the script fails with a clear error message.

**Usage summary**: run one month at a time, for example `make run YEAR=2024 MONTH=1`.

## Project structure

```
.
├── Dockerfile
├── Makefile               # setup, run, test, clean commands
├── Pipfile                # Python dependencies
├── README.md
├── src/
│   ├── main.py            # orchestrator and CLI entry point
│   ├── task.py            # ETL class and DuckDB query logic
│   └── helper.py          # validation and URL helper functions
├── tests/
│   └── test_main.py       # unit tests (URL generation, percentile, filtering)
└── output/                # generated results (git-ignored)
```

## Future improvements

1. Add optional `--percentile` input (default `0.9`) so clients can request different percentile thresholds without code changes.
2. Define and implement outliers treatment.
3. If multi-file processing is needed in the future, redesign ingestion to avoid too many HTTP requests in a single run. For example,  maintain a metadata table of available files and refresh it periodically with an automated updater, or ingest the files into internal storage as new datasets are published.
4. Add retry logic with exponential backoff for transient network errors when reading the remote parquet files.
5. Add schema checks before running the query to fail fast with a clear message if the TLC dataset schema changes.
6. Add CI checks (format/lint/tests) so every change is automatically validated in pull requests.
