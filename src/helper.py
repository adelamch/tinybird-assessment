from datetime import date

MIN_YEAR = 2009
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def latest_allowed_year_month(today: date | None = None) -> tuple[int, int]:
    """
    Return the latest (year, month) allowed by the "2 full months delay" rule.

    Example:
    - If today is 2026-02-11, latest allowed is 2025-11.
    - If today is 2028-05-23, latest allowed is 2028-02.
    """
    if today is None:
        today = date.today()

    # "Two full months ago" means current month and previous two months are not allowed.
    # Therefore latest allowed month is current_month - 3.
    month_index = (today.year * 12 + (today.month - 1)) - 3
    allowed_year = month_index // 12
    allowed_month = (month_index % 12) + 1
    return allowed_year, allowed_month


def validate_year_month(year: int, month: int, today: date | None = None) -> None:
    """Validate year/month against dataset constraints."""
    if year < MIN_YEAR:
        raise ValueError(f"year must be >= {MIN_YEAR}")
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")

    max_year, max_month = latest_allowed_year_month(today=today)
    requested_index = year * 12 + (month - 1)
    max_index = max_year * 12 + (max_month - 1)
    if requested_index > max_index:
        raise ValueError(
            "Requested year-month is too recent. "
            f"Latest allowed is {max_year}-{max_month:02d} (2 full months delay rule)."
        )


def build_yellow_taxi_url(year: int, month: int, base_url: str = BASE_URL) -> str:
    """Build Yellow Taxi parquet URL for a given year/month (no validation)."""
    return f"{base_url}/yellow_tripdata_{year}-{month:02d}.parquet"
