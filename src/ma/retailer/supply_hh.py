import sys
from pathlib import Path
from typing import Dict, Optional

import click
import pandas as pd
import pandera as pa
import logging

from ma.ofgem.regos import RegosByTechMonthHolder, RegosProcessed, RegosRaw
from ma.neso.grid_mix import GridMixByTechMonth, GridMixProcessed, GridMixRaw
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DateTimeEngine as DTE

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class UpsampledSupplyHalfHourly(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict(
        timestamp         =CS(check=pa.Index(DTE(dayfirst=False))),
        supply_mwh        =CS(check=pa.Column(float)),
        tech              =CS(check=pa.Column(str)),
        retailer          =CS(check=pa.Column(str)),
    )
    # fmt: on


def _calculate_scaling_factors(
    grid_mix_by_tech_by_month: GridMixByTechMonth, regos_by_tech_month_holder: RegosByTechMonthHolder
) -> pd.DataFrame:
    """
    Calculate scaling factors for each technology, retailer and month using vectorized operations.

    Takes prepared grid mix and retailer data and calculates the proportion
    of grid generation that should be allocated to each retailer.
    """

    grid_mix_by_tech_month_df = grid_mix_by_tech_by_month.df
    tech_columns = [col for col in grid_mix_by_tech_month_df.columns if col.endswith("_mwh")]

    # Convert from wide to long format, resetting index to get month as a column
    grid_mix_long = pd.melt(
        grid_mix_by_tech_month_df.reset_index(),
        id_vars=["month"],
        value_vars=tech_columns,
        var_name="tech_column",
        value_name="grid_total_mwh",
    )

    # Standardise tech names
    grid_mix_long["tech"] = grid_mix_long["tech_column"].str.replace("_mwh", "")

    # Reset index to get month as a column
    rego_df = regos_by_tech_month_holder.df.reset_index()

    # Merge with retailer data
    merged_data = pd.merge(
        rego_df[["month", "tech", "current_holder", "rego_mwh"]],
        grid_mix_long[["month", "tech", "grid_total_mwh"]],
        on=["month", "tech"],
        how="left",
    )

    # Calculate fraction of grid in a vectorized way
    merged_data["fraction_of_grid"] = merged_data["rego_mwh"] / merged_data["grid_total_mwh"]

    # Rename columns to match expected output format
    result_df = merged_data.rename(columns={"current_holder": "retailer", "rego_mwh": "retailer_mwh"})
    result = result_df[["month", "tech", "retailer", "retailer_mwh", "fraction_of_grid"]]

    return result


def _scale_hh_with_fraction_of_grid(grid_mix: GridMixProcessed, scaling_df: pd.DataFrame) -> UpsampledSupplyHalfHourly:
    """
    Apply scaling factors to half-hourly grid mix data using vectorized operations.

    Creates a long-format DataFrame with columns for:
    - timestamp: The datetime of the generation
    - tech: The technology type (wind, solar, etc.)
    - retailer: The energy retailer
    - generation_mwh: The generation amount in MWh
    """
    # Return empty DataFrame with the right structure if scaling_df is empty
    if scaling_df.empty:
        return UpsampledSupplyHalfHourly(pd.DataFrame(columns=["timestamp", "tech", "retailer", "supply_mwh"]))

    grid_df = grid_mix.df
    scaling = scaling_df.copy()

    # Extract year and month from the timestamp index to enable joining
    grid_df = grid_df.reset_index()
    # Convert to first day of month to match GridMixByTechMonth schema
    grid_df["month"] = pd.to_datetime(grid_df["datetime"].dt.to_period("M").astype(str))
    grid_df = grid_df.rename(columns={"datetime": "timestamp"})

    # Identify technology columns
    tech_columns = [col for col in grid_df.columns if col.endswith("_mwh")]

    # Melt the dataframe to convert from wide to long format
    grid_long = pd.melt(
        grid_df,
        id_vars=["timestamp", "month"],
        value_vars=tech_columns,
        var_name="tech_col",
        value_name="supply_mwh",
    )

    # Extract technology name from column name
    grid_long["tech"] = grid_long["tech_col"].str.replace("_mwh", "")

    # Merge with scaling factors using the datetime month and tech
    result = pd.merge(
        grid_long,
        scaling,
        on=["month", "tech"],
        how="inner",
    )

    # Apply scaling factors to calculate retailer-specific generation
    result["supply_mwh"] = result["supply_mwh"] * result["fraction_of_grid"]

    result = result.drop(columns=["month", "fraction_of_grid", "tech_col", "retailer_mwh"]).set_index("timestamp")
    return UpsampledSupplyHalfHourly(result)


def _validate_date_ranges(
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    grid_mix: GridMixProcessed,
    regos: RegosProcessed,
) -> bool:
    """
    Validates that the requested date range is available in both grid mix and REGOS data.
    Uses half-open intervals [start_datetime, end_datetime) that are inclusive of start
    and exclusive of end.

    Parameters
    ----------
    start_datetime : pd.Timestamp
        The start date for the analysis (inclusive)
    end_datetime : pd.Timestamp
        The end date for the analysis (exclusive)
    grid_mix : GridMixProcessed
        The half-hourly grid mix data with DatetimeIndex
    regos : RegosProcessed
        The REGOS certificate data with a 'month' column

    Returns
    -------
    bool
        True if all checks pass, otherwise raises ValueError with appropriate message
    """
    error_messages = []

    # Check REGOS data range
    regos_df = regos.df
    regos_period_starts = pd.to_datetime(regos_df["start_year_month"])
    regos_period_ends = pd.to_datetime(regos_df["end_year_month"])
    regos_min_date = regos_period_starts.min()
    regos_max_date = regos_period_ends.max()

    # For half-open intervals [start_datetime, end_datetime)
    if start_datetime < regos_min_date:
        error_messages.append("Start date is before the earliest date in the REGOS data.")
    # For exclusive end - end_datetime should be <= regos_max_date
    if end_datetime > regos_max_date:
        error_messages.append("End date is after the latest date in the REGOS data.")

    # Check grid mix data range with half-open interval handling
    grid_min_date = grid_mix.df.index.min()
    grid_max_date = grid_mix.df.index.max()

    # Calculate the next timestamp after the last available data point
    # This represents the first invalid timestamp in a half-open interval
    next_timestamp_after_max = grid_max_date + pd.Timedelta(minutes=30)

    # For half-open intervals [start_datetime, end_datetime)
    if start_datetime < grid_min_date:
        error_messages.append("Start date is before the earliest date in the grid mix data.")
    # For exclusive end - end_datetime must be <= next_timestamp_after_max
    if end_datetime > next_timestamp_after_max:
        error_messages.append("End date is after the latest date in the grid mix data.")

    # Apply half-open interval [start_datetime, end_datetime) for filtering
    filtered_grid_mix = grid_mix.df[(grid_mix.df.index >= start_datetime) & (grid_mix.df.index < end_datetime)]
    if len(filtered_grid_mix) == 0:
        error_messages.append("No grid mix data available within the specified date range.")

    # Check for missing half-hourly data points using half-open interval
    expected_periods = pd.date_range(start=start_datetime, end=end_datetime, freq="30min", inclusive="left")
    if len(filtered_grid_mix) != len(expected_periods):
        # Find the missing timestamps
        actual_timestamps = set(filtered_grid_mix.index)
        missing_timestamps = [ts for ts in expected_periods if ts not in actual_timestamps]
        if missing_timestamps:
            error_messages.append(
                f"Missing half-hourly data points. Expected {len(expected_periods)} periods, "
                f"but found {len(filtered_grid_mix)}. First few missing: {missing_timestamps[:5]}"
            )

    # Return True if validation passed, otherwise raise ValueError with all error messages
    if error_messages:
        raise ValueError("\n".join(error_messages))

    return True


def upsample_retailer_monthly_supply_to_hh(
    rego_holder_reference: str,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    grid_mix: GridMixProcessed,
    regos_processed: RegosProcessed,
    output_path: Optional[Path] = None,
) -> UpsampledSupplyHalfHourly:
    """
    Upsamples the monthly supply of a specific retailer using half-hourly grid mix data.

    Parameters
    ----------
    rego_holder_reference : str
        The reference for the REGO holder to be scaled
    start_datetime : pd.Timestamp
        The start date for the analysis
    end_datetime : pd.Timestamp
        The end date for the analysis (exclusive)
    grid_mix : GridMixProcessed
        The half-hourly grid mix data with DatetimeIndex
    regos_processed : RegosProcessed
        The REGOS certificate data with a 'month' column
    """
    try:
        _validate_date_ranges(start_datetime, end_datetime, grid_mix, regos_processed)
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    # Step 1: Load and prepare the half-hourly grid mix data
    filtered_grid_mix = grid_mix.filter(start_datetime, end_datetime)
    grid_mix_tech_month = filtered_grid_mix.transform_to_grid_mix_by_tech_month()

    # Step 2: Prepare data for scaling calculation (convert units, align column names, extract year and month)
    regos_by_tech_month_holder = regos_processed.transform_to_regos_by_tech_month_holder().filter(
        holders=[rego_holder_reference]
    )
    # regos_by_tech_month_holder = _prepare_supply_retailer_month(rego_holder_reference, regos_processed)

    # Step 3: Calculate scaling factors
    scaling_df = _calculate_scaling_factors(grid_mix_tech_month, regos_by_tech_month_holder)

    # Step 4: Apply scaling to half-hourly data
    result = _scale_hh_with_fraction_of_grid(grid_mix, scaling_df)

    # Set timestamp as index to make subsequent operations easier

    if output_path:
        result.df.to_csv(output_path)
        click.echo(f"Results saved to {output_path}")
    else:
        click.echo("Results calculated but not saved (no output path provided)")

    return result


@click.command()
@click.option(
    "--grid-mix-path", type=click.Path(exists=True, path_type=Path), help="Path to the grid mix data CSV file"
)
@click.option("--regos-path", type=click.Path(exists=True, path_type=Path), help="Path to the REGOS data CSV file")
@click.option(
    "--rego-holder-reference",
    type=str,
    help="The reference for the REGOS holder to be scaled",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default="2022-04-01",
    help="Start date in YYYY-MM-DD format",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default="2023-04-01",
    help="End date in YYYY-MM-DD format (exclusive)",
)
@click.option("--output-path", type=click.Path(path_type=Path), default=None, help="Path to save the output CSV file")
def cli(
    grid_mix_path: Path,
    regos_path: Path,
    rego_holder_reference: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    output_path: Optional[Path] = None,
) -> None:
    upsample_retailer_monthly_supply_to_hh(
        rego_holder_reference=rego_holder_reference,
        start_datetime=pd.Timestamp(start_date),
        end_datetime=pd.Timestamp(end_date),
        grid_mix=GridMixRaw(grid_mix_path).transform_to_grid_mix_processed(),
        regos_processed=RegosRaw(regos_path).transform_to_regos_processed(),
        output_path=output_path,
    )


if __name__ == "__main__":
    cli()
