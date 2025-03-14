import datetime
import sys
from pathlib import Path
from typing import Optional, Union

import click
import pandas as pd

import ma.neso.grid_mix
from ma.ofgem.regos import RegosProcessed, RegosRaw


def _prepare_supply_supplier_month(
    rego_holder: str,
    regos_processed: RegosProcessed,
) -> pd.DataFrame:
    """
    Prepare supplier generation data for scaling calculation.

    Filters the data to the specified rego holder reference and converts units, aligns column names, and extracts year and month information.
    """
    supply_by_supplier_by_month = regos_processed.filter(holders=[rego_holder])
    supply_by_supplier_by_month["rego_mwh"] = supply_by_supplier_by_month["rego_gwh"] * 1000  # Convert GWh to MWh
    supply_by_supplier_by_month = supply_by_supplier_by_month.rename(columns={"tech": "tech"})  # Align column names
    supply_by_supplier_by_month["tech"] = supply_by_supplier_by_month["tech"].str.lower()  # Align tech names across dfs
    supply_by_supplier_by_month["year"] = supply_by_supplier_by_month["start_year_month"].dt.year
    supply_by_supplier_by_month["month_num"] = supply_by_supplier_by_month["start_year_month"].dt.month
    return supply_by_supplier_by_month


def _prepare_grid_mix_monthly(grid_mix_by_tech_by_month: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare grid mix data for scaling calculation.

    Extracts year and month information and aggregates to monthly totals.
    """
    grid_mix_monthly = grid_mix_by_tech_by_month.copy().reset_index()

    # Extract year and month
    grid_mix_monthly["year"] = grid_mix_monthly["datetime"].dt.year
    grid_mix_monthly["month"] = grid_mix_monthly["datetime"].dt.month

    # Group by year and month, sum only numeric columns
    numeric_cols = grid_mix_monthly.select_dtypes(include="number").columns.tolist()
    numeric_cols.remove("year")
    numeric_cols.remove("month")

    # Group by year and month, sum only numeric columns
    grid_mix_monthly = grid_mix_monthly.groupby(["year", "month"])[numeric_cols].sum()
    # Reset index to have year and month as columns
    grid_mix_monthly = grid_mix_monthly.reset_index()

    return grid_mix_monthly


def _calculate_scaling_factors(
    grid_mix_by_tech_by_month: pd.DataFrame, supply_by_supplier_by_month: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate scaling factors for each technology, supplier and month using vectorized operations.

    Takes prepared grid mix and supplier data and calculates the proportion
    of grid generation that should be allocated to each supplier.
    """
    supplier_supply = supply_by_supplier_by_month.copy()
    supplier_supply = supplier_supply.rename(columns={"month_num": "month"})  # required for joining

    grid_mix = grid_mix_by_tech_by_month.copy()
    tech_columns = [col for col in grid_mix.columns if col.endswith("_mwh")]

    # Select only the technology columns and year/month
    tech_data = grid_mix[["year", "month"] + tech_columns].copy()

    # Convert from wide to long format
    grid_mix_long = pd.melt(
        tech_data,
        id_vars=["year", "month"],
        value_vars=tech_columns,
        var_name="tech_column",
        value_name="grid_total_mwh",
    )

    # Standardise tech names
    grid_mix_long["tech"] = grid_mix_long["tech_column"].str.replace("_mwh", "")

    # Merge with supplier data
    merged_data = pd.merge(
        supplier_supply[["year", "month", "tech", "current_holder", "rego_mwh"]],
        grid_mix_long[["year", "month", "tech", "grid_total_mwh"]],
        on=["year", "month", "tech"],
        how="left",
    )

    # Calculate fraction of grid in a vectorized way
    merged_data["fraction_of_grid"] = merged_data["rego_mwh"] / merged_data["grid_total_mwh"]

    # Rename columns to match expected output format
    result_df = merged_data.rename(columns={"current_holder": "supplier", "rego_mwh": "supplier_mwh"})
    return result_df[["year", "month", "tech", "supplier", "grid_total_mwh", "supplier_mwh", "fraction_of_grid"]]


def _scale_hh_with_fraction_of_grid(grid_mix_hh: pd.DataFrame, scaling_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply scaling factors to half-hourly grid mix data using vectorized operations.

    Creates a long-format DataFrame with columns for:
    - timestamp: The datetime of the generation
    - tech: The technology type (wind, solar, etc.)
    - supplier: The energy supplier
    - generation_mwh: The generation amount in MWh
    """
    # Return empty DataFrame with the right structure if scaling_df is empty
    if scaling_df.empty:
        return pd.DataFrame(columns=["timestamp", "tech", "supplier", "supply_mwh"])

    grid_hh = grid_mix_hh.copy()
    scaling = scaling_df.copy()

    # Extract year and month from the timestamp index to enable joining
    grid_hh = grid_hh.reset_index()
    grid_hh["year"] = grid_hh["datetime"].dt.year
    grid_hh["month"] = grid_hh["datetime"].dt.month
    grid_hh = grid_hh.rename(columns={"datetime": "timestamp"})

    # Identify technology columns
    tech_columns = [col for col in grid_hh.columns if col.endswith("_mwh")]

    # Melt the dataframe to convert from wide to long format
    grid_long = pd.melt(
        grid_hh,
        id_vars=["timestamp", "year", "month"],
        value_vars=tech_columns,
        var_name="tech_col",
        value_name="supply_mwh",
    )

    # Extract technology name from column name
    grid_long["tech"] = grid_long["tech_col"].str.replace("_mwh", "")

    # Merge with scaling factors
    result = pd.merge(
        grid_long,
        scaling[["year", "month", "tech", "supplier", "fraction_of_grid"]],
        on=["year", "month", "tech"],
        how="inner",
    )

    # Apply scaling factors to calculate supplier-specific generation
    result["supply_mwh"] = result["supply_mwh"] * result["fraction_of_grid"]

    return result[["timestamp", "tech", "supplier", "supply_mwh"]]


def _validate_date_ranges(
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    grid_mix_hh: pd.DataFrame,
    regos_data: pd.DataFrame,
) -> bool:
    """
    Validates that the requested date range is available in both grid mix and REGOS data.

    Parameters
    ----------
    start_datetime : pd.Timestamp
        The start date for the analysis
    end_datetime : pd.Timestamp
        The end date for the analysis (exclusive)
    grid_mix_hh : pd.DataFrame
        The half-hourly grid mix data with DatetimeIndex
    regos_data : pd.DataFrame
        The REGOS certificate data with a 'month' column

    Returns
    -------
    bool
        True if all checks pass, otherwise raises ValueError with appropriate message
    """
    # Validate grid_mix_hh has DatetimeIndex
    if not isinstance(grid_mix_hh.index, pd.DatetimeIndex):
        raise ValueError("Grid mix data must have a DatetimeIndex")

    error_messages = []

    # Check grid mix data range
    if start_datetime < grid_mix_hh.index.min() or end_datetime > grid_mix_hh.index.max():
        error_messages.append("Date range outside of grid mix data range.")

    filtered_grid_mix = grid_mix_hh[(grid_mix_hh.index >= start_datetime) & (grid_mix_hh.index < end_datetime)]
    if len(filtered_grid_mix) == 0:
        error_messages.append("No grid mix data available within the specified date range.")

    # Check for missing half-hourly data points
    expected_periods = pd.date_range(start=start_datetime, end=end_datetime - pd.Timedelta(minutes=30), freq="30min")
    if len(filtered_grid_mix) != len(expected_periods):
        # Find the missing timestamps
        actual_timestamps = set(filtered_grid_mix.index)
        missing_timestamps = [ts for ts in expected_periods if ts not in actual_timestamps]
        if missing_timestamps:
            error_messages.append(
                f"Missing half-hourly data points. Expected {len(expected_periods)} periods, "
                f"but found {len(filtered_grid_mix)}. First few missing: {missing_timestamps[:5]}"
            )

    # Check REGOS data range
    if "month" in regos_data.columns:
        regos_dates = pd.to_datetime(regos_data["month"])
        regos_min_date = regos_dates.min()
        regos_max_date = regos_dates.max()

        if start_datetime < regos_min_date or end_datetime > regos_max_date:
            error_messages.append("Date range outside of REGOS data range.")
    else:
        error_messages.append("Could not determine date range in REGOS data (no 'month' column found).")

    # Return True if validation passed, otherwise raise ValueError with all error messages
    if error_messages:
        raise ValueError("\n".join(error_messages))

    return True


def upsample_supplier_monthly_supply_to_hh(
    rego_holder_reference: str,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    grid_mix_tech_month: pd.DataFrame,
    regos_processed: RegosProcessed,
    output_path: Optional[Path] = None,
) -> pd.DataFrame:
    # Step 1: Load and prepare the half-hourly grid mix data
    grid_mix_hh = ma.neso.grid_mix.filter(grid_mix_tech_month, start_datetime, end_datetime)

    # Step 2: Prepare data for scaling calculation (convert units, align column names, extract year and month)
    supply_supplier_month = _prepare_supply_supplier_month(rego_holder_reference, regos_processed)
    grid_mix_tech_month = _prepare_grid_mix_monthly(grid_mix_tech_month)

    # Step 3: Calculate scaling factors
    scaling_df = _calculate_scaling_factors(grid_mix_tech_month, supply_supplier_month)

    # Step 4: Apply scaling to half-hourly data
    result_df = _scale_hh_with_fraction_of_grid(grid_mix_hh, scaling_df)

    # Set timestamp as index to make subsequent operations easier
    result_df = result_df.set_index("timestamp")

    if output_path:
        result_df.to_csv(output_path)
        click.echo(f"Results saved to {output_path}")
    else:
        click.echo("Results calculated but not saved (no output path provided)")

    return result_df


def _prepare_upsampling_data(
    grid_mix_path: Path,
    regos_path: Path,
    start_date: Union[str, pd.Timestamp, datetime.datetime],
    end_date: Union[str, pd.Timestamp, datetime.datetime],
) -> tuple[pd.DataFrame, RegosProcessed, pd.Timestamp, pd.Timestamp]:
    """
    Prepare data for upsampling by loading, validating, and transforming the input data.

    Parameters
    ----------
    grid_mix_path : Path
        Path to the grid mix data CSV file
    regos_path : Path
        Path to the REGOS data CSV file
    start_date : Union[str, pd.Timestamp, datetime.datetime]
        The start date for the analysis, will be converted to pd.Timestamp
    end_date : Union[str, pd.Timestamp, datetime.datetime]
        The end date for the analysis (exclusive), will be converted to pd.Timestamp
    exit_on_error : bool, default=False
        Whether to exit the program on error (intended for CLI use)

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp, pd.Timestamp]
        A tuple containing (grid_mix_by_month, supply_by_supplier_by_month, start_datetime, end_datetime)

    Raises
    ------
    ValueError
        If date validation fails and exit_on_error is False
    """
    try:
        # Convert input dates to pandas Timestamp
        start_datetime = pd.Timestamp(start_date)
        end_datetime = pd.Timestamp(end_date)

        # Load grid mix and regos data
        grid_mix_hh = ma.neso.grid_mix.load(grid_mix_path)
        regos = RegosRaw(regos_path).transform_to_regos_processed()

        # Validate date ranges
        # TODO #18  _validate_date_ranges(start_datetime, end_datetime, grid_mix_hh, regos.df)

        # Prepare inputs
        # TODO #18
        # grid_mix_by_month = ma.neso.grid_mix.groupby_tech_and_month(grid_mix_hh)
        # regos_by_tech_month_holder = regos.transform_to_regos_by_tech_month_holder()

        # TODO #18 return grid_mix_by_month, regos_by_tech_month_holder, start_datetime, end_datetime
        return grid_mix_hh, regos, start_datetime, end_datetime
    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


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
    """Run the upsampling from the command line."""
    # Prepare data with CLI-appropriate error handling
    # If the dates are not valid, the function will raise a ValueError and exit the program
    grid_mix_by_month, regos_processed, start_datetime, end_datetime = _prepare_upsampling_data(
        grid_mix_path, regos_path, start_date, end_date
    )

    # Run the upsampling
    upsample_supplier_monthly_supply_to_hh(
        rego_holder_reference=rego_holder_reference,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        grid_mix_tech_month=grid_mix_by_month,
        regos_processed=regos_processed,
        output_path=output_path,
    )


if __name__ == "__main__":
    cli()
