from pathlib import Path
import sys
from typing import Optional
import pandas as pd
import click

import ma.neso.grid_mix
import ma.ofgem.regos


def _prepare_gen_supplier_month(rego_holder: str, gen_by_supplier_by_month: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare supplier generation data for scaling calculation.

    Filters the data to the specified rego holder reference and converts units, aligns column names, and extracts year and month information.
    """
    gen_by_supplier_by_month = gen_by_supplier_by_month.copy()
    gen_by_supplier_by_month = ma.ofgem.regos.filter(gen_by_supplier_by_month, holders=[rego_holder])  # Filter holder
    gen_by_supplier_by_month["rego_mwh"] = gen_by_supplier_by_month["rego_gwh"] * 1000  # Convert GWh to MWh
    gen_by_supplier_by_month = gen_by_supplier_by_month.rename(columns={"tech_category": "tech"})  # Align column names
    gen_by_supplier_by_month["tech"] = gen_by_supplier_by_month["tech"].str.lower()  # Align tech names across dfs
    gen_by_supplier_by_month["year"] = gen_by_supplier_by_month["start_year_month"].dt.year
    gen_by_supplier_by_month["month_num"] = gen_by_supplier_by_month["start_year_month"].dt.month
    return gen_by_supplier_by_month


def _prepare_grid_mix_monthly(grid_mix_by_tech_by_month: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare grid mix data for scaling calculation.

    Extracts year and month information and aggregates to monthly totals.
    """
    grid_mix_monthly = grid_mix_by_tech_by_month.copy()

    # Reset the index to get datetime as a column
    grid_mix_monthly = grid_mix_monthly.reset_index()

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
    grid_mix_by_tech_by_month: pd.DataFrame, supply_gen_by_month: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate scaling factors for each technology and supplier using vectorized operations.

    Takes prepared grid mix and supplier data and calculates the proportion
    of grid generation that should be allocated to each supplier.
    """
    grid_mix_pivoted = grid_mix_by_tech_by_month.copy()
    tech_columns = [col for col in grid_mix_pivoted.columns if col.endswith("_mwh")]

    # Create a copy of the supplier data to avoid modifying the original
    supply_gen = supply_gen_by_month.copy()
    supply_gen = supply_gen.rename(columns={"month_num": "month"})  # required for joining

    # Select only the technology columns and year/month
    tech_data = grid_mix_pivoted[["year", "month"] + tech_columns].copy()

    # Convert from wide to long format
    grid_long = pd.melt(
        tech_data,
        id_vars=["year", "month"],
        value_vars=tech_columns,
        var_name="tech_column",
        value_name="grid_total_mwh",
    )

    # Standardise tech names
    grid_long["tech"] = grid_long["tech_column"].str.replace("_mwh", "")

    # Merge with supplier data
    merged_data = pd.merge(
        supply_gen[["year", "month", "tech", "current_holder", "rego_mwh"]],
        grid_long[["year", "month", "tech", "grid_total_mwh"]],
        on=["year", "month", "tech"],
        how="inner",
    )

    # Calculate scaling factors in a vectorized way
    merged_data["scaling_factor"] = merged_data["rego_mwh"] / merged_data["grid_total_mwh"]

    # Rename columns to match expected output format
    result_df = merged_data.rename(columns={"current_holder": "supplier", "rego_mwh": "supplier_mwh"})
    return result_df[["year", "month", "tech", "supplier", "grid_total_mwh", "supplier_mwh", "scaling_factor"]]


def _apply_scaling_to_hh(grid_mix_hh: pd.DataFrame, scaling_df: pd.DataFrame) -> pd.DataFrame:
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
        return pd.DataFrame(columns=["timestamp", "tech", "supplier", "generation_mwh"])

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
        value_name="generation_mwh",
    )

    # Extract technology name from column name
    grid_long["tech"] = grid_long["tech_col"].str.replace("_mwh", "")

    # Merge with scaling factors
    result = pd.merge(
        grid_long,
        scaling[["year", "month", "tech", "supplier", "scaling_factor"]],
        on=["year", "month", "tech"],
        how="inner",
    )

    # Apply scaling factors to calculate supplier-specific generation
    result["generation_mwh"] = result["generation_mwh"] * result["scaling_factor"]

    return result[["timestamp", "tech", "supplier", "generation_mwh"]]


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


def upsample_supply_monthly_gen_to_hh(
    rego_holder_reference: str,
    start_datetime: pd.Timestamp,
    end_datetime: pd.Timestamp,
    grid_mix_tech_month: pd.DataFrame,
    gen_supplier_month: pd.DataFrame,
) -> pd.DataFrame:
    # Step 1: Load and prepare the half-hourly grid mix data
    grid_mix_hh = ma.neso.grid_mix.filter(grid_mix_tech_month, start_datetime, end_datetime)

    # Step 2: Prepare data for scaling calculation (convert units, align column names, extract year and month)
    gen_supplier_month = _prepare_gen_supplier_month(rego_holder_reference, gen_supplier_month)
    grid_mix_tech_month = _prepare_grid_mix_monthly(grid_mix_tech_month)

    # Step 3: Calculate scaling factors
    scaling_df = _calculate_scaling_factors(grid_mix_tech_month, gen_supplier_month)

    # Step 4: Apply scaling to half-hourly data
    result_df = _apply_scaling_to_hh(grid_mix_hh, scaling_df)

    return result_df


@click.command()
@click.option(
    "--grid-mix-path", type=click.Path(exists=True, path_type=Path), help="Path to the grid mix data CSV file"
)
@click.option("--regos-path", type=click.Path(exists=True, path_type=Path), help="Path to the REGOS data CSV file")
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
    try:
        # Load grid mix data first to check date range
        grid_mix_hh = ma.neso.grid_mix.load(grid_mix_path)
        regos_data = ma.ofgem.regos.load(regos_path)

        # Convert click DateTime to pandas Timestamp
        start_datetime = pd.Timestamp(start_date)
        end_datetime = pd.Timestamp(end_date)

        # Validate date ranges
        try:
            _validate_date_ranges(start_datetime, end_datetime, grid_mix_hh, regos_data)
        except ValueError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)

        # Prepare inputs
        grid_mix_by_month = ma.neso.grid_mix.groupby_tech_and_month(grid_mix_hh)
        gen_by_supplier_by_month = ma.ofgem.regos.groupby_tech_month_holder(regos_data)

        # Run the upsampling
        result = upsample_supply_monthly_gen_to_hh(
            rego_holder_reference=rego_holder_reference,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            grid_mix_tech_month=grid_mix_by_month,
            gen_supplier_month=gen_by_supplier_by_month,
        )

        if output_path:
            result.to_csv(output_path)
            click.echo(f"Results saved to {output_path}")
        else:
            click.echo("Results calculated but not saved (no output path provided)")

    except Exception as e:
        import traceback

        click.echo(f"Error: {e}", err=True)
        traceback.print_exc()


if __name__ == "__main__":
    cli()
