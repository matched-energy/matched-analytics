from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pandera as pa
from dateutil.relativedelta import relativedelta

from ma.ofgem.enums import RegoCompliancePeriod, RegoScheme, RegoStatus
from ma.utils.enums import SupplyTechEnum
from ma.utils.pandas import ColumnSchema as CS
from ma.utils.pandas import DataFrameAsset
from ma.utils.pandas import DateTimeEngine as DTE


class RegosRaw(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict( 
        accreditation_number        =CS(check=pa.Column(str)),
        station_name                =CS(check=pa.Column(str)),
        station_tic                 =CS(check=pa.Column(float)),
        scheme                      =CS(check=pa.Column(str)),
        country                     =CS(check=pa.Column(str)),
        technology_group            =CS(check=pa.Column(str)),
        generation_type             =CS(check=pa.Column(str, nullable=True)),
        output_period               =CS(check=pa.Column(str)),
        certificate_count           =CS(check=pa.Column(int)),
        certificate_start           =CS(check=pa.Column(str)),
        certificate_end             =CS(check=pa.Column(str)),
        mwh_per_certificate         =CS(check=pa.Column(float)),
        issue_date                  =CS(check=pa.Column(DTE(dayfirst=True))),
        certificate_status          =CS(check=pa.Column(str)),
        status_date                 =CS(check=pa.Column(DTE(dayfirst=True))),
        current_holder              =CS(check=pa.Column(str)),
        company_registration_number =CS(check=pa.Column(str, nullable=True)),
    )
    from_file_with_index = False
    from_file_skiprows = 4 
    from_file_header = None
    # fmt: on

    tech_categories = {
        "Photovoltaic": SupplyTechEnum.SOLAR,
        "Hydro": SupplyTechEnum.HYDRO,
        "Wind": SupplyTechEnum.WIND,
        "Biomass": SupplyTechEnum.BIOMASS,
        "Biogas": SupplyTechEnum.BIOMASS,
        "Landfill Gas": SupplyTechEnum.BIOMASS,
        "On-shore Wind": SupplyTechEnum.WIND,
        "Hydro 20MW DNC or less": SupplyTechEnum.HYDRO,
        "Fuelled": SupplyTechEnum.BIOMASS,
        "Off-shore Wind": SupplyTechEnum.WIND,
        "Micro Hydro": SupplyTechEnum.HYDRO,
        "Biomass 50kW DNC or less": SupplyTechEnum.BIOMASS,
    }

    def transform_to_regos_processed(self) -> RegosProcessed:
        regos = self.df
        regos["rego_gwh"] = regos["mwh_per_certificate"] * regos["certificate_count"] / 1e3
        regos["tech"] = regos["technology_group"].map(self.tech_categories)
        # TODO - quantify how much volume has status 'unknown'
        regos["tech"] = regos["technology_group"].map(self.tech_categories).fillna(SupplyTechEnum.UNKNOWN)
        regos = self.add_output_period_columns(regos)
        return RegosProcessed(regos)

    @classmethod
    def parse_date_range(cls, date_str: str) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
        # e.g. 01/09/2022 - 30/09/2022
        if "/" in date_str:
            start, end = date_str.split(" - ")
            start_dt = pd.to_datetime(start, dayfirst=True)
            end_dt = pd.to_datetime(end, dayfirst=True) + +np.timedelta64(1, "D")

        # e.g. 2022 - 2023: we presume this should be taken to cover a compliance year
        elif " - " in date_str:
            year_start, year_end = date_str.split(" - ")
            start_dt = pd.to_datetime("01/04/" + year_start, dayfirst=True)
            end_dt = pd.to_datetime("31/03/" + year_end, dayfirst=True) + np.timedelta64(1, "D")

        # e.g. May-2022
        elif "-" in date_str:
            month_year = pd.to_datetime(date_str, format="%b-%Y")
            start_dt = month_year.replace(day=1)
            end_dt = month_year + pd.offsets.MonthEnd(0) + np.timedelta64(1, "D")

        else:
            raise ValueError(r"Invalid date string {}".format(date_str))

        # Check start/end dates are first/last days of month (can be in different months)
        if (
            start_dt.day != 1
            or (end_dt - pd.Timedelta(days=1)).day != ((end_dt - pd.Timedelta(days=1)) + pd.offsets.MonthEnd(0)).day
        ):
            raise ValueError(f"{date_str} days are not the first and last days of month")

        period_duration = relativedelta(end_dt, start_dt)

        # Check period_duration is less than or equal to 1 year
        if period_duration.years > 1 or period_duration.months > 12:
            raise ValueError(f"{date_str} period_duration is more than 12 months")

        months_difference = period_duration.years * 12 + period_duration.months

        return start_dt, end_dt, months_difference

    @classmethod
    def add_output_period_columns(cls, regos: pd.DataFrame) -> pd.DataFrame:
        column_names = ["start_year_month", "end_year_month", "period_months"]
        period_columns = pd.DataFrame(columns=column_names)
        if not regos.empty:
            period_columns = regos["output_period"].apply(lambda x: pd.Series(cls.parse_date_range(x)))
            period_columns.columns = pd.Index(column_names)
        return pd.concat([regos, period_columns], axis=1)


class RegosProcessed(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict( 
        accreditation_number        =CS(check=pa.Column(str)),
        station_name                =CS(check=pa.Column(str)),
        station_tic                 =CS(check=pa.Column(float)),
        scheme                      =CS(check=pa.Column(str)),
        country                     =CS(check=pa.Column(str)),
        technology_group            =CS(check=pa.Column(str)),
        generation_type             =CS(check=pa.Column(str, nullable=True)),
        output_period               =CS(check=pa.Column(str)),
        certificate_count           =CS(check=pa.Column(int)),
        certificate_start           =CS(check=pa.Column(str)),
        certificate_end             =CS(check=pa.Column(str)),
        mwh_per_certificate         =CS(check=pa.Column(float)),
        issue_date                  =CS(check=pa.Column(DTE(dayfirst=True))),
        certificate_status          =CS(check=pa.Column(str)),
        status_date                 =CS(check=pa.Column(DTE(dayfirst=True))),
        current_holder              =CS(check=pa.Column(str)),
        company_registration_number =CS(check=pa.Column(str, nullable=True)),
        rego_gwh                    =CS(check=pa.Column(float)),
        tech                        =CS(check=pa.Column(str)),
        start_year_month            =CS(check=pa.Column(DTE(dayfirst=False))),
        end_year_month              =CS(check=pa.Column(DTE(dayfirst=False))),
        period_months               =CS(check=pa.Column(int)),
    )
    # fmt: on

    def filter(
        self,
        holders: Optional[list[str]] = None,
        statuses: Optional[list[RegoStatus]] = None,
        schemes: Optional[list[RegoScheme]] = [RegoScheme.REGO],
        reporting_period: Optional[RegoCompliancePeriod] = None,
    ) -> RegosProcessed:
        filters = []
        if holders:
            filters.append((self.df["current_holder"].isin(holders)))

        if statuses:
            filters.append(self.df["certificate_status"].isin(statuses))

        if schemes:
            filters.append(self.df["scheme"].isin(schemes))

        if reporting_period:
            start_date, end_date = reporting_period.date_range
            start_year_month = pd.Timestamp(start_date)
            end_year_month = pd.Timestamp(end_date)
            period_filter = (self.df["start_year_month"] >= start_year_month) & (
                self.df["end_year_month"] < end_year_month
            )
            filters.append(period_filter)

        if not filters:
            return RegosProcessed(self.df)
        else:
            return RegosProcessed(self.df.loc[np.logical_and.reduce(filters)])

    def groupby_station(self) -> pd.DataFrame:
        # Note: this function could become 'transform_to_regos_by_station' if/when we introduce
        # RegosByStation(DataFrameAsset)

        # Check columns that are expected to be unique
        unique_count_by_station = self.df.groupby("station_name").agg(
            accredition_number_unique=("accreditation_number", "nunique"),
            company_registration_number_unique=("company_registration_number", "nunique"),
            technology_group_unique=("technology_group", "nunique"),
            generation_type_unique=("generation_type", "nunique"),
            tech_category_unique=("tech", "nunique"),
        )
        non_unique_by_station = unique_count_by_station[(unique_count_by_station > 1).any(axis=1)]
        if not non_unique_by_station.empty:
            raise AssertionError(
                f"Stations {list(non_unique_by_station.index)} have non-unique values {non_unique_by_station}"
            )

        # Groupby
        regos_by_station = (
            self.df.groupby("station_name")
            .agg(
                accredition_number=("accreditation_number", "first"),
                company_registration_number=("company_registration_number", "first"),
                rego_gwh=("rego_gwh", "sum"),
                technology_group=("technology_group", "first"),
                generation_type=("generation_type", "first"),
                tech=("tech", "first"),
            )
            .sort_values(by="rego_gwh", ascending=False)
        )

        # Station output as a fraction of a whole
        regos_by_station["percentage_of_whole"] = (
            regos_by_station["rego_gwh"] / regos_by_station["rego_gwh"].sum() * 100
        )

        return regos_by_station.reset_index()

    def _expand_multi_month_certificates(self) -> pd.DataFrame:
        """
        Expand certificates that span multiple months into separate rows, with
        the generation amount evenly distributed across each month.
        """
        expanded_rows = []

        for _, row in self.df.iterrows():
            # If there's only one month, keep the row as is
            if row["period_months"] == 1:
                expanded_rows.append(row.to_dict())
            else:
                # Create a row for each month in the period
                start_date = row["start_year_month"]
                months_to_generate = row["period_months"]

                # Calculate the per-month generation
                per_month_gwh = row["rego_gwh"] / months_to_generate

                # Generate rows for each month in the period
                for month_offset in range(months_to_generate):
                    month_date = start_date + pd.DateOffset(months=month_offset)
                    new_row = row.to_dict().copy()
                    new_row["start_year_month"] = month_date  # Update start_year_month
                    new_row["rego_gwh"] = per_month_gwh  # Distribute generation evenly
                    expanded_rows.append(new_row)

        return pd.DataFrame(expanded_rows)

    def transform_to_regos_by_tech_month_holder(self) -> RegosByTechMonthHolder:
        # Extract month from start_year_month for grouping
        regos = self.df

        # Expand certificates that span multiple months
        # Check if period_months exists and there are records with multi-month periods
        if "period_months" in regos.columns and (regos["period_months"] > 1).any():
            regos = self._expand_multi_month_certificates()

        regos["month"] = regos["start_year_month"].dt.to_period("M")

        # Groupby tech, month, and holder
        regos_by_tech_month_holder = (
            regos.groupby(["tech", "month", "current_holder"])
            .agg(
                rego_gwh=("rego_gwh", "sum"),
                station_count=("station_name", "nunique"),
            )
            .sort_values(by=["tech", "month", "current_holder"])
        )

        results = regos_by_tech_month_holder.reset_index().set_index("month")
        results.index = results.index.to_timestamp()  # type: ignore
        return RegosByTechMonthHolder(results)


class RegosByTechMonthHolder(DataFrameAsset):
    # fmt: off
    schema: Dict[str, CS] = dict( 
        month             =CS(check=pa.Index(DTE(dayfirst=False))),
        tech              =CS(check=pa.Column(str)),
        current_holder    =CS(check=pa.Column(str)),
        rego_gwh          =CS(check=pa.Column(float)),
        station_count     =CS(check=pa.Column(int)),
    )
    # fmt: on

    def filter(
        self,
        holders: List[str],
    ) -> RegosByTechMonthHolder:
        return RegosByTechMonthHolder(self.df[self.df["current_holder"].isin(holders)])
