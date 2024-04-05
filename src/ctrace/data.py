"""
The data loading facilities for the ctrace package.
"""

import polars as pl

pl.enable_string_cache()  # TODO: remove eventually with proper enums
from zipfile import ZipFile
from pathlib import Path
from polars import col as C
import logging
from typing import Optional
import pooch

from .constants import *
from .enums import *

_logger = logging.getLogger(__name__)

# The archive files released by V2 of the Climate TRACE proje
_files = [
    "agriculture.zip",
    "buildings.zip",
    "fluorinated_gases.zip",
    "forestry_and_land_use.zip",
    "fossil_fuel_operations.zip",
    "manufacturing.zip",
    "mineral_extraction.zip",
    "power.zip",
    "transportation.zip",
    "waste.zip",
]

_ct_dset = pooch.create(
    # TODO: eventually allow versioning of the dataset
    path=pooch.os_cache("climate_trace"),
    version="v2-2023",
    base_url="https://downloads.climatetrace.org/v02/sector_packages/",
    # The registry specifies the files that can be fetched
    registry={
        "agriculture.zip": "sha256:c151b397a487c80a7496d50f85202421e9b2008ba43d7b1f44bf3fa047e09ea9",
        "buildings.zip": "sha256:39d612d8da0deeaa58dcbefd80f08cfb070fd274fd5c12c7043841fdfd42c1de",
        "fluorinated_gases.zip": "sha256:463bc8b8d218f35bfa6ddc8d543ef4a082c6daf25361c72223ecc963aa742b6b",
        "forestry_and_land_use.zip": "sha256:cf184ab131172043afc93ada1488737de9e7f6b4d72705951b545027729ef19a",
        "fossil_fuel_operations.zip": "sha256:06bb1fa346c694c0b25a69a8cd1e55ec42958c639d010cd5477c65fa34ee40df",
        "manufacturing.zip": "sha256:6ead625e62dab1409ed2aa243595276ac024bdd2a4400805a4742733ecd163bd",
        "mineral_extraction.zip": "sha256:7dcf0a5f091b3be64872b615de1a30b6542243977be7bd0f5b8ebc3c7fefc171",
        "power.zip": "sha256:164d4f004cefcc6f070f394bfc1357955fa7d2597f05d67484eb3f5e7405cea0",
        "transportation.zip": "sha256:c9196bb5cd91d69f47f95fde3aa37135abb5755cfd51e5142295724332127b34",
        "waste.zip": "sha256:7c1ef405becc10478225beab2d7b5cd6a514136f66f92ce7b04947757bbbac5b",
    },
)


def read_source_emissions(
    year: Optional[int], p: Optional[Path] = None
) -> pl.DataFrame:
    """
    Read all the source emissions data from the given path.

    The year is used to filter the data on a specific year.
    If None, all the data is read.

    The path points to the directory holding the parquet files.
    If None, the data is read from the default location.

    """
    # TODO: put the proper logic
    # TODO: year logic
    assert year is not None, "Year must be specified for now"
    # TODO: currently using a cached copy of the data on my own google drive repo. This is dangerous.
    # The cached files for the emissions:
    # TODO: all the previous years. To do when the format is more stabilized.
    google_url = "https://drive.usercontent.google.com/download?id={fileid}&confirm=xxx"
    _files = {
        2020: (
            "1W457WW7PIvgjKTx9PcL2tG2KpgTYz1IH",
            "sha256:774385d865d35d3cc4b7fe55b6c2d69275ddcb926cd9c91fc1602a55c7efb30f",
        ),
        2021: (
            "1GJiWEFNb1Aoavo5NhnTiPmPXyK55nW1Q",
            "sha256:c46005c2ed1a0e91a583cedafc92d79368099649f0e15b3cde041b95a161161a",
        ),
        2022: (
            "15WDKBl9Z2xIBEJlHJmJBVGddUOhBfrGl",
            "sha256:4fc857839641dfca9b075604c1e3252f2c0e2bfa0d604cbc6286adf2f3f6278b",
        ),
    }
    (google_id, sha) = _files[year]
    local_p = pooch.retrieve(
        url=google_url.format(fileid=google_id),
        known_hash=sha,
        fname=f"climate_trace_{year}.parquet",
    )
    return pl.scan_parquet(local_p).pipe(recast_parquet)


def recast_parquet(df: pl.DataFrame) -> pl.DataFrame:
    """
    Takes a loaded polars dataframe and recasts the columns to the appropriate types.

    This information gets lost in the parquet format.
    """
    cf_cols = [
        SOURCE_TYPE,
        CAPACITY,
        CAPACITY_FACTOR,
        ACTIVITY,
        CO2_EMISSIONS_FACTOR,
        CH4_EMISSIONS_FACTOR,
        N2O_EMISSIONS_FACTOR,
        CO2_EMISSIONS,
        CH4_EMISSIONS,
        N2O_EMISSIONS,
        TOTAL_CO2E_20YRGWP,
        TOTAL_CO2E_100YRGWP,
    ]
    return df.with_columns(
        c_iso3_country.cast(iso3_enum).alias(ISO3_COUNTRY),
        c_gas.cast(gas_enum, strict=False).alias(GAS),
        # TODO: change eventually to enum
        c_original_inventory_sector.cast(pl.String).alias(ORIGINAL_INVENTORY_SECTOR),
        c_temporal_granularity.cast(temporal_granularity_enum).alias(
            TEMPORAL_GRANULARITY
        ),
    ).with_columns(
        # TODO: use an enum eventually
        *[
            C("conf_" + col_name)
            .cast(pl.Categorical, strict=False)
            .alias("conf_" + col_name)
            for col_name in cf_cols
        ]
    )


def read_country_emissions(p: Optional[Path] = None) -> pl.DataFrame:
    """Read all the country emissions data from the given path."""
    # TODO: add meta info about the provenance: ct_package and ct_file
    # So fast there is no need to store a materialized version.
    dfs = []
    for fname in _files:
        if p is None:
            _logger.debug(f"Fetching path {fname}")
            local_p = _ct_dset.fetch(fname)
        else:
            local_p = p
        _logger.debug(f"Opening path {fname} from {local_p}")
        zf = ZipFile(local_p)
        source_names = [
            n for n in zf.namelist() if n.endswith("_country_emissions.csv")
        ]
        _logger.debug(f"sources: {source_names}")
        for sname in source_names:
            _logger.debug(f"opening {fname} / {sname}")
            df = _load_country_emissions(zf.open(sname))
            df = df.with_columns(
                pl.lit(fname.replace(".zip", ""))
                .cast(pl.Categorical)
                .alias(CT_PACKAGE),
                pl.lit(sname.replace("_country_emissions.csv", ""))
                .cast(pl.Categorical)
                .alias(CT_FILE),
            )
            dfs.append(df)
    res_df = pl.concat(dfs)
    return res_df


def _load_country_emissions(fp) -> pl.DataFrame:
    dates = [START_TIME, END_TIME, CREATED_DATE, MODIFIED_DATE]
    floats = [EMISSIONS_QUANTITY]
    df = pl.read_csv(fp.read(), infer_schema_length=0)
    all_columns = [
        "iso3_country",
        "start_time",
        "end_time",
        "original_inventory_sector",
        "gas",
        "emissions_quantity",
        "emissions_quantity_units",
        "temporal_granularity",
        "created_date",
        "modified_date",
    ]
    # Check that the columns match exactly
    s1 = set(df.columns)
    s2 = set(all_columns)
    assert s1 == s2, (
        s1 - s2,
        s2 - s1,
        list(zip(sorted(df.columns), sorted(all_columns))),
    )
    df = df.select(*all_columns)
    return (
        df.with_columns(
            # Only start_time and end_time are required
            *[
                C(col_name)
                .str.to_datetime(
                    format="%Y-%m-%d %H:%M:%S%.f",
                    strict=(col_name in {START_TIME, END_TIME}),
                )
                .alias(col_name)
                for col_name in dates
            ]
        )
        .with_columns(
            c_iso3_country.cast(iso3_enum).alias(ISO3_COUNTRY),
            c_gas.cast(gas_enum, strict=False).alias(GAS),
            # TODO: causing issues
            # c_original_inventory_sector.cast(pl.Categorical).alias(ORIGINAL_INVENTORY_SECTOR),
            c_temporal_granularity.cast(temporal_granularity_enum).alias(
                TEMPORAL_GRANULARITY
            ),
        )
        .with_columns(
            *[
                C(col_name).cast(pl.Float64, strict=False).alias(col_name)
                for col_name in floats
            ]
        )
    )
