"""
The data loading facilities for the ctrace package.

The main functions are `read_country_emissions` and `read_source_emissions`.
"""

import functools
import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple, TypeVar, Union
from zipfile import ZipFile

import huggingface_hub  # type: ignore
import huggingface_hub.file_download  # type: ignore
import polars as pl
import pooch  # type: ignore
from polars import col as C

from .constants import *
from .enums import *

_logger = logging.getLogger(__name__)

# A union type for the polars dataframes
Frame = TypeVar("Frame", pl.DataFrame, pl.LazyFrame)


# The archive files released by V3 of the Climate TRACE project
# TODO: this is only CO2E_100YR, add the other gas later.
# TODO: ask CT to provide the sha256 checksums directly, it is annoying to precalculate myself and less secure.
_files = {
    "ch4": {
        "agriculture.zip": "sha256:17408922af42ef9c6e662ebfd64752d8559accfe7258b2a72fb99fcb96ecbf63",
        "buildings.zip": "sha256:027cd2cf50ced82d86fbe6fefe4e77894afc23b9ef68cf3d1cb98b0b8a3ee186",
        "fluorinated_gases.zip": "sha256:ef0b53830e5ed55c590ac07b5e9c8797fec6c371245ca28ed3326d162fdd3b7e",
        "forestry-and-land-use.zip": "sha256:76660b3a30a0cee85281895f7bb7ad1b5bf2df51925b1b382d43c84a7ceb08ec",
        "fossil-fuel-operations.zip": "sha256:70d46c8fce9de01f375ab696e35f001fb825482d0a8e87a66b19bd2cf6c4db71",
        "manufacturing.zip": "sha256:2115f3a0bf5206bf3b2a901e30a0668bbb84d1b5db64565fe5b996cf59c80c34",
        "mineral-extraction.zip": "sha256:4cf8245f62828d01868360066d1d20ceb3ec7082bd05e5714c0dd7bb239dc544",
        "power.zip": "sha256:b5d1fb94190132866c8f5ccef538cc414233932dcaee046071f453fe58ca2003",
        "transportation.zip": "sha256:992b525d00d953170fda89d4f381c576da1435a1fec588595da132154507d693",
        "waste.zip": "sha256:492091e28e8ea8c5a1b3c197c5a1dacdce5391a4ee2b5c934319f852448101a9",
    },
    "n2o": {
        "agriculture.zip": "sha256:0d77cd43d47bfc76f1b7de60f64e7c3b4202ea5d69bd381408d03c8340cabb58",
        "buildings.zip": "sha256:e9fedcda3ce8f9cf94bc81cf9c7ec04235150799ce39c19107acf67e52c6847c",
        "fluorinated-gases.zip": "sha256:aadcdcd962355db93784784e91a801533d5ac20da3dfa8daccd5e8e31c7bcbad",
        "forestry-and-land-use.zip": "sha256:6f76414c0cfd2e17d354c09cf3510cdca8c012cf1a91e2ddc044ff7e5e316c78",
        "fossil-fuel-operations.zip": "sha256:fd7abb79d747ac408ce96cd81ab73eaaed225bdf5c56fc97f14637abcee922fd",
        "manufacturing.zip": "sha256:0a4a4fdbbe2f1fe2b757dddee829b2abbe0ebebd331a46e97a9e7e07992ef7fc",
        "mineral-extraction.zip": "sha256:e30aabe4088581714d59295767bcd3debeb7de43f6c9e5f96632275acc3d2b54",
        "power.zip": "sha256:d9bbbc0faada288d9b8eca0d2158cdb69ce426c9a341b3fa83c6f5f5eaad7cb9",
        "transportation.zip": "sha256:3debbfeccb4cee32ecb3225186f65f1ed06cf442fe45f277ec8b71456439c1da",
        "waste.zip": "sha256:721661d9a8706595116ee1ab7c6ccc6a22c692f105cb9e51be38f4551abe6287",
    },
    "co2": {
        "agriculture.zip": "sha256:fdd01821f1ccee672650c83bbf42e0675d752ec96c0261b5e1963f91b201b32f",
        "buildings.zip": "sha256:5f1386632116f4e58df96aa6b228870034e3531b976f85616aa6a746d660d425",
        "fluorinated-gases.zip": "sha256:ca01fa9c4549d46ba101bdbc097d70198c0967dd00e5e17e6fe8ae4505ec12ff",
        "forestry-and-land-use.zip": "sha256:b1ab80083389c934cae62ea41614233cd55eeca05750d0debb9e4661aa849196",
        "fossil-fuel-operations.zip": "sha256:cca1ac9adee693e90aa3c0215f8bac14958f995febf2e4a3e8425991d95d7054",
        "manufacturing.zip": "sha256:0e402700ad9b37b0c51b0c592b42111370e5a5b54dec36f01ac1531f2929b18f",
        "mineral-extraction.zip": "sha256:a37aa5e5811cf649e2f958da40df2ecb53f156e8516a8d8c6b9575fefbe6a873",
        "power.zip": "sha256:36d08445199f10402559bea41866d8179eb89818ba380bea12b69e0438b96773",
        "transportation.zip": "sha256:063954edbf13fb0bc0dd1621ecb28b3238451b8acf0a808534adc39f905a49e2",
        "waste.zip": "sha256:54b9f9c6a924c3dafefea39848e835d4fb24bb427e352540c0615ba2dc8e4186",
    },
    "co2e_100yr": {
        "agriculture.zip": "sha256:683122af6819687ea2b95277a2615e58577521e0aae449f3c47fbef25ee8c20a",
        "buildings.zip": "sha256:02728636c1cd13e99cf701e325b1ee0805e3e35fa7f14ea435f71f2f9a394323",
        "fluorinated-gases.zip": "sha256:16a20b7d4c16d37455170fe95dfeffc256846fb03db7a7d95a171e6bc6476004",
        "forestry-and-land-use.zip": "sha256:aa318afc41e7cf70497148d64e6b5abcdfd23daad75fca16c40ab4228baf7195",
        "fossil-fuel-operations.zip": "sha256:eb720e38d990179e6ef2bc1c9949ab368d1c681ee6f65550c71c4e5e9416c70f",
        "manufacturing.zip": "sha256:a4f28ef3a7def17ec493550289ea00bb3093526f7546d65d51d54973ea5aaacd",
        "mineral-extraction.zip": "sha256:0d2fe9e61753c60bbc7688b99ed794eac2fe7e808a1829ae07105c7f8371e815",
        "power.zip": "sha256:aa5189c413a0d9e2c606952ff2271bbb0abafc6a35632612374dc3a44d3417ac",
        "transportation.zip": "sha256:90117bbce92793b6ea205ff18f50220e709fa5229d0a66789f29ec53f6064ca7",
        "waste.zip": "sha256:092034816686c1170c269aacf23ccbaf6abeb6f38be7eaf4d07ff24b536cc3cd",
    },
}

# The version of the dataset
# The first version "v2" is the official release from Climate TRACE.
# The second (year) is the release year, and then the snapshot date.
# The third (ct) is the version of this dataset as generated by the ctrace package.
version = "v3-2024-ct5"
# The years covered by the dataset.
# The range could be larger but it will be extended based on interest.
years = list(range(2021, 2025))


def _create_pooch(gas: Gas) -> pooch.Pooch:
    # Some files are misnamed by the Climate TRACE project.
    # TODO: open a ticket to fix the names.
    urls = {}
    for n, _ in _files[gas].items():
        n2 = n.replace("-", "_")
        urls[n] = (
            "https://downloads.climatetrace.org/v3/sector_packages/{gas}/{file}".format(
                gas=gas, file=n2
            )
        )
    return pooch.create(
        # TODO: eventually allow versioning of the dataset
        path=pooch.os_cache(f"climate_trace_{gas}"),
        version="v3-2024",
        urls=urls,
        registry={n: None for n in _files[gas]},  # TODO
        base_url="",
    )


@functools.cache
def _ct_dset(gas: Gas) -> pooch.Pooch:
    return _create_pooch(gas)


def read_source_emissions(
    gas: Union[Gas, List[Gas]],
    year: Union[int, List[int], None] = None,
    p: Optional[Path] = None,
) -> pl.LazyFrame:
    """
    Read all the source emissions data from the given path, assuming
    the source emissions have already been compacted into parquet files.

    If no path is provided, the data is read from a pre-compacted file
    stored on the internet.

    If you want to build the parquet files directly, use the `load_sources_parquet`
    functions.

    The year is used to filter the data on a specific year.
    If None, all the data is read.

    The path points to the directory holding the parquet files.
    If None, the data is read from the default location.

    """
    ys = _check_year(year)
    gases = _check_gas(gas)
    fname = "{version}/climate_trace-sources_{version}_{year}_{gas}.parquet"
    if p is None:
        local_paths = [
            huggingface_hub.file_download.hf_hub_download(
                repo_id="tjhunter/climate-trace",
                filename=fname.format(year=year_, version=version, gas=gas),
                repo_type="dataset",
            )
            for year_ in ys
            for gas in gases
        ]
    else:
        local_paths = [
            Path(p) / fname.format(gas=gas, year=year_, version=version)
            for year_ in ys
            for gas in gases
        ]
    return pl.concat(
        [
            pl.scan_parquet(local_p).pipe(recast_parquet, conf=True)
            for local_p in local_paths
        ]
    )


def load_source_compact(p: Optional[Path] = None) -> Tuple[pl.LazyFrame, List[Path]]:
    """
    Reads the source emissions data from the given path and creates
    a compacted view in Polars.
    """
    # Polars still has some issues with memory, especially because we are
    # joining the confidence while scanning the data.
    # The current strategy is to read eagerly each subsector, write them
    # to parquet in temporary files and then reload the full dataframe lazily..
    # TODO: replace by a proper temp directory
    tmp_dir = Path(tempfile.gettempdir())
    data_files: List[Path] = []
    p = p or True
    for gas in GAS_LIST:
        for fname in _files[gas]:
            _logger.debug(f"Opening path {fname} {gas}")
            (zf, _) = _get_zip(p, gas, fname)
            source_names_l = [n for n in zf.namelist() if n.endswith("sources.csv")]
            # The zip files do not seem to have been created correctly and some
            # entries are duplicated.
            source_names = sorted(set(source_names_l))
            _logger.debug(f"sources:{gas}: {fname} -> {source_names}")
            for sname in source_names:
                _logger.debug(f"opening {fname} / {sname}")
                tmp_name = (
                    tmp_dir
                    / gas
                    / sname.replace(".csv", ".parquet").replace("DATA/", "")
                )

                c_name = sname.replace(
                    "_emissions_sources.csv", "_emissions_sources_confidence.csv"
                )
                _logger.debug(f"opening {fname} / {sname} and {c_name}")
                df = _load_source_conf(zf.open(sname), zf.open(c_name))
                # Remove all the empty strings, this provides better statistics and
                # removes unnecessary string compression.
                df = df.with_columns(
                    [
                        pl.when(pl.col(pl.Utf8).str.len_bytes() == 0)
                        .then(None)
                        .otherwise(pl.col(pl.Utf8))
                        .name.keep()
                    ]
                )
                _logger.debug(f"writing {tmp_name}")
                # Create directories if they do not exist
                tmp_name.parent.mkdir(parents=True, exist_ok=True)
                # Making large groups because they will be broken into smaller
                # during the split by year.
                df.write_parquet(
                    tmp_name,
                    compression="zstd",
                    statistics=True,
                    row_group_size=2_000_000,
                    use_pyarrow=True,
                )
                data_files.append(tmp_name)
                _logger.debug(f"wrote {tmp_name}")
    dfs: List[pl.LazyFrame] = []
    for tmp_name in data_files:
        _logger.debug(f"scan {tmp_name}")
        df_ = pl.scan_parquet(tmp_name)
        df_ = df_.pipe(recast_parquet, conf=True)
        dfs.append(df_)
    res_df: pl.LazyFrame = pl.concat(dfs)
    return res_df, data_files


def _load_csv(
    filter,
    cols: Optional[List[str]] = None,
    p: Optional[Path] = None,
) -> pl.DataFrame:
    """
    Returns a subset of the CSV files, all merged into a single dataframe.

    There is no interpretation of the column types, they are all strings.

    Data is loaded eagerly, which consumes a lot of memory.

    This is mostly useful for debugging purposes, not part of the official API.
    """
    dfs: List[pl.DataFrame] = []
    for fname in _files:
        (zf, _) = _get_zip(p, "co2", fname)
        source_names = [n for n in zf.namelist() if filter(fname, n)]
        _logger.debug(f"sources: {source_names}")
        for sname in source_names:
            _logger.debug(f"opening {fname} / {sname}")
            df = pl.read_csv(zf.open(sname), infer_schema_length=0)
            if cols is not None:
                df = df.select(cols)
            df = df.with_columns(
                pl.lit(fname.replace(".zip", "")).alias("zip_name"),
                pl.lit(sname.replace(".csv", "")).alias("file_name"),
            )
            dfs.append(df)
    # diagonal -> make a union of all columns (pandas-like behavior)
    res_df: pl.DataFrame = pl.concat(dfs, how="diagonal")
    return res_df


def _get_zip(p: Union[Path, bool, None], gas: Gas, name: str) -> Tuple[ZipFile, Path]:
    if p == True:
        local_p = _ct_dset(gas).fetch(name)
    else:
        local_p = p / gas / name
    return (ZipFile(local_p), local_p)


def _load_source_conf(s_fp, c_fp) -> pl.DataFrame:
    s_df = _load_sources(s_fp)
    c_df = _load_source_confidence(c_fp)
    # Workaround: some confidence records are duplicated:
    c_df = (
        c_df.group_by(START_TIME, END_TIME, ISO3_COUNTRY, SOURCE_ID)
        .agg(pl.first("*"))
        .shrink_to_fit()
    )
    return s_df.join(
        c_df.drop(["created_date", "modified_date", SECTOR, SUBSECTOR]),
        on=[START_TIME, END_TIME, ISO3_COUNTRY, SOURCE_ID, GAS],
        how="left",
    ).shrink_to_fit()


def _load_source_confidence(fp) -> pl.DataFrame:
    dates = [START_TIME, END_TIME, CREATED_DATE, MODIFIED_DATE]
    cf_cols = [
        SOURCE_TYPE,
        CAPACITY,
        CAPACITY_FACTOR,
        ACTIVITY,
        EMISSIONS_FACTOR,
        EMISSIONS_QUANTITY,
    ]
    # Even with Polars, loading large CSV files is memory intensive.
    _logger.debug(f"loading source conf {fp}")
    # TODO: make it lazy
    df = pl.read_csv(
        fp.read(),
        has_header=True,
        infer_schema_length=0,
        infer_schema=False,
        try_parse_dates=False,
        rechunk=False,
        low_memory=True,
        batch_size=100_000,
    ).shrink_to_fit()  # .lazy()
    _logger.debug(f"columns: {df.columns}")
    sels = (
        [_parse_date(col_name) for col_name in dates]
        + [
            c_iso3_country.cast(iso3_enum).alias(ISO3_COUNTRY),
            c_source_id.cast(pl.UInt64).alias(SOURCE_ID),
            c_sector.cast(sector_enum).alias(SECTOR),
            c_subsector.cast(subsector_enum).alias(SUBSECTOR),
            c_gas.cast(gas_enum).alias(GAS),
        ]
        + [
            C(col_name)
            .cast(confidence_level_enum, strict=False)
            .alias("conf_" + col_name)
            for col_name in cf_cols
        ]
    )
    df = df.select(*sels).shrink_to_fit()
    _logger.debug("source conf: ")
    # For debugging
    return df  # .limit(1_000)


def _load_sources(fp) -> pl.DataFrame:
    dates = [START_TIME, END_TIME, CREATED_DATE, MODIFIED_DATE]
    uint64s = [SOURCE_ID]
    floats = [
        EMISSIONS_QUANTITY,
        EMISSIONS_FACTOR,
        CAPACITY,
        CAPACITY_FACTOR,
        ACTIVITY,
        LAT,
        LON,
    ]
    # Even with Polars, loading large CSV files is memory intensive.
    # The following options are used to reduce the memory footprint.
    # TODO: make it lazy
    _logger.debug(f"loading source {fp}")
    init_schema = {
        "source_id": pl.UInt64,
        "iso3_country": pl.Categorical,
        "gas": pl.Categorical,
        "sector": pl.Categorical,
        "subsector": pl.Categorical,
        "emissions_quantity": pl.Float64,
        "emissions_factor": pl.Float64,
        "capacity": pl.Float64,
        "capacity_factor": pl.Float64,
        "activity": pl.Float64,
        "lat": pl.Float64,
        "lon": pl.Float64,
    }
    df = pl.read_csv(
        fp.read(),
        has_header=True,
        infer_schema_length=0,
        infer_schema=False,
        schema_overrides=init_schema,
        try_parse_dates=False,
        low_memory=True,
        rechunk=False,
        batch_size=100_000,
    ).shrink_to_fit()
    num_other = 12
    check_cols = (
        [
            ACTIVITY,
            ACTIVITY_UNITS,
            EMISSIONS_QUANTITY,
            EMISSIONS_FACTOR,
            EMISSIONS_FACTOR_UNITS,
            CAPACITY_UNITS,
            CAPACITY,
            CAPACITY_FACTOR,
            GEOMETRY_REF,
            LAT,
            LON,
            ORIGINAL_INVENTORY_SECTOR,
        ]
        + [f"other{i}" for i in range(1, num_other + 1)]
        + [f"other{i}_def" for i in range(1, num_other + 1)]
    )
    for col_name in check_cols:
        if col_name not in df.columns:
            df = df.with_columns(
                pl.lit(None).cast(pl.String, strict=False).alias(col_name)
            )
    # Check that the columns match exactly
    # Some of the files have extra columns, we ignore them for now.
    s1 = set(df.columns)
    s2 = set(all_columns)
    assert s2.issubset(s1), (
        s1 - s2,
        s2 - s1,
        list(zip(sorted(df.columns), sorted(all_columns))),
    )
    df = df.select(*all_columns).shrink_to_fit()
    _logger.debug("loaded str")
    df = (
        df.with_columns(
            # Only start_time and end_time are required
            *[_parse_date(col_name) for col_name in dates]
        )
        .with_columns(
            c_iso3_country.cast(iso3_enum).alias(ISO3_COUNTRY),
            c_gas.cast(gas_enum).alias(GAS),
            c_temporal_granularity.cast(temporal_granularity_enum).alias(
                TEMPORAL_GRANULARITY
            ),
            c_original_inventory_sector.cast(original_inventory_sector_enum).alias(
                ORIGINAL_INVENTORY_SECTOR
            ),
            c_sector.cast(sector_enum).alias(SECTOR),
            c_subsector.cast(subsector_enum).alias(SUBSECTOR),
        )
        .with_columns(
            *[
                C(col_name).cast(pl.Float64, strict=False).alias(col_name)
                for col_name in floats
            ]
        )
        .with_columns(
            *[C(col_name).cast(pl.UInt64).alias(col_name) for col_name in uint64s]
        )
        # This is for debugging the memory consumption, remove eventually
        .limit(1_000_000_000)
        .shrink_to_fit()
    )
    _logger.debug("recast")
    return df


def recast_parquet(df: Frame, conf: bool) -> Frame:
    """
    Takes a loaded polars dataframe and recasts the columns to the appropriate types.

    This information gets lost in the parquet format.
    """
    df = df.with_columns(
        c_iso3_country.cast(iso3_enum).alias(ISO3_COUNTRY),
        c_gas.cast(gas_enum, strict=False).alias(GAS),
        # c_original_inventory_sector.cast(original_inventory_sector_enum).alias(
        #     ORIGINAL_INVENTORY_SECTOR
        # ),
        c_temporal_granularity.cast(temporal_granularity_enum).alias(
            TEMPORAL_GRANULARITY
        ),
        c_subsector.cast(subsector_enum).alias(SUBSECTOR),
        c_sector.cast(sector_enum).alias(SECTOR),
    )
    if EMISSIONS_QUANTITY_UNITS in df.collect_schema().names():
        # There is no emissions quantity for the sources (it is all defined in metric tonnes).
        # This applies to the country emissions.
        # TODO: make it an enum? it as always tonnes it seems for countries
        df = df.with_columns(
            c_emissions_quantity_units.cast(pl.Categorical).alias(
                EMISSIONS_QUANTITY_UNITS
            )
        )
    if conf:
        cf_cols = [
            SOURCE_TYPE,
            CAPACITY,
            CAPACITY_FACTOR,
            ACTIVITY,
            EMISSIONS_FACTOR,
            EMISSIONS_QUANTITY,
        ]
        df = df.with_columns(
            *[
                C("conf_" + col_name)
                .cast(confidence_level_enum, strict=False)
                .alias("conf_" + col_name)
                for col_name in cf_cols
            ]
        )
    return df


def read_country_emissions(
    gas: Optional[Gas] = GAS_LIST,
    archive_path: Union[Path, bool, None] = None,
    parquet_path: Optional[Path] = None,
) -> pl.DataFrame:
    """Read all the country emissions data from the given path."""
    # with V3 there is enough data that a materialized view is useful.
    dfs = []
    gases = _check_gas(gas)

    def _read_parquet(p: Path) -> pl.DataFrame:
        return (
            pl.read_parquet(p)
            .pipe(recast_parquet, conf=False)
            .filter(pl.col(GAS).is_in(gases))
        )

    if parquet_path is not None:
        return _read_parquet(parquet_path)
    elif archive_path is not None:
        for gas_ in gases:
            for fname in _files[gas_]:
                (zf, local_p) = _get_zip(archive_path, gas_, fname)
                _logger.debug(f"Opening path {fname} from {local_p}")
                source_names = [
                    n for n in zf.namelist() if n.endswith("_country_emissions.csv")
                ]
                # TODO(V3) There seems to be duplicate entries (but not data) in the zip files.
                source_names = sorted(set(source_names))
                _logger.debug(f"sources: {source_names}")
                for sname in source_names:
                    _logger.debug(f"opening {fname} / {sname}")
                    df = _load_country_emissions(zf.open(sname))
                    df = df.pipe(recast_parquet, conf=False)
                    dfs.append(df)
    else:
        # By default, load from from HF.
        fname = "climate-trace-countries-{version}.parquet"
        local_path = huggingface_hub.file_download.hf_hub_download(
            repo_id="tjhunter/climate-trace",
            filename=fname.format(version=version),
            repo_type="dataset",
        )
        return _read_parquet(local_path)

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
        "gas",
        "sector",
        "subsector",
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
    return df.with_columns(
        # Only start_time and end_time are required
        *[_parse_date(col_name) for col_name in dates]
    ).with_columns(
        *[
            C(col_name).cast(pl.Float64, strict=False).alias(col_name)
            for col_name in floats
        ]
    )


def _check_year(y: Union[int, List[int], None]) -> List[int]:
    if y is None or y == []:
        y = years
    if isinstance(y, int):
        y = [y]
    for year in y:
        assert year in years, f"Year {year} not a valid year. Valid years are {years}"
    return y


def _check_gas(g: Union[Gas, List[Gas]]) -> List[Gas]:
    if isinstance(g, str):
        g = [g]
    for gas in g:
        assert gas in GAS_LIST, f"Gas {gas} not a valid gas. Valid gases are {GAS_LIST}"
    return g


def _parse_date(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name)
        .str.to_datetime(
            strict=(col_name in {START_TIME, END_TIME}),
            format="%Y-%m-%d %H:%M:%S",
            time_unit="ms",
            time_zone="UTC",
        )
        .alias(col_name)
    )
