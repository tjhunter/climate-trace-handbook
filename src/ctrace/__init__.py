"""
ctrace: Utilities for working with Climate TRACE data.

released by the Climate TRACE Consortium.
"""

__version__ = "0.4.0"

# Just importing everything, there are too many imported constants.
from . import data
from .data import (
    read_country_emissions,
    read_source_emissions,
    recast_parquet,
)
from .enums import *
