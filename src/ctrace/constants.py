from typing import Literal

from polars import col as C

# Extra columns for the country emissions:
EMISSIONS_QUANTITY_UNITS = "emissions_quantity_units"
c_emissions_quantity_units = C("emissions_quantity_units")

# Extra columns for the climate trace metadata:
# TODO: rename to ct_sector and ct_subsector. This is how it is called in the CT documentation.
SECTOR = "sector"
SUBSECTOR = "subsector"

c_sector = C("sector")
c_subsector = C("subsector")

# Gas names:
CO2 = "co2"
CH4 = "ch4"
N2O = "n2o"
CO2E_100YR = "co2e_100yr"
CO2E_20YR = "co2e_20yr"

GAS_LIST = [CO2, CH4, N2O, CO2E_100YR, CO2E_20YR]

Gas = Literal["co2", "ch4", "n2o", "co2e_100yr", "co2e_20yr"]

## ***** CONFIDENCE LEVELS *****
VERY_HIGH = "very high"
HIGH = "high"
MEDIUM = "medium"
LOW = "low"
VERY_LOW = "very low"

CONFIDENCES = [VERY_HIGH, HIGH, MEDIUM, LOW, VERY_LOW]

Confidence = Literal["very high", "high", "medium", "low", "very low"]


# Extra columns for the confidence levels:
# Gen code:
# cols = ("co2_emissions_factor	ch4_emissions_factor	n2o_emissions_factor	co2_emissions	ch4_emissions	n2o_emissions	total_co2e_20yrgwp"
#         "\t"
#        "total_co2e_100yrgwp"
#        )
# for col in cols.split():
#     print(f"{col.upper()} = '{col}'")
# print("\n\n")
# for col in cols.split():
#     print(f"c_{col} = C('{col}')")
# print("\n\n")
# for col in cols.split():
#     print(f"CONF_{col.upper()} = 'conf_{col}'")
# print("\n\n")
# for col in cols.split():
#     print(f"c_conf_{col} = C('conf_{col}')")

CO2_EMISSIONS_FACTOR = "co2_emissions_factor"
CH4_EMISSIONS_FACTOR = "ch4_emissions_factor"
N2O_EMISSIONS_FACTOR = "n2o_emissions_factor"
CO2_EMISSIONS = "co2_emissions"
CH4_EMISSIONS = "ch4_emissions"
N2O_EMISSIONS = "n2o_emissions"
TOTAL_CO2E_20YRGWP = "total_co2e_20yrgwp"
TOTAL_CO2E_100YRGWP = "total_co2e_100yrgwp"


c_co2_emissions_factor = C("co2_emissions_factor")
c_ch4_emissions_factor = C("ch4_emissions_factor")
c_n2o_emissions_factor = C("n2o_emissions_factor")
c_co2_emissions = C("co2_emissions")
c_ch4_emissions = C("ch4_emissions")
c_n2o_emissions = C("n2o_emissions")
c_total_co2e_20yrgwp = C("total_co2e_20yrgwp")
c_total_co2e_100yrgwp = C("total_co2e_100yrgwp")


CONF_CO2_EMISSIONS_FACTOR = "conf_co2_emissions_factor"
CONF_CH4_EMISSIONS_FACTOR = "conf_ch4_emissions_factor"
CONF_N2O_EMISSIONS_FACTOR = "conf_n2o_emissions_factor"
CONF_CO2_EMISSIONS = "conf_co2_emissions"
CONF_CH4_EMISSIONS = "conf_ch4_emissions"
CONF_N2O_EMISSIONS = "conf_n2o_emissions"
CONF_TOTAL_CO2E_20YRGWP = "conf_total_co2e_20yrgwp"
CONF_TOTAL_CO2E_100YRGWP = "conf_total_co2e_100yrgwp"


c_conf_co2_emissions_factor = C("conf_co2_emissions_factor")
c_conf_ch4_emissions_factor = C("conf_ch4_emissions_factor")
c_conf_n2o_emissions_factor = C("conf_n2o_emissions_factor")
c_conf_co2_emissions = C("conf_co2_emissions")
c_conf_ch4_emissions = C("conf_ch4_emissions")
c_conf_n2o_emissions = C("conf_n2o_emissions")
c_conf_total_co2e_20yrgwp = C("conf_total_co2e_20yrgwp")
c_conf_total_co2e_100yrgwp = C("conf_total_co2e_100yrgwp")


## ***** SUBSECTORS *****

SUBSECTORS = [
    "aluminum",
    "bauxite-mining",
    "biological-treatment-of-solid-waste-and-biogenic",
    "cement",
    "chemicals",
    "coal-mining",
    "copper-mining",
    "cropland-fires",
    "domestic-aviation",
    "domestic-shipping",
    "electricity-generation",
    "enteric-fermentation-cattle-feedlot",
    "enteric-fermentation-cattle-pasture",
    "enteric-fermentation-other",
    "fluorinated-gases",
    "forest-land-clearing",
    "forest-land-degradation",
    "forest-land-fires",
    "incineration-and-open-burning-of-waste",
    "international-aviation",
    "international-shipping",
    "iron-mining",
    "manure-left-on-pasture-cattle",
    "manure-management-cattle-feedlot",
    "manure-management-other",
    "net-forest-land",
    "net-shrubgrass",
    "net-wetland",
    "oil-and-gas-production-and-transport",
    "oil-and-gas-refining",
    "other-agricultural-soil-emissions",
    "other-energy-use",
    "other-fossil-fuel-operations",
    "other-manufacturing",
    "other-onsite-fuel-usage",
    "other-transport",
    "petrochemicals",
    "pulp-and-paper",
    "railways",
    "removals",
    "residential-and-commercial-onsite-fuel-usage",
    "rice-cultivation",
    "road-transportation",
    "rock-quarrying",
    "sand-quarrying",
    "shrubgrass-fires",
    "solid-fuel-transformation",
    "solid-waste-disposal",
    "steel",
    "synthetic-fertilizer-application",
    "wastewater-treatment-and-discharge",
    "water-reservoirs",
    "wetland-fires",
]


def _code_subsector():
    for s in SUBSECTORS:
        s2 = s.upper().replace("-", "_")
        print(f"{s2} = '{s}'")

    print("\n\n")
    sub = ", ".join([f"'{s}'" for s in SUBSECTORS])
    print(f"SubSector = Literal[{sub}]")
    print("\n\n")


ALUMINUM = "aluminum"
BAUXITE_MINING = "bauxite-mining"
BIOLOGICAL_TREATMENT_OF_SOLID_WASTE_AND_BIOGENIC = (
    "biological-treatment-of-solid-waste-and-biogenic"
)
CEMENT = "cement"
CHEMICALS = "chemicals"
COAL_MINING = "coal-mining"
COPPER_MINING = "copper-mining"
CROPLAND_FIRES = "cropland-fires"
DOMESTIC_AVIATION = "domestic-aviation"
DOMESTIC_SHIPPING = "domestic-shipping"
ELECTRICITY_GENERATION = "electricity-generation"
ENTERIC_FERMENTATION_CATTLE_FEEDLOT = "enteric-fermentation-cattle-feedlot"
ENTERIC_FERMENTATION_CATTLE_PASTURE = "enteric-fermentation-cattle-pasture"
ENTERIC_FERMENTATION_OTHER = "enteric-fermentation-other"
FLUORINATED_GASES = "fluorinated-gases"
FOREST_LAND_CLEARING = "forest-land-clearing"
FOREST_LAND_DEGRADATION = "forest-land-degradation"
FOREST_LAND_FIRES = "forest-land-fires"
INCINERATION_AND_OPEN_BURNING_OF_WASTE = "incineration-and-open-burning-of-waste"
INTERNATIONAL_AVIATION = "international-aviation"
INTERNATIONAL_SHIPPING = "international-shipping"
IRON_MINING = "iron-mining"
MANURE_LEFT_ON_PASTURE_CATTLE = "manure-left-on-pasture-cattle"
MANURE_MANAGEMENT_CATTLE_FEEDLOT = "manure-management-cattle-feedlot"
MANURE_MANAGEMENT_OTHER = "manure-management-other"
NET_FOREST_LAND = "net-forest-land"
NET_SHRUBGRASS = "net-shrubgrass"
NET_WETLAND = "net-wetland"
OIL_AND_GAS_PRODUCTION_AND_TRANSPORT = "oil-and-gas-production-and-transport"
OIL_AND_GAS_REFINING = "oil-and-gas-refining"
OTHER_AGRICULTURAL_SOIL_EMISSIONS = "other-agricultural-soil-emissions"
OTHER_ENERGY_USE = "other-energy-use"
OTHER_FOSSIL_FUEL_OPERATIONS = "other-fossil-fuel-operations"
OTHER_MANUFACTURING = "other-manufacturing"
OTHER_ONSITE_FUEL_USAGE = "other-onsite-fuel-usage"
OTHER_TRANSPORT = "other-transport"
PETROCHEMICALS = "petrochemicals"
PULP_AND_PAPER = "pulp-and-paper"
RAILWAYS = "railways"
REMOVALS = "removals"
RESIDENTIAL_AND_COMMERCIAL_ONSITE_FUEL_USAGE = (
    "residential-and-commercial-onsite-fuel-usage"
)
RICE_CULTIVATION = "rice-cultivation"
ROAD_TRANSPORTATION = "road-transportation"
ROCK_QUARRYING = "rock-quarrying"
SAND_QUARRYING = "sand-quarrying"
SHRUBGRASS_FIRES = "shrubgrass-fires"
SOLID_FUEL_TRANSFORMATION = "solid-fuel-transformation"
SOLID_WASTE_DISPOSAL = "solid-waste-disposal"
STEEL = "steel"
SYNTHETIC_FERTILIZER_APPLICATION = "synthetic-fertilizer-application"
WASTEWATER_TREATMENT_AND_DISCHARGE = "wastewater-treatment-and-discharge"
WATER_RESERVOIRS = "water-reservoirs"
WETLAND_FIRES = "wetland-fires"


SubSector = Literal[
    "aluminum",
    "bauxite-mining",
    "biological-treatment-of-solid-waste-and-biogenic",
    "cement",
    "chemicals",
    "coal-mining",
    "copper-mining",
    "cropland-fires",
    "domestic-aviation",
    "domestic-shipping",
    "electricity-generation",
    "enteric-fermentation-cattle-feedlot",
    "enteric-fermentation-cattle-pasture",
    "enteric-fermentation-other",
    "fluorinated-gases",
    "forest-land-clearing",
    "forest-land-degradation",
    "forest-land-fires",
    "incineration-and-open-burning-of-waste",
    "international-aviation",
    "international-shipping",
    "iron-mining",
    "manure-left-on-pasture-cattle",
    "manure-management-cattle-feedlot",
    "manure-management-other",
    "net-forest-land",
    "net-shrubgrass",
    "net-wetland",
    "oil-and-gas-production-and-transport",
    "oil-and-gas-refining",
    "other-agricultural-soil-emissions",
    "other-energy-use",
    "other-fossil-fuel-operations",
    "other-manufacturing",
    "other-onsite-fuel-usage",
    "other-transport",
    "petrochemicals",
    "pulp-and-paper",
    "railways",
    "removals",
    "residential-and-commercial-onsite-fuel-usage",
    "rice-cultivation",
    "road-transportation",
    "rock-quarrying",
    "sand-quarrying",
    "shrubgrass-fires",
    "solid-fuel-transformation",
    "solid-waste-disposal",
    "steel",
    "synthetic-fertilizer-application",
    "wastewater-treatment-and-discharge",
    "water-reservoirs",
    "wetland-fires",
]

## ***** SECTORS *****

SECTORS = [
    "agriculture",
    "buildings",
    "fluorinated_gases",
    "forestry_and_land_use",
    "fossil_fuel_operations",
    "manufacturing",
    "mineral_extraction",
    "power",
    "transportation",
    "waste",
]


def _code_sector():
    for s in SECTORS:
        s2 = s.upper().replace("-", "_")
        print(f"{s2} = '{s}'")

    print("\n\n")
    sub = ", ".join([f"'{s}'" for s in SECTORS])
    print(f"Sector = Literal[{sub}]")
    print("\n\n")


AGRICULTURE = "agriculture"
BUILDINGS = "buildings"
FLUORINATED_GASES = "fluorinated_gases"
FORESTRY_AND_LAND_USE = "forestry_and_land_use"
FOSSIL_FUEL_OPERATIONS = "fossil_fuel_operations"
MANUFACTURING = "manufacturing"
MINERAL_EXTRACTION = "mineral_extraction"
POWER = "power"
TRANSPORTATION = "transportation"
WASTE = "waste"


Sector = Literal[
    "agriculture",
    "buildings",
    "fluorinated_gases",
    "forestry_and_land_use",
    "fossil_fuel_operations",
    "manufacturing",
    "mineral_extraction",
    "power",
    "transportation",
    "waste",
]

## ***** ORIGINAL INVENTORY SECTORS *****

ORIGINAL_INVENTORY_SECTORS = [
    "aluminum",
    "bauxite-mining",
    "biological-treatment-of-solid-waste-and-biogenic",
    "cement",
    "chemicals",
    "coal-mining",
    "copper-mining",
    "cropland-fires",
    "domestic-aviation",
    "domestic-shipping",
    "electricity-generation",
    "enteric-fermentation-cattle-feedlot",
    "enteric-fermentation-cattle-pasture",
    "enteric-fermentation-other",
    "fluorinated-gases",
    "forest-land-clearing",
    "forest-land-degradation",
    "forest-land-fires",
    "incineration-and-open-burning-of-waste",
    "international-aviation",
    "international-shipping",
    "iron-mining",
    "manure-left-on-pasture-cattle",
    "manure-management-cattle-feedlot",
    "manure-management-other",
    "net-forest-land",
    "net-shrubgrass",
    "net-wetland",
    "oil-and-gas-production-and-transport",
    "oil-and-gas-refining",
    "other-agricultural-soil-emissions",
    "other-energy-use",
    "other-fossil-fuel-operations",
    "other-manufacturing",
    "other-onsite-fuel-usage",
    "other-transport",
    "petrochemicals",
    "pulp-and-paper",
    "railways",
    "removals",
    "residential-and-commercial-onsite-fuel-usage",
    "rice-cultivation",
    "road-transportation",
    "rock-quarrying",
    "sand-quarrying",
    "shrubgrass-fires",
    "solid-fuel-transformation",
    "solid-waste-disposal",
    "steel",
    "synthetic-fertilizer-application",
    "wastewater-treatment-and-discharge",
    "water-reservoirs",
    "wetland-fires",
]


def _code_original_inventory_sector():
    for s in ORIGINAL_INVENTORY_SECTORS:
        s2 = s.upper().replace("-", "_")
        print(f"{s2} = '{s}'")

    print("\n\n")
    sub = ", ".join([f"'{s}'" for s in ORIGINAL_INVENTORY_SECTORS])
    print(f"OriginalInventorySector = Literal[{sub}]")
    print("\n\n")


ALUMINUM = "aluminum"
BAUXITE_MINING = "bauxite-mining"
BIOLOGICAL_TREATMENT_OF_SOLID_WASTE_AND_BIOGENIC = (
    "biological-treatment-of-solid-waste-and-biogenic"
)
CEMENT = "cement"
CHEMICALS = "chemicals"
COAL_MINING = "coal-mining"
COPPER_MINING = "copper-mining"
CROPLAND_FIRES = "cropland-fires"
DOMESTIC_AVIATION = "domestic-aviation"
DOMESTIC_SHIPPING = "domestic-shipping"
ELECTRICITY_GENERATION = "electricity-generation"
ENTERIC_FERMENTATION_CATTLE_FEEDLOT = "enteric-fermentation-cattle-feedlot"
ENTERIC_FERMENTATION_CATTLE_PASTURE = "enteric-fermentation-cattle-pasture"
ENTERIC_FERMENTATION_OTHER = "enteric-fermentation-other"
FLUORINATED_GASES = "fluorinated-gases"
FOREST_LAND_CLEARING = "forest-land-clearing"
FOREST_LAND_DEGRADATION = "forest-land-degradation"
FOREST_LAND_FIRES = "forest-land-fires"
INCINERATION_AND_OPEN_BURNING_OF_WASTE = "incineration-and-open-burning-of-waste"
INTERNATIONAL_AVIATION = "international-aviation"
INTERNATIONAL_SHIPPING = "international-shipping"
IRON_MINING = "iron-mining"
MANURE_LEFT_ON_PASTURE_CATTLE = "manure-left-on-pasture-cattle"
MANURE_MANAGEMENT_CATTLE_FEEDLOT = "manure-management-cattle-feedlot"
MANURE_MANAGEMENT_OTHER = "manure-management-other"
NET_FOREST_LAND = "net-forest-land"
NET_SHRUBGRASS = "net-shrubgrass"
NET_WETLAND = "net-wetland"
OIL_AND_GAS_PRODUCTION_AND_TRANSPORT = "oil-and-gas-production-and-transport"
OIL_AND_GAS_REFINING = "oil-and-gas-refining"
OTHER_AGRICULTURAL_SOIL_EMISSIONS = "other-agricultural-soil-emissions"
OTHER_ENERGY_USE = "other-energy-use"
OTHER_FOSSIL_FUEL_OPERATIONS = "other-fossil-fuel-operations"
OTHER_MANUFACTURING = "other-manufacturing"
OTHER_ONSITE_FUEL_USAGE = "other-onsite-fuel-usage"
OTHER_TRANSPORT = "other-transport"
PETROCHEMICALS = "petrochemicals"
PULP_AND_PAPER = "pulp-and-paper"
RAILWAYS = "railways"
REMOVALS = "removals"
RESIDENTIAL_AND_COMMERCIAL_ONSITE_FUEL_USAGE = (
    "residential-and-commercial-onsite-fuel-usage"
)
RICE_CULTIVATION = "rice-cultivation"
ROAD_TRANSPORTATION = "road-transportation"
ROCK_QUARRYING = "rock-quarrying"
SAND_QUARRYING = "sand-quarrying"
SHRUBGRASS_FIRES = "shrubgrass-fires"
SOLID_FUEL_TRANSFORMATION = "solid-fuel-transformation"
SOLID_WASTE_DISPOSAL = "solid-waste-disposal"
STEEL = "steel"
SYNTHETIC_FERTILIZER_APPLICATION = "synthetic-fertilizer-application"
WASTEWATER_TREATMENT_AND_DISCHARGE = "wastewater-treatment-and-discharge"
WATER_RESERVOIRS = "water-reservoirs"
WETLAND_FIRES = "wetland-fires"


OriginalInventorySector = Literal[
    "aluminum",
    "bauxite-mining",
    "biological-treatment-of-solid-waste-and-biogenic",
    "cement",
    "chemicals",
    "coal-mining",
    "copper-mining",
    "cropland-fires",
    "domestic-aviation",
    "domestic-shipping",
    "electricity-generation",
    "enteric-fermentation-cattle-feedlot",
    "enteric-fermentation-cattle-pasture",
    "enteric-fermentation-other",
    "fluorinated-gases",
    "forest-land-clearing",
    "forest-land-degradation",
    "forest-land-fires",
    "incineration-and-open-burning-of-waste",
    "international-aviation",
    "international-shipping",
    "iron-mining",
    "manure-left-on-pasture-cattle",
    "manure-management-cattle-feedlot",
    "manure-management-other",
    "net-forest-land",
    "net-shrubgrass",
    "net-wetland",
    "oil-and-gas-production-and-transport",
    "oil-and-gas-refining",
    "other-agricultural-soil-emissions",
    "other-energy-use",
    "other-fossil-fuel-operations",
    "other-manufacturing",
    "other-onsite-fuel-usage",
    "other-transport",
    "petrochemicals",
    "pulp-and-paper",
    "railways",
    "removals",
    "residential-and-commercial-onsite-fuel-usage",
    "rice-cultivation",
    "road-transportation",
    "rock-quarrying",
    "sand-quarrying",
    "shrubgrass-fires",
    "solid-fuel-transformation",
    "solid-waste-disposal",
    "steel",
    "synthetic-fertilizer-application",
    "wastewater-treatment-and-discharge",
    "water-reservoirs",
    "wetland-fires",
]

## ***** COLUMNS *****

# The code was generated by the following snippet:
# Gen the code:
# cols = "source_id	iso3_country	original_inventory_sector	start_time	end_time	temporal_granularity	gas	emissions_quantity	emissions_factor	emissions_factor_units	capacity	capacity_units	capacity_factor	activity	activity_units	created_date	modified_date	source_name	source_type	lat	lon	other1	other2	other3	other4 other5 other6 other7 other8 other9 other10 other11 other12 other1_def	other2_def	other3_def	other4_def other5_def other6_def other7_def other8_def other9_def other10_def other11_def other12_def geometry_ref"
# for col in cols.split():
#     print(f"{col.upper()} = '{col}'")
# print("\nall_columns=[" + ",".join([col.upper() for col in cols.split()]) + "]\n")
# for col in cols.split():
#     print(f"c_{col} = C('{col}')")

SOURCE_ID = "source_id"
ISO3_COUNTRY = "iso3_country"
ORIGINAL_INVENTORY_SECTOR = "original_inventory_sector"
START_TIME = "start_time"
END_TIME = "end_time"
TEMPORAL_GRANULARITY = "temporal_granularity"
GAS = "gas"
EMISSIONS_QUANTITY = "emissions_quantity"
EMISSIONS_FACTOR = "emissions_factor"
EMISSIONS_FACTOR_UNITS = "emissions_factor_units"
CAPACITY = "capacity"
CAPACITY_UNITS = "capacity_units"
CAPACITY_FACTOR = "capacity_factor"
ACTIVITY = "activity"
ACTIVITY_UNITS = "activity_units"
CREATED_DATE = "created_date"
MODIFIED_DATE = "modified_date"
SOURCE_NAME = "source_name"
SOURCE_TYPE = "source_type"
LAT = "lat"
LON = "lon"
OTHER1 = "other1"
OTHER2 = "other2"
OTHER3 = "other3"
OTHER4 = "other4"
OTHER5 = "other5"
OTHER6 = "other6"
OTHER7 = "other7"
OTHER8 = "other8"
OTHER9 = "other9"
OTHER10 = "other10"
OTHER11 = "other11"
OTHER12 = "other12"
OTHER1_DEF = "other1_def"
OTHER2_DEF = "other2_def"
OTHER3_DEF = "other3_def"
OTHER4_DEF = "other4_def"
OTHER5_DEF = "other5_def"
OTHER6_DEF = "other6_def"
OTHER7_DEF = "other7_def"
OTHER8_DEF = "other8_def"
OTHER9_DEF = "other9_def"
OTHER10_DEF = "other10_def"
OTHER11_DEF = "other11_def"
OTHER12_DEF = "other12_def"
GEOMETRY_REF = "geometry_ref"

all_columns = [
    SOURCE_ID,
    ISO3_COUNTRY,
    ORIGINAL_INVENTORY_SECTOR,
    START_TIME,
    END_TIME,
    TEMPORAL_GRANULARITY,
    GAS,
    EMISSIONS_QUANTITY,
    EMISSIONS_FACTOR,
    EMISSIONS_FACTOR_UNITS,
    CAPACITY,
    CAPACITY_UNITS,
    CAPACITY_FACTOR,
    ACTIVITY,
    ACTIVITY_UNITS,
    CREATED_DATE,
    MODIFIED_DATE,
    SOURCE_NAME,
    SOURCE_TYPE,
    LAT,
    LON,
    OTHER1,
    OTHER2,
    OTHER3,
    OTHER4,
    OTHER5,
    OTHER6,
    OTHER7,
    OTHER8,
    OTHER9,
    OTHER10,
    OTHER11,
    OTHER12,
    OTHER1_DEF,
    OTHER2_DEF,
    OTHER3_DEF,
    OTHER4_DEF,
    OTHER5_DEF,
    OTHER6_DEF,
    OTHER7_DEF,
    OTHER8_DEF,
    OTHER9_DEF,
    OTHER10_DEF,
    OTHER11_DEF,
    OTHER12_DEF,
    GEOMETRY_REF,
]

c_source_id = C("source_id")
c_iso3_country = C("iso3_country")
c_original_inventory_sector = C("original_inventory_sector")
c_start_time = C("start_time")
c_end_time = C("end_time")
c_temporal_granularity = C("temporal_granularity")
c_gas = C("gas")
c_emissions_quantity = C("emissions_quantity")
c_emissions_factor = C("emissions_factor")
c_emissions_factor_units = C("emissions_factor_units")
c_capacity = C("capacity")
c_capacity_units = C("capacity_units")
c_capacity_factor = C("capacity_factor")
c_activity = C("activity")
c_activity_units = C("activity_units")
c_created_date = C("created_date")
c_modified_date = C("modified_date")
c_source_name = C("source_name")
c_source_type = C("source_type")
c_lat = C("lat")
c_lon = C("lon")
c_other1 = C("other1")
c_other2 = C("other2")
c_other3 = C("other3")
c_other4 = C("other4")
c_other5 = C("other5")
c_other6 = C("other6")
c_other7 = C("other7")
c_other8 = C("other8")
c_other9 = C("other9")
c_other10 = C("other10")
c_other11 = C("other11")
c_other12 = C("other12")
c_other1_def = C("other1_def")
c_other2_def = C("other2_def")
c_other3_def = C("other3_def")
c_other4_def = C("other4_def")
c_other5_def = C("other5_def")
c_other6_def = C("other6_def")
c_other7_def = C("other7_def")
c_other8_def = C("other8_def")
c_other9_def = C("other9_def")
c_other10_def = C("other10_def")
c_other11_def = C("other11_def")
c_other12_def = C("other12_def")
c_geometry_ref = C("geometry_ref")
