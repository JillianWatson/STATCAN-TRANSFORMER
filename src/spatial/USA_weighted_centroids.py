import math
from dotenv import load_dotenv
from src.preprocessing.config import PROCESSED_DATA_DIR
import os
import geopandas as gpd
import requests
import pandas as pd

load_dotenv()
NASS = os.getenv("NASS_API_KEY")

COUNTY_URL = 'https://www2.census.gov/geo/tiger/TIGER2022/COUNTY/tl_2022_us_county.zip'

#calculated from online geocoder
#destination for haversine distance (Coutts AB/Sweetgrass MO Port of Entry)
AB_PORT_LAT = 49.001291
AB_PORT_LON = -111.960102

#location of output csv that includes USA states with their haversine distance from AB Border and cattle-density weighted centroids 
CENTROIDS_OUTPUT_PATH = PROCESSED_DATA_DIR / "spatial" / "usa_weighted_centroids.csv"
#location of csv that holds cattle totals for each state/county to be used with tick data in src/references/alht_data.py
CATTLE_INVENTORY_OUTPUT_PATH = PROCESSED_DATA_DIR / "spatial" / "cattle_inventories.csv"


def fetch_counties():
    """
    Load USA Census Counties from USA Census Bureau
    base crs is EPSG:3347
    """
    try:
        load_counties = gpd.read_file(COUNTY_URL)
        subset_counties = load_counties[["GEOID", "STATEFP", "NAME", "geometry"]]

        return subset_counties
    
    except Exception as e:
        print(f"Failed to fetch TIGER county boundaries: {e}")
        raise


def fetch_nass_inventory():
    """
    Load USA county cattle inventory totals to be used for density weighted centroid calculation
    """
    try:
        params = {
            "key": NASS,
            "source_desc": "CENSUS",
            "agg_level_desc": "COUNTY",
            "year": "2022",
            "commodity_desc": "CATTLE",
            "short_desc": "CATTLE, INCL CALVES - INVENTORY",
            "domain_desc": "TOTAL"
        }
        res = requests.get("https://quickstats.nass.usda.gov/api/api_GET/",
                                params=params)
        res.raise_for_status()
        data = res.json()

        if "data" not in data or len(data["data"]) == 0:
            raise ValueError("Failed to return any data. Check query parameters")
        
        df = pd.DataFrame(data["data"])
        df = df[["state_name", "state_ansi", "county_ansi", "Value"]]

        df["Value"] = df["Value"].str.replace(',', '', regex=False)
        df["Value"] = pd.to_numeric(df["Value"], errors='coerce')
        
        df = df.dropna(subset=["Value"])
        
        return df

    except Exception as e:
        print(f"Failed to fetch NASS cattle inventories per county: {e}")
        raise


# to be used on census geopandas df
def filter_contiguous_states(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    
    df = df.copy()
    #STATEFP values that represent American Territories, Alaska, and Hawaii
    removal_list = ["02", "15", "72", "78", "66", "69", "60"]
    contig_states = df[~df["STATEFP"].isin(removal_list)]

    return contig_states


# to be used on NASS pandas df
def build_nass_geoid(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df["GEOID"] = df["state_ansi"].str.zfill(2) + df["county_ansi"].str.zfill(3)

    return df


# join geodf of USA counties with NASS cattle stats to produce a geodf
# resulting geodf stats:
# shape: (2997, 8)
# dtype: int64
def join_dataframes(gdf: gpd.GeoDataFrame, df: pd.DataFrame) -> gpd.GeoDataFrame:
    result = gdf.merge(df, how="inner", on="GEOID")

    return result


def compute_county_centroids(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    projected_geodf = gdf.to_crs("EPSG:5070")
    projected_geodf["centroid"] = projected_geodf.geometry.centroid

    centroids_wgs84 = projected_geodf["centroid"].to_crs("EPSG:4326")

    projected_geodf["centroid_lat"] = centroids_wgs84.y
    projected_geodf["centroid_lon"] = centroids_wgs84.x

    return projected_geodf


# use weighted average formula to recalculate centroids
# sum(lat_i × Value_i) / sum(Value_i)
def compute_state_cattle_summary(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    df = gdf.groupby("state_name").agg( 
                weighted_lat = ("centroid_lat", lambda x: (x * gdf.loc[x.index, "Value"]).sum() / gdf.loc[x.index, "Value"].sum()),
                weighted_lon = ("centroid_lon", lambda x: (x * gdf.loc[x.index, "Value"]).sum() / gdf.loc[x.index, "Value"].sum()),
                total_inventory = ("Value", "sum")
                )

    return df


#calculates haversine distance between two locations
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dist_lat = lat2 - lat1
    dist_lon = lon2 - lon1

    a = math.sin(dist_lat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dist_lon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = 6371.0 * c

    return distance


def main():

    raw_county_df = fetch_counties()
    raw_nass_df = fetch_nass_inventory()
    
    contiguous_states = filter_contiguous_states(raw_county_df)
    nass_df = build_nass_geoid(raw_nass_df)

    joined_gdf = join_dataframes(contiguous_states, nass_df)

    print(joined_gdf.info())
    
    cattle_inventory = joined_gdf[["GEOID", "state_name", "NAME", "Value"]]
    cattle_inventory["GEOID"] = cattle_inventory["GEOID"].astype(str).str.zfill(5)
    cattle_inventory.to_csv(CATTLE_INVENTORY_OUTPUT_PATH, index=False)

    get_centroid = compute_county_centroids(joined_gdf)
    weighted_centroids = compute_state_cattle_summary(get_centroid)

    weighted_centroids["distance_to_alberta"] = weighted_centroids.apply(lambda row: haversine(row["weighted_lat"], row["weighted_lon"], AB_PORT_LAT, AB_PORT_LON), axis= 1)

    weighted_centroids.reset_index().to_csv(CENTROIDS_OUTPUT_PATH, index=False)


if __name__ == "__main__":
    main()
