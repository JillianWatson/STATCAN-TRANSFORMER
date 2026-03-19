from dotenv import load_dotenv
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

def fetch_counties():
    """
    Load USA Census Counties from USA Census Bureau
    base crs is EPSG:3347
    """
    try:
        load_counties = gpd.read_file(COUNTY_URL)
        subset_counties = load_counties[['GEOID', 'STATEFP', 'geometry']]

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
def join_dataframes(gdf: gpd.GeoDataFrame, df: pd.DataFrame) -> gpd.GeoDataFrame:
    result = gdf.merge(df, how="inner", on="GEOID")

    return result

    
def main():

    raw_county_df = fetch_counties()
    raw_nass_df = fetch_nass_inventory()
    
    contiguous_states = filter_contiguous_states(raw_county_df)
    nass_df = build_nass_geoid(raw_nass_df)

    resulting_gdf = join_dataframes(contiguous_states, nass_df)
    print(resulting_gdf.head())
    print(resulting_gdf.isna().sum())
    print(resulting_gdf.shape)


if __name__ == "__main__":
    main()
