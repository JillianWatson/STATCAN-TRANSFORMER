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
        print(load_counties.crs)
        subset_counties = load_counties[['GEOID', 'STATEFP', 'NAME', 'geometry']]
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
        print(df.head())

        return df

    except Exception as e:
        print(f"Failed to fetch NASS cattle inventories per county: {e}")
        raise
    
def main():

    fetch_counties()
    fetch_nass_inventory()


if __name__ == "__main__":
    main()
