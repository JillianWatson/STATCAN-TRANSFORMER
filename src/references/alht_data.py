import geopandas as gpd
import pandas as pd
import numpy as np
from src.preprocessing.config import PROCESSED_DATA_DIR
from dotenv import load_dotenv
import os

def compute_county_inventories(df):
    df["established_inventory"] = np.where(df["map_tick_s"] == "established", df["Value"], 0)
    df["reported_inventory"] = np.where(df["map_tick_s"] == "reported", df["Value"], 0)

    return df


def compute_state_proportions(df):

    return df

def main():
    load_dotenv()

    ALHT_PATH = os.getenv("ALHT_LOCAL_DBF")
    CATTLE_INV_PATH = PROCESSED_DATA_DIR / "spatial" / "cattle_inventories.csv"

    tick_df = gpd.read_file(ALHT_PATH)
    tick_df = tick_df[["GEOID", "map_tick_s"]]
    cattle_inv = pd.read_csv(CATTLE_INV_PATH, dtype={"GEOID":str})

    joined_df = cattle_inv.merge(tick_df, how="left", on="GEOID")

    get_county_inventory = compute_county_inventories(joined_df)
    print(get_county_inventory[get_county_inventory["reported_inventory"] > 0 ][["GEOID", "NAME", "state_name", "Value", "map_tick_s", "established_inventory", "reported_inventory"]])


if __name__ == "__main__":
    main()