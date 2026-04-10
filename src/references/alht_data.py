import geopandas as gpd
from src.preprocessing.config import PROJECT_ROOT
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()
ALHT_PATH = os.getenv("ALHT_LOCAL_DBF")
df = gpd.read_file(ALHT_PATH)

print(df.columns.tolist())

for col in ["map_tick_s", "tick_statu", "p_status_t", "p_status_c", "dec_25_tic"]:
    print(f"\n{col}:")
    print(df[col].value_counts(dropna=False))