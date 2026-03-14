from src.preprocessing.config import PROJECT_ROOT
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv
import os

load_dotenv()
CARS_PATH = os.getenv("CARS_LOCAL_FILE")
cars = gpd.read_file(CARS_PATH)
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "spatial" / "alberta_car_centroids.csv"

print(cars.columns.tolist())
print(cars.head())
print(cars.crs) # EPSG:3347

#filter to Alberta (PRUID = 48)
alberta_cars = cars[cars["PRUID"] == "48"].copy()
print(f"\nAlberta CARs found: {len(alberta_cars)}")

#8 regions
print(alberta_cars[["CARUID", "CARENAME"]])

#re-project the geometry col
alberta_cars = alberta_cars.to_crs("EPSG:3400")
alberta_cars["centroid"] = alberta_cars.geometry.centroid

centroids_wgs84 = alberta_cars["centroid"].to_crs("EPSG:4326")

#compute centroids
alberta_cars["centroid_lat"] = centroids_wgs84.y
alberta_cars["centroid_lon"] = centroids_wgs84.x

alberta_cars[["CARUID", "CARENAME", "centroid_lat", "centroid_lon"]].to_csv(
    OUTPUT_PATH, index=False
)