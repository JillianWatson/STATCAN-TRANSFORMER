from pyspark.sql import SparkSession
from pathlib import Path

# project directory paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
RAW_DATA_DIR = DATA_DIR / "raw"

# data file paths
DATA_FILES = {
    "bovine_imports": RAW_DATA_DIR / "bovine_imports.csv",
    "bovine_exports": RAW_DATA_DIR / "bovine_exports.csv",
    "equine_imports": RAW_DATA_DIR / "equine_imports.csv",
    "equine_exports": RAW_DATA_DIR / "equine_exports.csv"
}

# (constants) Temporal range for analysis
START_DATE = "2019-10-01"
END_DATE = "2024-09-30"

# spark configuration
SPARK_CONFIG = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.sql.shuffle.partitions": "10"
}

# functions
def get_spark_session(app_name="Alberta-Livestock-Trade"):
    builder = SparkSession.builder.appName(app_name)
    
    #apply the config parameters
    for key,val in SPARK_CONFIG.items():
        builder = builder.config(key, val)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def ensure_directories():
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


def stop_spark_session(spark):
    spark.stop()