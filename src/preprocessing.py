import config
import data_loader
import data_cleaner
from pyspark.sql.functions import col

def main():
    print("Initializing Spark Session...")
    spark = config.get_spark_session()

    try:

        raw_data = data_loader.load_all_files(spark)
        data_loader.preview(raw_data)

        data_prepped = data_cleaner.clean_and_aggregate(raw_data)
        data_cleaner.preview_aggregated(data_prepped, n=10)

        #un-comment to view unique commodities in the dataset
        #data_prepped.select("Commodity").distinct().orderBy("Commodity").show(60, truncate=False)
        data_prepped.filter(col("Commodity").like("0101.21.00 %")) \
        .select("Commodity", "Flow_Type").distinct().show(truncate=False)
    
    except Exception as e:
        print(f"\n Error during preprocessing: {str(e)}")
        raise

    finally:
        config.stop_spark_session(spark)
        print(f"\n Spark Session Terminated\n")


if __name__ == "__main__":
    main()
