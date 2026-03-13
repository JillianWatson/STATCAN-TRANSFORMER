import src.preprocessing.config as config
import src.preprocessing.data_loader as data_loader
import src.preprocessing.data_cleaner as data_cleaner
from pyspark.sql.functions import col
import src.preprocessing.commodity_mapper as commodity_mapper

def main():
    print("Initializing Spark Session...")
    spark = config.get_spark_session()

    try:

        raw_data = data_loader.load_all_files(spark)
        data_prepped = data_cleaner.clean_and_aggregate(raw_data)

        commodity_mapper.preview_commodity_mapping(data_prepped)
    
    except Exception as e:
        print(f"\n Error during preprocessing: {str(e)}")
        raise

    finally:
        config.stop_spark_session(spark)
        print(f"\n Spark Session Terminated\n")


if __name__ == "__main__":
    main()
