import config
import data_loader

def main():
    print("Initializing Spark Session...")
    spark = config.get_spark_session()

    try:

        raw_data = data_loader.load_all_files(spark)
        data_loader.preview(raw_data)
    
    except Exception as e:
        print(f"\n Error during preprocessing: {str(e)}")
        raise

    finally:
        config.stop_spark_session(spark)
        print(f"\n Spark Session Terminated\n")


if __name__ == "__main__":
    main()
