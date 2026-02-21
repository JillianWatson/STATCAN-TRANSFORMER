import config
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, to_date, date_format, year, month, 
    sum as _sum, avg, count, when, coalesce, concat_ws, min as _min, max as _max
)

# Ensure all entries adhere to the date range (oct2019 - sept2024)
def validate_date_range(df: DataFrame) -> DataFrame:
    
    #get date range from the df
    date_stats = df.select(
        col("Period").alias("date")
    ).agg(
        _sum(when(col("date").isNull(), 1).otherwise(0)).alias("null_dates"),
        count("*").alias("total_records")
    ).collect()[0]
    
    print(f"Total records before filtering: {date_stats['total_records']}")
    print(f"Records with null dates: {date_stats['null_dates']}")
    
    #grab any flagged records to be able to cross reference later
    excluded = df.filter(
    (col("Period") < config.START_DATE) | 
    (col("Period") > config.END_DATE)
    )

    #filter rows for the actual date range
    df_filtered = df.filter(
        (col("Period") >= config.START_DATE) & 
        (col("Period") <= config.END_DATE)
    )
    
    filtered_count = df_filtered.count()
    date_range = df_filtered.select(
        col("Period").alias("date")
    ).agg(
        _min("date").alias("min_date"),
        _max("date").alias("max_date")
    ).collect()[0]
    
    print(f"\nDate range filter: {config.START_DATE} to {config.END_DATE}")
    print(f"Records after filtering: {filtered_count}")
    print(f"Actual date range: {date_range['min_date']} to {date_range['max_date']}")
    print(f"Records removed: {date_stats['total_records'] - filtered_count}")
    print(f"\nRecords outside date range:")
    excluded.show(truncate=False)
    
    return df_filtered


# Handle null values
def clean_data(df: DataFrame) -> DataFrame:
    
    initial_count = df.count()
    
    null_checks = df.select(
        _sum(when(col("Period").isNull(), 1).otherwise(0)).alias("null_period"),
        _sum(when(col("State").isNull(), 1).otherwise(0)).alias("null_state"),
        _sum(when(col("Value ($)").isNull(), 1).otherwise(0)).alias("null_value"),
        _sum(when(col("Quantity").isNull(), 1).otherwise(0)).alias("null_quantity"),
        _sum(when(col("Animal_Type").isNull(), 1).otherwise(0)).alias("null_animal_type"),
        _sum(when(col("Flow_Type").isNull(), 1).otherwise(0)).alias("null_flow_type")
    ).collect()[0]
    
    print("\nNull value counts before cleaning: ")
    for field, count in null_checks.asDict().items():
        if count > 0:
            print(f"  {field}: {count}")
        else:
            print(f" 0")
    
    df_clean = df.filter(
        col("Period").isNotNull() &
        col("State").isNotNull() &
        col("Value ($)").isNotNull() &
        col("Animal_Type").isNotNull() &
        col("Flow_Type").isNotNull()
    )
    
    #if null quantities found, set to 0
    df_clean = df_clean.withColumn(
        "Quantity", 
        coalesce(col("Quantity"), lit(0))
    )
    
    #ensure non-negative values
    df_clean = df_clean.filter(
        (col("Value ($)") >= 0) &
        (col("Quantity") >= 0)
    )
    
    final_count = df_clean.count()
    
    print(f"\nRecords before cleaning: {initial_count}")
    print(f"Records after cleaning: {final_count}")
    print(f"Records removed: {initial_count - final_count}")
    print(f"Data retention rate: {(final_count/initial_count)*100:.2f}%")
    
    #data quality summary
    print("\n--- Data Quality Summary ---")
    quality_stats = df_clean.select(
        count("*").alias("total_records"),
        _sum("Value ($)").alias("total_value"),
        _sum("Quantity").alias("total_quantity"),
        avg("Value ($)").alias("avg_value"),
        avg("Quantity").alias("avg_quantity")
    ).collect()[0]
    
    print(f"Total trade value: ${quality_stats['total_value']:,.2f}")
    print(f"Total quantity traded: {quality_stats['total_quantity']:,.0f} animals")
    print(f"Average trade value: ${quality_stats['avg_value']:,.2f}")
    print(f"Average quantity per record: {quality_stats['avg_quantity']:.2f} animals")
    
    return df_clean


def clean_and_aggregate(df: DataFrame) -> DataFrame:
    """
    Main pipeline function that runs all cleaning and aggregation steps.
    
    Args:
        df: raw trade dataframe from data_loader
        
    Returns:
        cleaned and aggregated monthly dataframe
    """
    print(f"\n{'='*10}")
    print("Begin data cleaning and preparation")
    
    #validate and filter date range
    df = validate_date_range(df)
    
    #clean data (handle nulls, validate values)
    df = clean_data(df)

    print("End data cleaning")
    print(f"{'='*10}")
    print(f"Final schema:")
    df.printSchema()
    
    return df


def preview_aggregated(df: DataFrame, n: int = 10) -> None:

    print(f"\nSample {n} aggregated monthly records:")
    df.orderBy("Period").show(n, truncate=False)