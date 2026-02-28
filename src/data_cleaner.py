import config
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, date_format, year, month, 
    sum as _sum, avg, count, when, coalesce, concat_ws, min as _min, max as _max
)
import commodity_mapper

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
    
    null_checks = df.select(
        _sum(when(col("Period").isNull(), 1).otherwise(0)).alias("null_period"),
        _sum(when(col("State").isNull(), 1).otherwise(0)).alias("null_state"),
        _sum(when(col("Value ($)").isNull(), 1).otherwise(0)).alias("null_value"),
        _sum(when(col("Quantity").isNull(), 1).otherwise(0)).alias("null_quantity"),
        _sum(when(col("Animal_Type").isNull(), 1).otherwise(0)).alias("null_animal_type"),
        _sum(when(col("Flow_Type").isNull(), 1).otherwise(0)).alias("null_flow_type")
    ).collect()[0]
    
    null_fields = {field: num for field, num in null_checks.asDict().items() if num > 0 }
    if null_fields:
        print("\nNull values identified: ")
        for field, num in null_fields.items():
            print(f"  {field}: {num}")

        #remove nulls from df if found
        df = df.filter(
            col("Period").isNotNull() &
            col("State").isNotNull() &
            col("Value ($)").isNotNull() &
            col("Animal_Type").isNotNull() &
            col("Flow_Type").isNotNull()
        )
    else:
        print("  No null values found in dataframe")
    
    #if null values found, default them to 0
    df = df.withColumn(
        "Quantity", 
        coalesce(col("Quantity"), lit(0))
        ).filter((col("Value ($)") >= 0) & (col("Quantity") >=0))
    
    return df


# Add temporal features (month, year, date) needed for analyses
def add_time_features(df: DataFrame) -> DataFrame:

    df_with_time = df.withColumn("Year", year(col("Period"))).withColumn("Month", month(col("Period"))).withColumn("Year_Month", date_format(col("Period"), "yyyy-MM"))
    
    print("Added columns: Year, Month, Year_Month")
    
    #distribution by year
    print("\nRecords by year:")
    df_with_time.groupBy("Year").count().orderBy("Year").show()
    
    return df_with_time


# Aggregate multiple transactions per month into one row per unique trading series
def aggregate_trade_series(df: DataFrame) -> DataFrame:

    monthly_agg = df.groupBy(
        "Year_Month",
        "Period",
        "State",
        "Animal_Type",
        "Flow_Type",
        "Commodity_Label"
    ).agg(
        _sum("Value ($)").alias("Total_Value"),
        _sum("Quantity").alias("Total_Quantity"),
        count("*").alias("Num_Transactions")
    )

    #add column for unit price of livestock
    monthly_agg = monthly_agg.withColumn(
        "Avg_Unit_Price",
        when(col("Total_Quantity") > 0,
            col("Total_Value") / col("Total_Quantity"))
        .otherwise(0)
    )

    #unique series id
    monthly_agg = monthly_agg.withColumn(
        "SeriesID",
        concat_ws("_", col("State"), col("Animal_Type"), col("Flow_Type"), col("Commodity_Label"))
    )

    agg_count = monthly_agg.count()
    unique_series = monthly_agg.select("SeriesID").distinct().count()

    print(f"Total monthly records: {agg_count}")
    print(f"Unique trading series: {unique_series}")

    print("\nTop 10 trading series by total value:")
    monthly_agg.groupBy("SeriesID") \
               .agg(_sum("Total_Value").alias("Total_Trade_Value")) \
               .orderBy(col("Total_Trade_Value").desc()) \
               .show(10, truncate=False)

    coverage = monthly_agg.groupBy("SeriesID").agg(count("*").alias("Num_Months"))
    coverage_stats = coverage.select(
        avg("Num_Months").alias("Avg_Months"),
        count("*").alias("Total_Series")
    ).collect()[0]

    full_coverage = coverage.filter(col("Num_Months") == 60).count()

    print(f"\nAverage months per series: {coverage_stats['Avg_Months']:.2f}")
    print(f"Series with complete 60-month coverage: {full_coverage}")
    print(f"Series with partial coverage: {coverage_stats['Total_Series'] - full_coverage}")

    return monthly_agg


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

    #add temporal features
    df = add_time_features(df)

    #map commodity labels and filter out-of-scope commodities
    df = commodity_mapper.map_commodity_labels(df)

    #
    df = aggregate_trade_series(df)

    print("End data cleaning")
    print(f"{'='*10}")
    print(f"Final schema:")
    df.printSchema()
    
    return df


def preview_aggregated(df: DataFrame, n: int = 10) -> None:

    print(f"\nSample {n} aggregated monthly records:")
    df.orderBy("Period").show(n, truncate=False)