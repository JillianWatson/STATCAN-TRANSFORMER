from pyspark.sql import DataFrame
from pyspark.sql.functions import col, create_map, lit
from itertools import chain
from commodity_map import COMMODITY_MAP

def map_commodity_labels(df: DataFrame) -> DataFrame:

    initial_count = df.count()

    in_scope = {key: val for key, val in COMMODITY_MAP.items() if val is not None}
    out_scope = {key: val for key, val in COMMODITY_MAP.items() if val is None}

    #check for any commodities that were mistakenly not mapped
    known_commodities = set(COMMODITY_MAP.keys())
    distinct_commodities = {
        row["Commodity"]
        for row in df.select("Commodity").distinct().collect()
    }
    unmapped = distinct_commodities - known_commodities

    if unmapped:
        print(f"\nWarning: {len(unmapped)} unmapped commodity value(s) found:")
        for c in sorted(unmapped):
            print(f"  '{c}'")
        print("These will be dropped. Update COMMODITY_MAP to be included.")

    #spark map literal from in-scope dictionary
    expression = create_map([lit(x) for x in chain(*in_scope.items())])

    df_mapped = df.withColumn("Commodity_Label", expression[col("Commodity")])

    #drop out of scope rows
    df_mapped = df_mapped.filter(col("Commodity_Label").isNotNull())

    final_count = df_mapped.count()
    excluded_count = initial_count - final_count

    print(f"\nRecords before mapping: {initial_count}")
    print(f"Records retained: {final_count}")

    print("\nRecord counts by commodity label:")
    df_mapped.groupBy("Commodity_Label").count().orderBy("Commodity_Label").show(60, truncate=False)
    
    print(f"Records excluded (out of scope or unmapped): {excluded_count}")
    print("\nExcluded commodity labels:")
    df.filter(col("Commodity").isin(list(out_scope))).select("Commodity").distinct().orderBy("Commodity").show(60, truncate=False)

    return df_mapped


def preview_commodity_mapping(df: DataFrame) -> DataFrame:
    
    df.select(
        "Commodity", "Commodity_Label"
    ).distinct().orderBy("Commodity_Label").show(60, truncate=False)

