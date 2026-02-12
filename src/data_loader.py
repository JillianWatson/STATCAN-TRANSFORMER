import config
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
import tempfile
import os


# loads one csv file and adds metadata columns for distinguishing animal type and import or export identification
def load_csv(spark, file_path, animal_type, flow_type):
    with open(file_path, 'r', encoding='UTF-8') as f:
        lines = f.readlines()

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='UTF-8') as tmp:
        tmp.writelines(lines[1:])
        tmp_path = tmp.name    

    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").option("encoding", "UTF-8").csv(tmp_path)
        result = df.withColumn("Animal_Type", lit(animal_type)).withColumn("Flow_Type", lit(flow_type))

        result = result.cache()
        result.count()
        
        return result
    
    finally:
        os.unlink(tmp_path)

# loads all csv files and combines them to one file
def load_all_files(spark):

    bovine_imp = load_csv(
        spark,
        config.DATA_FILES["bovine_imports"],
        animal_type="Bovine",
        flow_type="Import"
    )

    bovine_exp = load_csv(
        spark,
        config.DATA_FILES["bovine_exports"],
        animal_type="Bovine",
        flow_type="Export"
    )

    equine_imp = load_csv(
        spark,
        config.DATA_FILES["equine_imports"],
        animal_type="Equine",
        flow_type="Import"
    )

    equine_exp = load_csv(
        spark,
        config.DATA_FILES["equine_exports"],
        animal_type="Equine",
        flow_type="Export"
    )

    all_trade = bovine_imp.union(bovine_exp).union(equine_imp).union(equine_exp)

    return all_trade

#display data
def preview(df, n=5):
    df.printSchema()
    df.show(n, truncate=False)