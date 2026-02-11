import config
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

bovine_imports = spark.read.csv("alberta_bovine_imports.csv", header=True, inferSchema=True)
bovine_exports = spark.read.csv("alberta_bovine_exports.csv", header=True, inferSchema=True)
equine_imports = spark.read.csv("alberta_equine_imports.csv", header=True, inferSchema=True)
equine_exports = spark.read.csv("alberta_equine_exports.csv", header=True, inferSchema=True)
