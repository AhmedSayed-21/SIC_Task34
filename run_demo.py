from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark_job import clean_data

spark = SparkSession.builder.master("local[2]").appName("demo").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [
    ("Ahmed", 100.0),
    ("Sara", 50.0),
    ("Omar", 200.0),
    ("Mona", 75.0),
    ("Ali", 0.0),
    ("Hassan", -50.0),
    (None, 120.0),
    ("Youssef", 300.0),
]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
])

df = spark.createDataFrame(data, schema)
result = clean_data(df)

print("\n===Original Data===")
df.show()

print("=== After Cleaning (with amount_with_tax) ===")
result.show()

removed = df.exceptAll(result.drop("amount_with_tax"))
print("=== Removed Rows (Invalid Data) ===")
removed.show()

original_rows = df.collect()
clean_rows = result.collect()
removed_rows = removed.collect()

print("original data    :", len(original_rows))
print("clean data      :", len(clean_rows))
print("removed rows   :", len(removed_rows))

assert len(original_rows) == 8
assert len(clean_rows) == 5
assert len(removed_rows) == 3
assert len(clean_rows) + len(removed_rows) == len(original_rows)
print("\nAll assertions passed. Data cleaning is successful.")

spark.stop()