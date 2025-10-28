import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from delta import configure_spark_with_delta_pip

# Clean previous Bronze output
shutil.rmtree("data/bronze/customers", ignore_errors=True)

builder = SparkSession.builder.appName("BronzeJob") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

schema = StructType([
    StructField("txn_id", LongType(), nullable=False),
    StructField("account_id", LongType(), nullable=False),
    StructField("txn_type", StringType(), nullable=False),
    StructField("amount_cad", DoubleType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("txn_date", StringType(), nullable=False),
])

# ✅ Apply schema strictly
df = spark.read.schema(schema).json("data/raw/customers.json")

# ✅ Write strictly to Bronze
df.write.format("delta").mode("overwrite").save("data/bronze/customers")

spark.stop()
