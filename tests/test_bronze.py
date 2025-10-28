from framework.spark_session import get_spark
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

def test_bronze_schema():
    spark = get_spark("BronzeTest")
    df = spark.read.format("delta").load("data/bronze/customers")

    expected = StructType([
        StructField("txn_id",      LongType(),   True),
        StructField("account_id",  LongType(),   True),
        StructField("txn_type",    StringType(), True),
        StructField("amount_cad",  DoubleType(), True),
        StructField("city",        StringType(), True),
        StructField("txn_date",    StringType(), True),
    ])
    assert set((f.name, f.dataType.simpleString(), f.nullable) for f in df.schema) ==                set((f.name, f.dataType.simpleString(), f.nullable) for f in expected)

def test_bronze_has_rows():
    spark = get_spark("BronzeCount")
    df = spark.read.format("delta").load("data/bronze/customers")
    assert df.limit(1).count() == 1