from framework.spark_session import get_spark
from pyspark.sql import functions as F

def test_silver_no_duplicate_txn_ids():
    spark = get_spark("SilverDup")
    df = spark.read.format("delta").load("data/silver/customers_clean")
    dups = df.groupBy("txn_id").count().where("count > 1")
    assert dups.count() == 0

def test_silver_no_null_amounts():
    spark = get_spark("SilverNulls")
    df = spark.read.format("delta").load("data/silver/customers_clean")
    assert df.where(F.col("amount_cad").isNull()).limit(1).count() == 0