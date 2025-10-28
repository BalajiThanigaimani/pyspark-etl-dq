from framework.spark_session import get_spark
from pyspark.sql import functions as F, Window

# Silver job: clean & standardize. Remove null amounts, de-duplicate on txn_id (latest per date).
def main():
    spark = get_spark("SilverJob")

    bronze = (spark.read.format("delta")
              .load("data/bronze/customers"))

    w = Window.partitionBy("txn_id").orderBy(F.col("txn_date").desc())
    silver = (bronze
              .withColumn("rn", F.row_number().over(w))
              .where(F.col("rn") == 1)
              .drop("rn"))

    silver = (silver
              .where(F.col("amount_cad").isNotNull())
              .where(F.col("txn_type").isin("DEBIT", "CREDIT")))

    (silver.write
        .format("delta")
        .mode("overwrite")
        .save("data/silver/customers_clean"))

    spark.stop()

if __name__ == "__main__":
    main()