from framework.spark_session import get_spark
from pyspark.sql import functions as F

# Gold job: aggregate totals by account_id.
def main():
    spark = get_spark("GoldJob")

    silver = (spark.read.format("delta")
              .load("data/silver/customers_clean"))

    signed = silver.withColumn(
        "signed_amount_cad",
        F.when(F.col("txn_type") == "DEBIT", -F.col("amount_cad"))
         .otherwise(F.col("amount_cad"))
    )

    gold = (signed
            .groupBy("account_id")
            .agg(
                F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount_cad")).otherwise(F.lit(0.0))).alias("total_credit_cad"),
                F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount_cad")).otherwise(F.lit(0.0))).alias("total_debit_cad"),
                F.sum("signed_amount_cad").alias("net_total_cad"),
                F.count("*").alias("txn_count")
            ))

    (gold.write
        .format("delta")
        .mode("overwrite")
        .save("data/gold/customers_agg"))

    spark.stop()

if __name__ == "__main__":
    main()