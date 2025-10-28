from framework.spark_session import get_spark

def test_gold_unique_per_account():
    spark = get_spark("GoldUnique")
    df = spark.read.format("delta").load("data/gold/customers_agg")
    dups = df.groupBy("account_id").count().where("count > 1")
    assert dups.count() == 0

def test_gold_totals_computable():
    spark = get_spark("GoldTotals")
    df = spark.read.format("delta").load("data/gold/customers_agg")
    cols = set(df.columns)
    assert {"total_credit_cad","total_debit_cad","net_total_cad","txn_count","account_id"}.issubset(cols)
    assert df.limit(1).count() == 1