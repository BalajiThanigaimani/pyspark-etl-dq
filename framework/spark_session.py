from pyspark.sql import SparkSession

def get_spark(app_name: str = "ETL"):
    """
    Create a SparkSession that works locally and on clusters.
    Includes Delta Lake integration by resolving jars via spark.jars.packages.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # comment/remove on Databricks
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )