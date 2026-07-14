import sys

from pyspark.sql import SparkSession


jar_path = sys.argv[1]
fixture_path = sys.argv[2]

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("spark-dbf-pyspark-test")
    .config("spark.jars", jar_path)
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

try:
    df = (
        spark.read
        .format("dbf")
        .option("encoding", "cp866")
        .option("addSourceFile", "true")
        .load(fixture_path)
    )
    assert df.count() == 1
    assert df.rdd.getNumPartitions() >= 1
    assert "_source_file" in df.columns
    df.printSchema()
    df.show(truncate=False)
finally:
    spark.stop()

