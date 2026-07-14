import sys

from pyspark.sql import SparkSession


path = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///data/landing/dbf"

spark = (
    SparkSession.builder
    .appName("read-dbf")
    .getOrCreate()
)

df = (
    spark.read
    .format("dbf")
    .option("encoding", "cp866")
    .option("addSourceFile", "true")
    .load(path)
)

df.printSchema()
df.show(20, truncate=False)
print("count =", df.count())

spark.stop()

