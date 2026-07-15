from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


read_dbf = SparkSubmitOperator(
    task_id="read_dbf",
    application="/opt/jobs/read_dbf.py",
    conn_id="spark_default",
    jars="/opt/jars/spark-dbf_2.12-0.2.0-assembly.jar",
    application_args=["--input", "hdfs:///data/input/report.dbf"],
)

# For a Scala 2.13 Spark runtime, use spark-dbf_2.13-0.2.0-assembly.jar.
