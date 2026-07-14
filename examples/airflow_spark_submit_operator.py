from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


read_dbf = SparkSubmitOperator(
    task_id="read_dbf",
    application="/path/read_dbf.py",
    jars="/path/spark-dbf_2.13-0.1.0-assembly.jar",
    conn_id="spark_default",
)

