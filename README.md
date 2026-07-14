# spark-dbf

Spark DataSource for reading DBF files with Apache Spark 3.5 and Scala 2.13.

This project is a Spark 3.5 / Scala 2.13 compatible fork of
[mraad/spark-dbf](https://github.com/mraad/spark-dbf).

## Compatibility

| Library | Spark | Scala | Java |
|---|---|---|---|
| 0.1.x | 3.5.2-3.5.4 | 2.13 | 8/11/17 |

## Download

Download the assembly JAR from:

https://github.com/DAChernikov/spark-dbf/releases

Expected file name:

```text
spark-dbf_2.13-0.1.0-assembly.jar
```

## Usage

```python
df = (
    spark.read
    .format("dbf")
    .option("encoding", "cp866")
    .load("hdfs:///data/input/file.dbf")
)
```

The full provider package name also works:

```python
df = spark.read.format("com.github.dachernikov.spark.dbf").load("hdfs:///data/input/file.dbf")
```

## spark-submit

```bash
spark-submit \
  --jars /path/spark-dbf_2.13-0.1.0-assembly.jar \
  read_dbf.py
```

## Directory

```python
df = (
    spark.read
    .format("dbf")
    .option("recursiveFileLookup", "true")
    .option("addSourceFile", "true")
    .load("hdfs:///data/input/dbf/")
)
```

## Options

| Option | Default | Description |
|---|---:|---|
| `encoding` | `UTF-8` | DBF character encoding |
| `ignoreDeleted` | `true` | Skip records marked as deleted |
| `recursiveFileLookup` | `false` | Read DBF files recursively |
| `addSourceFile` | `false` | Add `_source_file` column |
| `columnNameCase` | `preserve` | `preserve`, `lower` or `upper` |
| `trimStrings` | `true` | Trim trailing spaces |

`corruptRecordMode` is `FAILFAST` in this release.

## Airflow

```python
SparkSubmitOperator(
    task_id="read_dbf",
    application="/path/read_dbf.py",
    jars="/path/spark-dbf_2.13-0.1.0-assembly.jar",
    conn_id="spark_default",
)
```

## Build

```bash
mvn clean verify
```

## Limitations

- One DBF file is read by one Spark task.
- Parallelism comes from reading multiple DBF files.
- Memo fields are not supported in this release.
- Spark Decimal precision is limited to 38.
- Files in one directory must have matching schemas.
- `mergeSchema` and byte-range splitting are not implemented.

## License

Apache License 2.0. This fork keeps attribution to the original
[mraad/spark-dbf](https://github.com/mraad/spark-dbf) project.

The assembly JAR includes JavaDBF, licensed under LGPL-3.0.

