# spark-dbf

Spark DataSource for reading DBF, DBT, and FPT files with Apache Spark 3.5.

This project is a maintained fork of [mraad/spark-dbf](https://github.com/mraad/spark-dbf).

## Compatibility

| Library | Spark | Scala | Java |
|---|---|---|---|
| 0.2.x | 3.5.2-3.5.4 | 2.12 / 2.13 | 11 / 17 |
| 0.1.x | 3.5.2-3.5.4 | 2.13 | 11 / 17 |

The assembly is compiled to Java 8 bytecode. CI runs the supported matrix on Java 11 and 17.

## Download

Download a ready-to-use assembly from [GitHub Releases](https://github.com/DAChernikov/spark-dbf/releases):

```text
spark-dbf_2.12-0.2.0-assembly.jar
spark-dbf_2.13-0.2.0-assembly.jar
```

Use `_2.12` when the Spark runtime uses Scala 2.12. Use `_2.13` only with a Spark distribution built for
Scala 2.13. Standard PyPI installations of PySpark 3.5.x use Scala 2.12.

The library is not published to Maven Central. The GitHub Release JARs are the supported distribution.

## Usage

```python
df = (
    spark.read
    .format("dbf")
    .option("encoding", "cp866")
    .load("hdfs:///data/input/report.dbf")
)
```

The full provider name remains available:

```python
df = spark.read.format("com.github.dachernikov.spark.dbf").load("hdfs:///data/input/report.dbf")
```

DBT and FPT companions are found beside each DBF using the same base name, including upper-case extensions:

```text
report.dbf + report.dbt
contracts.dbf + contracts.FPT
```

Text MEMO fields are returned as strings. The DBF `encoding` option is also used for text MEMO content.

## JupyterHub

Add the matching JAR before the `SparkSession` is created. Restart the notebook kernel first if `spark` already
exists:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("read-dbf")
    .config("spark.jars", "/shared/jars/spark-dbf_2.12-0.2.0-assembly.jar")
    .getOrCreate()
)

df = spark.read.format("dbf").option("encoding", "cp1251").load("hdfs:///data/report.dbf")
df.show(20, truncate=False)
```

For a JupyterHub launcher that creates Spark before notebook code runs, configure the same path in
`spark.jars`, or set this before importing PySpark:

```python
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars /shared/jars/spark-dbf_2.12-0.2.0-assembly.jar pyspark-shell"
)
```

## spark-submit

```bash
spark-submit \
  --master local[2] \
  --jars /opt/jars/spark-dbf_2.12-0.2.0-assembly.jar \
  read_dbf.py
```

Use the `_2.13` file with a Scala 2.13 Spark distribution.

## Directories

One DBF file creates one Spark partition. A directory of DBF files is read in parallel:

```python
df = (
    spark.read
    .format("dbf")
    .option("recursiveFileLookup", "true")
    .option("addSourceFile", "true")
    .load("hdfs:///data/input/dbf/")
)
```

Each partition resolves its own DBT or FPT companion on the executor through Hadoop `FileSystem`.

## Options

| Option | Default | Description |
|---|---:|---|
| `encoding` | `UTF-8` | DBF and text MEMO character encoding |
| `ignoreDeleted` | `true` | Skip records marked as deleted |
| `recursiveFileLookup` | `false` | Discover DBF files recursively |
| `addSourceFile` | `false` | Add the `_source_file` column |
| `columnNameCase` | `preserve` | `preserve`, `lower`, or `upper` |
| `trimStrings` | `true` | Trim trailing spaces in character fields |
| `memoFileMode` | `REQUIRED` | Missing companion behavior: `REQUIRED`, `NULL`, or `IGNORE` |

`REQUIRED` fails if an inferred MEMO field has no companion. `NULL` keeps the column and returns null values.
`IGNORE` omits inferred MEMO columns; it is rejected when an explicit schema requests a MEMO column.

Corrupt records and corrupt MEMO pointers are handled in fail-fast mode with DBF URI, companion URI, record,
field, and pointer diagnostics when available.

## Explicit Schema

```python
schema = "id long, title string, description string"

df = (
    spark.read.schema(schema)
    .format("dbf")
    .option("encoding", "cp1251")
    .load("hdfs:///data/contracts.dbf")
)
```

Text MEMO fields accept `StringType`; binary memo/blob fields accept `BinaryType` when JavaDBF identifies them.

## Airflow

Pass the JAR through `SparkSubmitOperator.jars`. A runnable example is in
[`examples/airflow_spark_submit_operator.py`](examples/airflow_spark_submit_operator.py).

## Examples

Generate deterministic DBF/DBT/FPT fixtures and validate them through a real Spark process:

```bash
python -m pip install -r examples/requirements.txt
python examples/generate_dbf_examples.py --output examples/generated --seed 42
scripts/build-all.sh
spark-submit --jars dist/spark-dbf_2.12-0.2.0-assembly.jar \
  examples/read_and_validate_dbf.py \
  --base-path examples/generated \
  --scala-binary-version 2.12
```

See [`examples/README.md`](examples/README.md) for the Scala 2.13 command and fixture coverage.

## Build

```bash
./mvnw clean verify -Pscala-2.12
./mvnw clean verify -Pscala-2.13
scripts/build-all.sh
```

`scripts/build-all.sh` writes both assemblies and SHA-256 files to `dist/`.

## Limitations

- One DBF file is read by one Spark task; byte-range splitting is not implemented.
- JavaDBF requires local files for MEMO access. A task stages its DBF and companion into a unique executor-local
  directory and deletes it on completion. Executors need enough temporary disk for concurrently running tasks.
- Ordinary DBF files without MEMO fields continue to stream directly from Hadoop `FileSystem`.
- Files read as one directory must have matching schemas; `mergeSchema` is not implemented.
- Spark decimal precision is limited to 38.
- XBase field descriptors limit physical column names to 10 characters in the generated examples.

## License

spark-dbf is Apache License 2.0. The assembly includes
[JavaDBF](https://github.com/albfernandez/javadbf) 1.14.1 under LGPL-3.0; attribution and the license text are
included in `NOTICE` and `META-INF`.
