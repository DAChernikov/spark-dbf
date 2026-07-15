# Examples

Install the generator dependency and create deterministic fixtures:

```bash
python -m pip install -r examples/requirements.txt
python examples/generate_dbf_examples.py --output examples/generated --seed 42
```

Build both JARs and validate with a matching Spark runtime:

```bash
scripts/build-all.sh
spark-submit --jars dist/spark-dbf_2.12-0.2.0-assembly.jar \
  examples/read_and_validate_dbf.py --base-path examples/generated --scala-binary-version 2.12
```

Use the `_2.13` JAR and a Spark Scala 2.13 distribution for the equivalent 2.13 run. The fixtures cover
character, numeric, decimal, logical, date, deleted records, DBT/FPT text memo, missing/corrupt companions,
and a four-file partitioned input. Generated fixtures, including the 100,000-row cases, are not stored in Git.

```bash
/path/to/spark-3.5.4-bin-hadoop3-scala2.13/bin/spark-submit \
  --jars dist/spark-dbf_2.13-0.2.0-assembly.jar \
  examples/read_and_validate_dbf.py \
  --base-path examples/generated \
  --scala-binary-version 2.13
```
