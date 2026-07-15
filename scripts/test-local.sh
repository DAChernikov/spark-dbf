#!/usr/bin/env bash
set -euo pipefail

SCALA_BINARY="${1:-2.12}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
JAR="dist/spark-dbf_${SCALA_BINARY}-0.2.0-assembly.jar"

python examples/generate_dbf_examples.py --small-only --overwrite
scripts/build-all.sh

"$SPARK_SUBMIT" \
  --master local[2] \
  --jars "$JAR" \
  examples/read_and_validate_dbf.py \
  --base-path examples/generated \
  --scala-binary-version "$SCALA_BINARY"

scripts/inspect-jar.sh "$JAR"
