#!/usr/bin/env bash
set -euo pipefail

JAR="${1:-target/spark-dbf_2.13-0.1.0-assembly.jar}"
FIXTURE="${2:-target/pyspark-fixtures/cp866.dbf}"

./mvnw -q -DskipTests test-compile
./mvnw -q -DskipTests package
./mvnw -q -Dexec.classpathScope=test -Dexec.mainClass=com.github.dachernikov.spark.dbf.GenerateDbfFixture -Dexec.args="$FIXTURE" org.codehaus.mojo:exec-maven-plugin:3.5.0:java

spark-submit \
  --master local[2] \
  --jars "$JAR" \
  src/test/python/pyspark_smoke.py "$JAR" "$FIXTURE"

scripts/inspect-jar.sh "$JAR"

