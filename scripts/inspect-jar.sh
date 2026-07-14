#!/usr/bin/env bash
set -euo pipefail

jar_path="${1:-target/spark-dbf_2.13-0.1.0-assembly.jar}"

if [[ ! -f "$jar_path" ]]; then
  echo "JAR not found: $jar_path" >&2
  exit 1
fi

tmp_listing="$(mktemp)"
trap 'rm -f "$tmp_listing"' EXIT
jar tf "$jar_path" > "$tmp_listing"

require_present() {
  local pattern="$1"
  if ! grep -q "$pattern" "$tmp_listing"; then
    echo "Missing required entry matching: $pattern" >&2
    exit 1
  fi
}

require_absent() {
  local pattern="$1"
  if grep -q "$pattern" "$tmp_listing"; then
    echo "Forbidden entry found matching: $pattern" >&2
    exit 1
  fi
}

require_present 'com/github/dachernikov/spark/dbf/DefaultSource.class'
require_present 'META-INF/services/org.apache.spark.sql.sources.DataSourceRegister'
require_present 'com/linuxense/javadbf/DBFReader.class'

require_absent '^org/apache/spark/'
require_absent '^scala/'
require_absent '^org/apache/hadoop/'
require_absent '^org/slf4j/impl/'
require_absent '^org/apache/log4j/'
require_absent '^org/apache/logging/log4j/'

echo "JAR inspection passed: $jar_path"

