#!/usr/bin/env bash
set -euo pipefail

jar_path="${1:-}"

if [[ -z "$jar_path" ]]; then
  echo "Usage: $0 <spark-dbf_2.12|2.13-*-assembly.jar>" >&2
  exit 2
fi

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
require_present 'com/linuxense/javadbf/DBFMemoFile.class'
require_present 'com/github/dachernikov/spark/dbf/DbfMemo.class'
require_present 'META-INF/LICENSE$'
require_present 'META-INF/NOTICE$'
require_present 'META-INF/LICENSE-javadbf.txt$'

require_absent '^org/apache/spark/'
require_absent '^scala/'
require_absent '^org/apache/hadoop/'
require_absent '^org/slf4j/impl/'
require_absent '^org/apache/log4j/'
require_absent '^org/apache/logging/log4j/'
require_absent '^com/github/dachernikov/spark/dbf/.*Suite'
require_absent '^com/github/dachernikov/spark/dbf/DbfTestFiles'

if [[ ! "$(basename "$jar_path")" =~ ^spark-dbf_2\.(12|13)-[0-9]+\.[0-9]+\.[0-9]+-assembly\.jar$ ]]; then
  echo "Unexpected assembly artifact name: $(basename "$jar_path")" >&2
  exit 1
fi

echo "JAR inspection passed: $jar_path"
