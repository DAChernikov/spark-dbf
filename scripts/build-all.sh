#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

version="$(./mvnw -q -Pscala-2.12 help:evaluate -Dexpression=project.version -DforceStdout)"
rm -rf dist
mkdir -p dist

for scala_binary in 2.12 2.13; do
  profile="scala-${scala_binary}"
  artifact="spark-dbf_${scala_binary}-${version}-assembly.jar"
  ./mvnw clean verify -P"$profile"
  cp "target/$artifact" "dist/$artifact"
  checksum="$(shasum -a 256 "dist/$artifact" | awk '{print $1}')"
  printf '%s  %s\n' "$checksum" "$artifact" > "dist/$artifact.sha256"
  scripts/inspect-jar.sh "dist/$artifact"
done

ls -lh dist
