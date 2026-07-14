#!/usr/bin/env bash
set -euo pipefail

MAVEN_VERSION="${MAVEN_VERSION:-3.9.9}"
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
DIST_DIR="$BASE_DIR/.mvn/wrapper/dists/apache-maven-$MAVEN_VERSION"
MAVEN_HOME="$DIST_DIR/apache-maven-$MAVEN_VERSION"
ARCHIVE="$DIST_DIR/apache-maven-$MAVEN_VERSION-bin.tar.gz"
URL="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"

if [[ ! -x "$MAVEN_HOME/bin/mvn" ]]; then
  mkdir -p "$DIST_DIR"
  if [[ ! -f "$ARCHIVE" ]]; then
    echo "Downloading Apache Maven $MAVEN_VERSION..." >&2
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL "$URL" -o "$ARCHIVE"
    elif command -v wget >/dev/null 2>&1; then
      wget -q "$URL" -O "$ARCHIVE"
    else
      echo "curl or wget is required to download Maven." >&2
      exit 1
    fi
  fi
  tar -xzf "$ARCHIVE" -C "$DIST_DIR"
fi

exec "$MAVEN_HOME/bin/mvn" "$@"

