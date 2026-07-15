#!/usr/bin/env python3
"""Validate generated fixtures through an assembly JAR and a real Spark runtime."""

import argparse
import hashlib
import json
from pathlib import Path

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--jar")
    parser.add_argument("--scala-binary-version", choices=("2.12", "2.13"))
    parser.add_argument("--spark-master", default="local[2]")
    return parser.parse_args()


def expect_failure(action, text):
    try:
        action()
    except Exception as exc:
        if text.lower() not in str(exc).lower():
            raise AssertionError("Expected %r in error: %s" % (text, exc))
    else:
        raise AssertionError("Expected failure containing %r" % text)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expected_memo(prefix, index, long=False):
    paragraphs = [
        "%s %d. Клиент подтвердил сведения и условия обработки данных." % (prefix, index + 1),
        "Вторая строка содержит детали, перенос строки и контрольное значение %d." % (index * 17),
    ]
    if long:
        paragraphs.extend(["Расширенная история взаимодействия. " * 30] * 3)
    return "\n".join(paragraphs)


def validate_checksums(base, fixture):
    path = base / fixture["file"]
    if path.is_file():
        assert sha256(path) == fixture["sha256"], fixture["file"]
    companion = fixture.get("companion_file")
    if companion and (base / companion).exists():
        assert sha256(base / companion) == fixture["companion_sha256"], companion
    for member in fixture.get("members", []):
        assert sha256(base / member["file"]) == member["sha256"], member["file"]


def main():
    args = parse_args()
    builder = SparkSession.builder.appName("spark-dbf-examples").master(args.spark_master)
    if args.jar:
        builder = builder.config("spark.jars", args.jar)
    spark = SparkSession.getActiveSession() or builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    runtime = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
    runtime_binary = ".".join(runtime.split(".")[:2])
    expected = args.scala_binary_version
    if expected and runtime_binary != expected:
        raise RuntimeError("Spark runtime uses Scala %s, but the selected JAR is built for Scala %s." % (runtime_binary, expected))

    base = Path(args.base_path).resolve()
    manifest = json.loads((base / "manifest.json").read_text(encoding="utf-8"))
    for fixture in manifest["fixtures"]:
        validate_checksums(base, fixture)
        path = str(base / fixture["file"])
        negative = fixture.get("negative")
        if negative == "missing":
            expect_failure(lambda: spark.read.format("dbf").option("encoding", fixture["encoding"]).load(path).count(), "required")
            null_df = spark.read.format("dbf").option("encoding", fixture["encoding"]).option("memoFileMode", "NULL").load(path)
            assert null_df.first()["NOTE"] is None
            ignored = spark.read.format("dbf").option("memoFileMode", "IGNORE").load(path)
            assert "NOTE" not in ignored.columns
            continue
        if negative == "corrupt":
            expect_failure(lambda: spark.read.format("dbf").option("encoding", fixture["encoding"]).load(path).count(), "memo")
            continue

        reader = spark.read.format("dbf").option("encoding", fixture["encoding"]).option("addSourceFile", "true")
        frame = reader.load(path)
        frame.printSchema()
        actual = frame.count()
        assert actual == fixture["expected_rows_ignore_deleted"], (fixture["file"], actual)
        assert frame.rdd.getNumPartitions() == fixture["expected_partitions"]
        assert "_source_file" in frame.columns
        expected_types = {field["name"]: field["type"].replace("long", "bigint") for field in fixture["schema"]}
        actual_types = dict(frame.dtypes)
        for field in fixture["schema"]:
            assert field["name"] in frame.columns, (fixture["file"], field["name"])
            assert actual_types[field["name"]] == expected_types[field["name"]], (
                fixture["file"], field["name"], actual_types[field["name"]]
            )
        if fixture["memo_fields"]:
            row = frame.where("%s IS NOT NULL" % fixture["memo_fields"][0]).first()
            assert row is not None and isinstance(row[fixture["memo_fields"][0]], str)
            multiline = frame.where("%s LIKE '%%\n%%'" % fixture["memo_fields"][0]).first()
            assert multiline is not None
        if "customers_with_dbt" in fixture["file"]:
            rows = {row["CUST_ID"]: row for row in frame.where("CUST_ID IN (1, 2)").collect()}
            assert rows[1]["DESCRIPT"] is None
            assert rows[2]["HISTORY"] == expected_memo("История", 1, long=True)
        if "contracts_with_fpt" in fixture["file"]:
            row = frame.where("CONT_ID = 3").first()
            assert row["CONTRACT_T"] == expected_memo("Текст договора", 2, long=True)
            assert row["SIGNED_AT"] is not None and isinstance(row["RATIO"], float)
        if "sales_report_cp866" in fixture["file"]:
            included = spark.read.format("dbf").option("encoding", "cp866").option("ignoreDeleted", "false").load(path).count()
            assert included == fixture["expected_rows_include_deleted"]
            assert frame.select("REGION").first()[0] in ["Москва", "Казань", "Самара", "Омск", "Тула"]

    print("Validated %d fixtures with Spark %s / Scala %s" % (len(manifest["fixtures"]), spark.version, runtime))
    spark.stop()


if __name__ == "__main__":
    main()
