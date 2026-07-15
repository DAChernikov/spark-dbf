#!/usr/bin/env python3
"""Generate deterministic dBase DBT and Visual FoxPro FPT examples."""

import argparse
import datetime as dt
import hashlib
import json
import random
import shutil
from decimal import Decimal
from pathlib import Path

import dbf


REGIONS = ["Москва", "Казань", "Самара", "Омск", "Тула"]
CATEGORIES = ["Книги", "Техника", "Одежда", "Продукты"]
CUSTOMERS = ["Альфа", "Бета", "Вектор", "Гамма", "Дельта"]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def remove_table(path):
    for suffix in (".dbf", ".dbt", ".fpt"):
        candidate = path.with_suffix(suffix)
        if candidate.exists():
            candidate.unlink()


def create_table(path, fields, rows, dbf_type, codepage, deleted=()):
    remove_table(path)
    table = dbf.Table(str(path), fields, dbf_type=dbf_type, codepage=codepage)
    table.open(mode=dbf.READ_WRITE)
    deleted = set(deleted)
    try:
        for index, row in enumerate(rows):
            table.append(row)
            if index in deleted:
                dbf.delete(table[-1])
    finally:
        table.close()
    return path


def sales_rows(count, seed):
    rng = random.Random(seed)
    for index in range(count):
        quantity = 1 + rng.randrange(25)
        unit_price = Decimal(rng.randrange(100, 100000)) / 100
        amount = unit_price * quantity
        short = "" if index % 19 == 0 else "Заказ %d: %s" % (index + 1, CATEGORIES[index % len(CATEGORIES)])
        yield (
            index + 1,
            CUSTOMERS[index % len(CUSTOMERS)],
            REGIONS[index % len(REGIONS)],
            CATEGORIES[index % len(CATEGORIES)],
            quantity,
            unit_price,
            amount,
            Decimal(index % 15) / 100,
            index % 3 != 0,
            dt.date(2024, 1 + index % 12, 1 + index % 27),
            short,
        )


def memo_text(prefix, index, encoding, long=False):
    paragraphs = [
        "%s %d. Клиент подтвердил сведения и условия обработки данных." % (prefix, index + 1),
        "Вторая строка содержит детали, перенос строки и контрольное значение %d." % (index * 17),
    ]
    if long:
        paragraphs.extend(["Расширенная история взаимодействия. " * 30] * 3)
    text = "\n".join(paragraphs)
    text.encode(encoding)
    return text


def add_entry(entries, root, path, dialect, encoding, physical, included, schema, memo_fields=(), partitions=1):
    companion = next((path.with_suffix(ext) for ext in (".dbt", ".fpt") if path.with_suffix(ext).exists()), None)
    entry = {
        "file": str(path.relative_to(root)),
        "companion_file": str(companion.relative_to(root)) if companion else None,
        "dbf_dialect": dialect,
        "encoding": encoding,
        "physical_records": physical,
        "expected_rows_ignore_deleted": included,
        "expected_rows_include_deleted": physical,
        "expected_partitions": partitions,
        "schema": schema,
        "memo_fields": list(memo_fields),
        "sha256": sha256(path),
        "companion_sha256": sha256(companion) if companion else None,
    }
    entries.append(entry)
    return entry


def file_checksums(root, paths):
    return [
        {"file": str(path.relative_to(root)), "sha256": sha256(path)}
        for path in sorted(paths)
    ]


def generate(output, seed, overwrite, small_only):
    if output.exists() and any(output.iterdir()) and not overwrite:
        existing = [p for p in output.iterdir() if p.name != ".gitkeep"]
        if existing:
            raise SystemExit("Output is not empty; pass --overwrite: %s" % output)
    if overwrite and output.exists():
        for child in output.iterdir():
            if child.name != ".gitkeep":
                shutil.rmtree(child) if child.is_dir() else child.unlink()
    output.mkdir(parents=True, exist_ok=True)
    entries = []
    sales_schema = [
        {"name": "REPORT_ID", "type": "long"},
        {"name": "CUSTOMER", "type": "string"},
        {"name": "REGION", "type": "string"},
        {"name": "CATEGORY", "type": "string"},
        {"name": "QUANTITY", "type": "long"},
        {"name": "UNIT_PRICE", "type": "decimal(12,2)"},
        {"name": "AMOUNT", "type": "decimal(16,2)"},
        {"name": "DISCOUNT", "type": "decimal(5,2)"},
        {"name": "IS_ACTIVE", "type": "boolean"},
        {"name": "REPORT_DT", "type": "date"},
        {"name": "COMMENT_SH", "type": "string"},
    ]
    sales_fields = (
        "REPORT_ID N(10,0); CUSTOMER C(30); REGION C(20); CATEGORY C(20); "
        "QUANTITY N(8,0); UNIT_PRICE N(12,2); AMOUNT N(16,2); DISCOUNT N(5,2); "
        "IS_ACTIVE L; REPORT_DT D; COMMENT_SH C(80)"
    )

    counts = (60, 120, 240) if small_only else (1000, 10000, 100000)
    for filename, count, encoding, deleted_count in (
        ("sales_report_cp866_1000.dbf", counts[0], "cp866", min(10, counts[0])),
        ("sales_report_cp1251_10000.dbf", counts[1], "cp1251", 0),
        ("sales_report_cp1251_100000.dbf", counts[2], "cp1251", 0),
    ):
        path = output / filename
        deleted = range(deleted_count)
        create_table(path, sales_fields, sales_rows(count, seed + count), "db3", encoding, deleted)
        add_entry(entries, output, path, "dBase III", encoding, count, count - deleted_count, sales_schema)

    dbt_count = 80 if small_only else 5000
    dbt_path = output / "customers_with_dbt_memo_cp866_5000.dbf"
    dbt_fields = "CUST_ID N(10,0); NAME C(40); DESCRIPT M; MGR_NOTE M; HISTORY M"
    dbt_rows = (
        (
            i + 1,
            "Клиент %05d" % (i + 1),
            "" if i % 11 == 0 else memo_text("Описание", i, "cp866", i % 37 == 0),
            "" if i % 13 == 0 else memo_text("Заметка", i, "cp866"),
            memo_text("История", i, "cp866", i == 1),
        )
        for i in range(dbt_count)
    )
    create_table(dbt_path, dbt_fields, dbt_rows, "db3", "cp866")
    dbt_schema = [
        {"name": "CUST_ID", "type": "long"}, {"name": "NAME", "type": "string"},
        {"name": "DESCRIPT", "type": "string"}, {"name": "MGR_NOTE", "type": "string"},
        {"name": "HISTORY", "type": "string"},
    ]
    add_entry(entries, output, dbt_path, "dBase III + DBT", "cp866", dbt_count, dbt_count, dbt_schema,
              ("DESCRIPT", "MGR_NOTE", "HISTORY"))

    fpt_count = 80 if small_only else 5000
    fpt_path = output / "contracts_with_fpt_memo_cp1251_5000.dbf"
    fpt_fields = (
        "CONT_ID I; TITLE C(50); CONTRACT_T M; LEGAL_NOTE M; AUDIT_COMM M; "
        "PRICE Y; SIGNED_AT T; RATIO B"
    )
    fpt_rows = (
        (
            i + 1,
            "Договор %05d" % (i + 1),
            memo_text("Текст договора", i, "cp1251", i == 2),
            "" if i % 17 == 0 else memo_text("Юридическая заметка", i, "cp1251"),
            memo_text("Аудит", i, "cp1251"),
            Decimal("1000.1250") + i,
            dt.datetime(2024, 1 + i % 12, 1 + i % 27, 10, 30, 15),
            i / 10.0,
        )
        for i in range(fpt_count)
    )
    create_table(fpt_path, fpt_fields, fpt_rows, "vfp", "cp1251")
    fpt_schema = [
        {"name": "CONT_ID", "type": "long"}, {"name": "TITLE", "type": "string"},
        {"name": "CONTRACT_T", "type": "string"}, {"name": "LEGAL_NOTE", "type": "string"},
        {"name": "AUDIT_COMM", "type": "string"}, {"name": "PRICE", "type": "decimal(19,4)"},
        {"name": "SIGNED_AT", "type": "timestamp"}, {"name": "RATIO", "type": "double"},
    ]
    add_entry(entries, output, fpt_path, "Visual FoxPro + FPT", "cp1251", fpt_count, fpt_count, fpt_schema,
              ("CONTRACT_T", "LEGAL_NOTE", "AUDIT_COMM"))

    partition_dir = output / "partitioned_dbt_report"
    partition_dir.mkdir(exist_ok=True)
    per_partition = 25 if small_only else 25000
    part_schema = [{"name": "ID", "type": "long"}, {"name": "PART_NO", "type": "long"},
                   {"name": "DETAILS", "type": "string"}]
    partition_files = []
    for part in range(1, 5):
        part_path = partition_dir / ("part_%04d.dbf" % part)
        rows = ((part * 1000000 + i, part, memo_text("Раздел", i, "cp866")) for i in range(per_partition))
        create_table(part_path, "ID N(12,0); PART_NO N(4,0); DETAILS M", rows, "db3", "cp866")
        partition_files.extend((part_path, part_path.with_suffix(".dbt")))
    partition_entry = add_entry(
        entries, output, partition_dir / "part_0001.dbf", "partitioned dBase III + DBT", "cp866",
        per_partition * 4, per_partition * 4, part_schema, ("DETAILS",), partitions=4
    )
    partition_entry["file"] = "partitioned_dbt_report"
    partition_entry["members"] = file_checksums(output, partition_files)

    missing_path = output / "missing_memo.dbf"
    create_table(missing_path, "ID N(10,0); NOTE M", [(1, "companion will be removed")], "db3", "cp866")
    missing_path.with_suffix(".dbt").unlink()
    add_entry(entries, output, missing_path, "dBase III + missing DBT", "cp866", 1, 1,
              [{"name": "ID", "type": "long"}, {"name": "NOTE", "type": "string"}], ("NOTE",))["negative"] = "missing"

    corrupt_path = output / "corrupt_memo.dbf"
    create_table(corrupt_path, "ID N(10,0); NOTE M", [(1, "corrupt companion pointer target")], "db3", "cp866")
    with corrupt_path.with_suffix(".dbt").open("r+b") as stream:
        stream.truncate(512)
    add_entry(entries, output, corrupt_path, "dBase III + corrupt DBT", "cp866", 1, 1,
              [{"name": "ID", "type": "long"}, {"name": "NOTE", "type": "string"}], ("NOTE",))["negative"] = "corrupt"

    manifest = {"generator": "dbf 0.99.11", "seed": seed, "small_only": small_only, "fixtures": entries}
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    files = sorted(path for path in output.rglob("*") if path.is_file() and path.name not in {".gitkeep", "SHA256SUMS"})
    (output / "SHA256SUMS").write_text(
        "".join("%s  %s\n" % (sha256(path), path.relative_to(output)) for path in files), encoding="ascii"
    )
    print("Generated %d fixtures under %s" % (len(entries), output))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path(__file__).parent / "generated")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--small-only", action="store_true")
    args = parser.parse_args()
    generate(args.output.resolve(), args.seed, args.overwrite, args.small_only)


if __name__ == "__main__":
    main()
