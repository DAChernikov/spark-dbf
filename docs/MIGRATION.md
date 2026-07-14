# Migration Notes

The original project targeted Spark 1.1-1.3, Scala 2.10 and the old
`SchemaRDD` APIs. The implementation also depended on `mraad/Shapefile`
for DBF parsing and Hadoop input format support.

Version 0.1.0 keeps the Apache 2.0 license, attribution and Git history,
but replaces the runtime implementation with a Spark 3.5 DataSource V1
reader in package `com.github.dachernikov.spark.dbf`.

DBF parsing is handled by JavaDBF 1.14.1 from Maven Central. JavaDBF is
LGPL-3.0, supports `InputStream`, exposes header metadata, and exposes
deleted record markers through `DBFRow.isDeleted`. Memo fields require a
local companion file API in JavaDBF, so memo support is intentionally not
claimed in the first release.

File access uses Hadoop `FileSystem` and `FSDataInputStream`. The driver
discovers files and reads DBF headers only. Records are read on Spark
executors, with one partition per DBF file.

