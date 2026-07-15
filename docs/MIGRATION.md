# Migration Notes

The original project targeted Spark 1.1-1.3, Scala 2.10 and the old
`SchemaRDD` APIs. The implementation also depended on `mraad/Shapefile`
for DBF parsing and Hadoop input format support.

Version 0.1.0 kept the Apache 2.0 license, attribution and Git history,
but replaces the runtime implementation with a Spark 3.5 DataSource V1
reader in package `com.github.dachernikov.spark.dbf`.

DBF parsing is handled by JavaDBF 1.14.1 from Maven Central. JavaDBF is
LGPL-3.0, supports `InputStream`, exposes header metadata, and exposes
deleted record markers through `DBFRow.isDeleted`.

File access uses Hadoop `FileSystem` and `FSDataInputStream`. The driver
discovers files and reads DBF headers only. Records are read on Spark
executors, with one partition per DBF file.

Version 0.2.0 cross-builds the same source for Scala 2.12 and 2.13. Choose
the JAR whose suffix matches the Scala binary version of the Spark runtime.

JavaDBF 1.14.1 accepts an `InputStream` for DBF data but only `java.io.File`
for DBT/FPT data. For a file with MEMO fields, each Spark task discovers the
matching companion through Hadoop `FileSystem`, stages both files in a unique
executor-local task-attempt directory, and removes that directory when the
task completes. DBF files without MEMO fields still use Hadoop streams
directly.
