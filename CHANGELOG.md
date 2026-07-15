# Changelog

## 0.2.0

- Added separate Scala 2.12 and Scala 2.13 assembly artifacts.
- Added dBase DBT and FoxPro/Visual FoxPro FPT text MEMO support.
- Added executor-side companion discovery through Hadoop `FileSystem` and task-local staging required by JavaDBF.
- Added `memoFileMode=REQUIRED|NULL|IGNORE` and fail-fast corrupt pointer diagnostics.
- Added real local filesystem and MiniDFSCluster integration coverage.
- Added deterministic DBF/DBT/FPT examples and assembly validation through real Spark runtimes.
- Added Spark 3.5.2-3.5.4 cross-version CI and release artifacts with SHA-256 checksums.

## 0.1.0

- Added Apache Spark 3.5 support.
- Added Scala 2.13 support.
- Added PySpark `spark.read.format("dbf")` API.
- Added local filesystem and HDFS support.
- Added reading of directories with multiple DBF files.
- Added schema inference and explicit schema support.
- Added configurable character encoding.
- Added executor-side DBF reading with one Spark partition per DBF file.
