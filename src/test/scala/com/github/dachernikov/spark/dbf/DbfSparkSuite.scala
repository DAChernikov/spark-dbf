package com.github.dachernikov.spark.dbf

import java.nio.charset.Charset
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DbfSparkSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-dbf-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("reads one DBF file with inferred schema") {
    val dir = Files.createTempDirectory("spark-dbf-one")
    val file = DbfTestFiles.createBasicDbf(dir)
    val df = spark.read.format("dbf").load(file.toString)
    assert(df.schema("ID").dataType == LongType)
    assert(df.schema("NAME").dataType == StringType)
    assert(df.schema("AMOUNT").dataType == DecimalType(10, 2))
    assert(df.count() == 2)
    assert(df.collect().map(_.getAs[String]("NAME")).toSeq == Seq("ALICE", "BOB"))
    assert(df.rdd.getNumPartitions == 1)
  }

  test("reads full provider package name") {
    val dir = Files.createTempDirectory("spark-dbf-full-name")
    val file = DbfTestFiles.createBasicDbf(dir)
    val df = spark.read.format("com.github.dachernikov.spark.dbf").load(file.toString)
    assert(df.count() == 2)
  }

  test("reads directory with multiple DBF files and source file column") {
    val dir = Files.createTempDirectory("spark-dbf-dir")
    DbfTestFiles.createBasicDbf(dir, "a.dbf")
    DbfTestFiles.createBasicDbf(dir, "b.DBF")
    val df = spark.read
      .format("dbf")
      .option("addSourceFile", "true")
      .load(dir.toString)
    assert(df.count() == 4)
    assert(df.schema.fieldNames.contains("_source_file"))
    assert(df.rdd.getNumPartitions == 2)
  }

  test("reads recursive directory lookup") {
    val dir = Files.createTempDirectory("spark-dbf-rec")
    val nested = Files.createDirectories(dir.resolve("nested"))
    DbfTestFiles.createBasicDbf(nested, "a.dbf")
    val df = spark.read.format("dbf").option("recursiveFileLookup", "true").load(dir.toString)
    assert(df.count() == 2)
  }

  test("reads explicit schema safely") {
    val dir = Files.createTempDirectory("spark-dbf-schema")
    val file = DbfTestFiles.createBasicDbf(dir)
    val schema = StructType(Seq(
      StructField("ID", LongType),
      StructField("NAME", StringType),
      StructField("AMOUNT", DecimalType(10, 2)),
      StructField("ACTIVE", BooleanType),
      StructField("BIRTH", DateType)
    ))
    val df = spark.read.schema(schema).format("dbf").load(file.toString)
    assert(df.schema == schema)
    assert(df.count() == 2)
  }

  test("supports cp866 and windows-1251 encodings") {
    val dir = Files.createTempDirectory("spark-dbf-enc")
    val cp866 = DbfTestFiles.createNamesDbf(dir, "cp866.dbf", "cp866", "Привет")
    val cp1251 = DbfTestFiles.createNamesDbf(dir, "cp1251.dbf", "windows-1251", "Москва")
    assert(spark.read.format("dbf").option("encoding", "cp866").load(cp866.toString).head().getString(0) == "Привет")
    assert(spark.read.format("dbf").option("encoding", "windows-1251").load(cp1251.toString).head().getString(0) == "Москва")
  }

  test("can preserve trailing spaces when trimStrings is false") {
    val dir = Files.createTempDirectory("spark-dbf-trim")
    val file = DbfTestFiles.createBasicDbf(dir, charset = Charset.forName("UTF-8"))
    val value = spark.read.format("dbf").option("trimStrings", "false").load(file.toString).head().getString(1)
    assert(value.startsWith("ALICE"))
    assert(value.length > "ALICE".length)
  }

  test("skips deleted records by default") {
    val dir = Files.createTempDirectory("spark-dbf-deleted")
    val file = DbfTestFiles.createBasicDbf(dir)
    DbfTestFiles.markFirstRecordDeleted(file)
    assert(spark.read.format("dbf").load(file.toString).count() == 1)
    assert(spark.read.format("dbf").option("ignoreDeleted", "false").load(file.toString).count() == 2)
  }

  test("reads empty DBF") {
    val dir = Files.createTempDirectory("spark-dbf-empty")
    val file = DbfTestFiles.createEmptyDbf(dir)
    val df = spark.read.format("dbf").load(file.toString)
    assert(df.count() == 0)
    assert(df.schema("ID").dataType == LongType)
  }

  test("reports corrupt header") {
    val dir = Files.createTempDirectory("spark-dbf-corrupt")
    val file = DbfTestFiles.corruptHeader(dir)
    assert(intercept[DbfException](spark.read.format("dbf").load(file.toString)).getMessage.contains("header"))
  }

  test("reports unsupported memo fields") {
    val dir = Files.createTempDirectory("spark-dbf-memo")
    val file = DbfTestFiles.createMemoDbf(dir)
    assert(intercept[DbfException](spark.read.format("dbf").load(file.toString)).getMessage.contains("Memo"))
  }

  test("reports schema mismatch across directory files") {
    val dir = Files.createTempDirectory("spark-dbf-mismatch")
    DbfTestFiles.createBasicDbf(dir, "a.dbf")
    DbfTestFiles.createNamesDbf(dir, "b.dbf", "UTF-8", "X")
    assert(intercept[DbfException](spark.read.format("dbf").load(dir.toString)).getMessage.contains("schema mismatch"))
  }
}

