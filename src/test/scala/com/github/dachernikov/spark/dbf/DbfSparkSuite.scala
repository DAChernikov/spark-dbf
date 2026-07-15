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

  test("reads a DBF whose path contains spaces without double-encoding the URI") {
    val dir = Files.createTempDirectory("spark-dbf-path-space")
    val file = DbfTestFiles.createBasicDbf(dir, "report with space.dbf", Charset.forName("cp866"))
    val df = spark.read.format("dbf").option("encoding", "cp866").load(file.toString)
    assert(df.count() == 2)
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

  test("reports a missing required memo companion") {
    val dir = Files.createTempDirectory("spark-dbf-missing-memo")
    val file = copyResource("/memo/missing.dbf", dir, "missing.dbf")
    val error = intercept[Exception](spark.read.format("dbf").option("encoding", "cp866").load(file.toString).count())
    assert(causeMessages(error).contains("memo companion file is required"))
  }

  test("reports schema mismatch across directory files") {
    val dir = Files.createTempDirectory("spark-dbf-mismatch")
    DbfTestFiles.createBasicDbf(dir, "a.dbf")
    DbfTestFiles.createNamesDbf(dir, "b.dbf", "UTF-8", "X")
    assert(intercept[DbfException](spark.read.format("dbf").load(dir.toString)).getMessage.contains("schema mismatch"))
  }

  test("reads DBT text memo including a complete long multiline value") {
    val dir = Files.createTempDirectory("spark-dbf-dbt")
    val dbf = copyResource("/memo/dbt.dbf", dir, "customers.dbf")
    copyResource("/memo/dbt.dbt", dir, "customers.DBT")
    val df = spark.read.format("dbf").option("encoding", "cp866").load(dbf.toString)
    assert(df.count() == 80)
    assert(df.schema("DESCRIPT").dataType == StringType)
    assert(df.where("CUST_ID = 1").head().getAs[String]("DESCRIPT") == null)
    val repeated = "Расширенная история взаимодействия. " * 30
    val expected = Seq(
      "История 2. Клиент подтвердил сведения и условия обработки данных.",
      "Вторая строка содержит детали, перенос строки и контрольное значение 17.",
      repeated,
      repeated,
      repeated).mkString("\n")
    assert(df.where("CUST_ID = 2").head().getAs[String]("HISTORY") == expected)
  }

  test("reads FPT text memo and Visual FoxPro scalar types") {
    val dir = Files.createTempDirectory("spark-dbf-fpt")
    val dbf = copyResource("/memo/fpt.dbf", dir, "contracts.dbf")
    copyResource("/memo/fpt.fpt", dir, "contracts.fpt")
    val df = spark.read.format("dbf").option("encoding", "cp1251").load(dbf.toString)
    assert(df.count() == 80)
    assert(df.schema("CONTRACT_T").dataType == StringType)
    assert(df.schema("PRICE").dataType == DecimalType(19, 4))
    assert(df.schema("SIGNED_AT").dataType == TimestampType)
    val longMemo = df.where("CONT_ID = 3").head().getAs[String]("CONTRACT_T")
    assert(longMemo.startsWith("Текст договора 3."))
    assert(longMemo.contains("\n"))
    assert(longMemo.length > 1000)
  }

  test("supports missing companion REQUIRED NULL and IGNORE modes") {
    val dir = Files.createTempDirectory("spark-dbf-missing-modes")
    val dbf = copyResource("/memo/missing.dbf", dir, "missing.dbf")
    val required = intercept[Exception](spark.read.format("dbf").option("encoding", "cp866").load(dbf.toString).count())
    val requiredMessages = causeMessages(required)
    assert(requiredMessages.contains("missing.dbt"))
    assert(requiredMessages.contains("NOTE"))

    val nullDf = spark.read.format("dbf").option("encoding", "cp866").option("memoFileMode", "NULL").load(dbf.toString)
    assert(nullDf.count() == 1)
    assert(nullDf.head().isNullAt(nullDf.schema.fieldIndex("NOTE")))

    val ignored = spark.read.format("dbf").option("memoFileMode", "IGNORE").load(dbf.toString)
    assert(ignored.columns.toSeq == Seq("ID"))
    assert(ignored.count() == 1)

    val explicit = StructType(Seq(StructField("ID", LongType), StructField("NOTE", StringType)))
    val explicitError = intercept[DbfException](
      spark.read.schema(explicit).format("dbf").option("memoFileMode", "IGNORE").load(dbf.toString))
    assert(explicitError.getMessage.contains("explicit MEMO fields"))
  }

  test("reads an explicit StringType schema for memo") {
    val dir = Files.createTempDirectory("spark-dbf-explicit-memo")
    val dbf = copyResource("/memo/dbt.dbf", dir, "memo.dbf")
    copyResource("/memo/dbt.dbt", dir, "memo.dbt")
    val schema = StructType(Seq(StructField("CUST_ID", LongType), StructField("DESCRIPT", StringType)))
    val df = spark.read.schema(schema).format("dbf").option("encoding", "cp866").load(dbf.toString)
    assert(df.schema == schema)
    assert(df.where("CUST_ID = 2").head().getString(1).startsWith("Описание 2."))
  }

  test("uses the matching DBT companion for each file partition") {
    val dir = Files.createTempDirectory("spark-dbf-partitioned-memo")
    (1 to 4).foreach { index =>
      copyResource("/memo/dbt.dbf", dir, f"part_$index%04d.dbf")
      copyResource("/memo/dbt.dbt", dir, f"part_$index%04d.dbt")
    }
    val df = spark.read.format("dbf").option("encoding", "cp866").load(dir.toString)
    assert(df.rdd.getNumPartitions == 4)
    assert(df.count() == 320)
    assert(df.where("CUST_ID = 2").select("HISTORY").distinct().count() == 1)
  }

  test("fails fast on a corrupt memo pointer target") {
    val dir = Files.createTempDirectory("spark-dbf-corrupt-memo")
    val dbf = copyResource("/memo/corrupt.dbf", dir, "corrupt.dbf")
    copyResource("/memo/corrupt.dbt", dir, "corrupt.dbt")
    val error = intercept[Exception](spark.read.format("dbf").option("encoding", "cp866").load(dbf.toString).count())
    val messages = causeMessages(error)
    assert(messages.contains("Corrupt DBF memo"))
    assert(messages.contains("field=NOTE"))
    assert(messages.contains("pointer="))
    assert(messages.contains("record=0"))
  }

  private def copyResource(resource: String, dir: java.nio.file.Path, name: String): java.nio.file.Path = {
    val target = dir.resolve(name)
    val input = Option(getClass.getResourceAsStream(resource)).getOrElse(
      throw new IllegalArgumentException(s"Missing test resource: $resource"))
    try Files.copy(input, target)
    finally input.close()
    target
  }

  private def causeMessages(error: Throwable): String = {
    val messages = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).map(_.getMessage).filter(_ != null)
    messages.mkString(" | ")
  }
}
