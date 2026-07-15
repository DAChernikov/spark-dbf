package com.github.dachernikov.spark.dbf

import java.nio.charset.Charset
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class DbfMemoSuite extends AnyFunSuite {
  test("resolves DBT and FPT companions case-insensitively") {
    val dir = Files.createTempDirectory("spark-dbf-companion")
    val dbf = Files.createFile(dir.resolve("report.dbf"))
    val upperDbt = Files.createFile(dir.resolve("report.DBT"))
    val conf = new Configuration()
    val path = new Path(dbf.toUri)
    val fs = path.getFileSystem(conf)
    assert(DbfMemo.findCompanion(path, fs).map(_.path.getName).contains(upperDbt.getFileName.toString))
    Files.delete(upperDbt)
    Files.createFile(dir.resolve("report.fPt"))
    assert(DbfMemo.findCompanion(path, fs).map(_.extension).contains(".fpt"))
  }

  test("reports ambiguous companions") {
    val dir = Files.createTempDirectory("spark-dbf-ambiguous")
    val dbf = Files.createFile(dir.resolve("report.dbf"))
    Files.createFile(dir.resolve("report.dbt"))
    Files.createFile(dir.resolve("report.fpt"))
    val path = new Path(dbf.toUri)
    val fs = path.getFileSystem(new Configuration())
    assert(intercept[DbfException](DbfMemo.findCompanion(path, fs)).getMessage.contains("Multiple"))
  }

  test("formats missing companion diagnostics") {
    val message = DbfMemo.missingMessage("hdfs:///data/report.dbf", new Path("/data/report.dbf"), Seq("NOTE"))
    assert(message.contains("hdfs:///data/report.dbf"))
    assert(message.contains("report.dbt"))
    assert(message.contains(".fpt"))
    assert(message.contains("NOTE"))
  }

  test("recognizes memo-backed field types") {
    assert(DbfMemo.isMemoBacked(com.linuxense.javadbf.DBFDataType.MEMO))
    assert(!DbfMemo.isMemoBacked(com.linuxense.javadbf.DBFDataType.CHARACTER))
    assert(Charset.forName("cp866").name().nonEmpty)
  }

  test("staged inputs close idempotently and remove task-attempt directories") {
    val cleanupRoot = Files.createTempDirectory("spark-dbf-cleanup")
    val attempt = cleanupRoot.resolve("app").resolve("1").resolve("0").resolve("42-uuid")
    Files.createDirectories(attempt)
    val dbf = Files.createFile(attempt.resolve("report.dbf")).toFile
    val memo = Files.createFile(attempt.resolve("report.dbt")).toFile
    val staged = new DbfMemo.Staged(dbf, memo, attempt, cleanupRoot)

    staged.close()
    staged.close()

    assert(!Files.exists(attempt))
    assert(!Files.exists(cleanupRoot.resolve("app")))
    assert(Files.exists(cleanupRoot))
    Files.delete(cleanupRoot)
  }
}
