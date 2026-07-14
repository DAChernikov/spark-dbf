package com.github.dachernikov.spark.dbf

import org.scalatest.funsuite.AnyFunSuite

class DbfOptionsSuite extends AnyFunSuite {
  test("parses defaults") {
    val options = DbfOptions(Map("path" -> "/tmp/file.dbf"))
    assert(options.encoding == "UTF-8")
    assert(!options.recursiveFileLookup)
    assert(!options.addSourceFile)
    assert(options.columnNameCase == "preserve")
    assert(options.trimStrings)
    assert(options.ignoreDeleted)
  }

  test("validates invalid encoding") {
    val e = intercept[DbfException](DbfOptions(Map("path" -> "/tmp/file.dbf", "encoding" -> "bad charset name")))
    assert(e.getMessage.contains("Invalid DBF encoding"))
  }

  test("normalizes column names") {
    assert(DbfOptions(Map("path" -> "x", "columnNameCase" -> "lower")).normalizeColumnName("MiXeD") == "mixed")
    assert(DbfOptions(Map("path" -> "x", "columnNameCase" -> "upper")).normalizeColumnName("MiXeD") == "MIXED")
  }

  test("requires path") {
    assert(intercept[DbfException](DbfOptions(Map.empty)).getMessage.contains("DBF path is missing"))
  }
}

