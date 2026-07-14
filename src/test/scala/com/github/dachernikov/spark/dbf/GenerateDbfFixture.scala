package com.github.dachernikov.spark.dbf

import java.nio.charset.Charset
import java.nio.file.Paths

/** Generates a deterministic DBF fixture for external PySpark smoke tests. */
object GenerateDbfFixture {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: GenerateDbfFixture <output-dbf>")
    }
    val path = Paths.get(args(0))
    DbfTestFiles.createNamesDbf(path.getParent, path.getFileName.toString, Charset.forName("cp866").name(), "Привет")
  }
}

