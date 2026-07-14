package com.github.dachernikov.spark.dbf

import java.io.{File, RandomAccessFile}
import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.Charset
import java.nio.file.{Files, Path}
import java.time.LocalDate
import java.time.ZoneId
import java.util.Date

import com.linuxense.javadbf.{DBFDataType, DBFField, DBFWriter}

object DbfTestFiles {
  def createBasicDbf(dir: Path, name: String = "people.dbf", charset: Charset = Charset.forName("UTF-8")): Path = {
    Files.createDirectories(dir)
    val file = dir.resolve(name)
    val writer = new DBFWriter(file.toFile, charset)
    writer.setFields(Array(
      new DBFField("ID", DBFDataType.NUMERIC, 10, 0),
      new DBFField("NAME", DBFDataType.CHARACTER, 24),
      new DBFField("AMOUNT", DBFDataType.NUMERIC, 10, 2),
      new DBFField("ACTIVE", DBFDataType.LOGICAL),
      new DBFField("BIRTH", DBFDataType.DATE)
    ))
    writer.addRecord(Array[AnyRef](
      new JBigDecimal("1"),
      "ALICE   ",
      new JBigDecimal("12.30"),
      java.lang.Boolean.TRUE,
      date("2020-01-02")
    ))
    writer.addRecord(Array[AnyRef](
      new JBigDecimal("2"),
      "BOB",
      new JBigDecimal("45.67"),
      java.lang.Boolean.FALSE,
      date("2021-03-04")
    ))
    writer.close()
    file
  }

  def createNamesDbf(dir: Path, name: String, charsetName: String, firstName: String): Path = {
    Files.createDirectories(dir)
    val file = dir.resolve(name)
    val writer = new DBFWriter(file.toFile, Charset.forName(charsetName))
    writer.setFields(Array(new DBFField("NAME", DBFDataType.CHARACTER, 32)))
    writer.addRecord(Array[AnyRef](firstName))
    writer.close()
    file
  }

  def createEmptyDbf(dir: Path): Path = {
    Files.createDirectories(dir)
    val file = dir.resolve("empty.dbf")
    val writer = new DBFWriter(file.toFile, Charset.forName("UTF-8"))
    writer.setFields(Array(new DBFField("ID", DBFDataType.NUMERIC, 10, 0)))
    writer.close()
    file
  }

  def createMemoDbf(dir: Path): Path = {
    val file = createNamesDbf(dir, "memo.dbf", "UTF-8", "memo")
    val raf = new RandomAccessFile(file.toFile, "rw")
    try {
      raf.seek(32L + 11L)
      raf.writeByte('M'.toInt)
    } finally {
      raf.close()
    }
    file
  }

  def markFirstRecordDeleted(file: Path): Unit = {
    val raf = new RandomAccessFile(file.toFile, "rw")
    try {
      raf.seek(8L)
      val low = raf.readUnsignedByte()
      val high = raf.readUnsignedByte()
      val headerLength = low + (high << 8)
      raf.seek(headerLength.toLong)
      raf.writeByte('*'.toInt)
    } finally {
      raf.close()
    }
  }

  def corruptHeader(dir: Path): Path = {
    Files.createDirectories(dir)
    val file = dir.resolve("corrupt.dbf")
    Files.write(file, Array[Byte](0x03, 0x01, 0x02))
    file
  }

  private def date(value: String): Date = {
    val instant = LocalDate.parse(value).atStartOfDay(ZoneId.systemDefault()).toInstant
    Date.from(instant)
  }
}
