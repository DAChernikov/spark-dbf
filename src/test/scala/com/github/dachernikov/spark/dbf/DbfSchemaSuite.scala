package com.github.dachernikov.spark.dbf

import com.linuxense.javadbf.DBFDataType
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class DbfSchemaSuite extends AnyFunSuite {
  test("maps DBF field types to Spark SQL types") {
    assert(DbfSchema.toSparkType("x.dbf", "C", DBFDataType.CHARACTER, 10, 0) == StringType)
    assert(DbfSchema.toSparkType("x.dbf", "L", DBFDataType.LOGICAL, 1, 0) == BooleanType)
    assert(DbfSchema.toSparkType("x.dbf", "D", DBFDataType.DATE, 8, 0) == DateType)
    assert(DbfSchema.toSparkType("x.dbf", "N", DBFDataType.NUMERIC, 10, 0) == LongType)
    assert(DbfSchema.toSparkType("x.dbf", "N", DBFDataType.NUMERIC, 10, 2) == DecimalType(10, 2))
    assert(DbfSchema.toSparkType("x.dbf", "F", DBFDataType.FLOATING_POINT, 10, 2) == DoubleType)
  }

  test("rejects numeric precision above Spark maximum") {
    val e = intercept[DbfException](DbfSchema.toSparkType("x.dbf", "N", DBFDataType.NUMERIC, 40, 2))
    assert(e.getMessage.contains("precision 40"))
  }

  test("rejects memo fields in first release") {
    val e = intercept[DbfException](DbfSchema.toSparkType("x.dbf", "M", DBFDataType.MEMO, 10, 0))
    assert(e.getMessage.contains("Memo companion files are not supported"))
  }
}

