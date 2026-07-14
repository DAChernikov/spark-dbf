package com.github.dachernikov.spark.dbf

import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.sql.{Date => SqlDate, Timestamp}
import java.time.{LocalDate, ZoneId}

import org.apache.spark.sql.types._

/** Converts JavaDBF values to Spark SQL row values with overflow checks. */
object DbfValueConverter {
  def convert(value: Any, meta: DbfSchema.FieldMeta, targetType: DataType, file: String, recordIndex: Long): Any = {
    if (value == null) return null
    try {
      targetType match {
        case StringType => value match {
            case s: String => s
            case other => other.toString
          }
        case BooleanType => value.asInstanceOf[java.lang.Boolean]
        case DateType => toSqlDate(value)
        case TimestampType => toTimestamp(value)
        case LongType => java.lang.Long.valueOf(toBigDecimal(value).toBigIntegerExact.longValueExact)
        case IntegerType => Integer.valueOf(toBigDecimal(value).toBigIntegerExact.intValueExact)
        case ShortType => java.lang.Short.valueOf(toBigDecimal(value).toBigIntegerExact.shortValueExact)
        case ByteType => java.lang.Byte.valueOf(toBigDecimal(value).toBigIntegerExact.byteValueExact)
        case DoubleType => java.lang.Double.valueOf(value.asInstanceOf[Number].doubleValue())
        case FloatType => java.lang.Float.valueOf(value.asInstanceOf[Number].floatValue())
        case decimal: DecimalType =>
          val scaled = toBigDecimal(value).setScale(decimal.scale)
          if (scaled.precision() > decimal.precision) {
            throw new ArithmeticException(s"precision ${scaled.precision()} exceeds ${decimal.precision}")
          }
          scaled
        case BinaryType => value.asInstanceOf[Array[Byte]]
        case other => throw new DbfException(s"Unsupported target Spark SQL type ${other.simpleString}.")
      }
    } catch {
      case e: DbfException => throw e
      case e: Exception =>
        throw new DbfException(
          s"Failed to convert DBF value. file=$file record=$recordIndex field=${meta.originalName} dbfType=${meta.dbfType} targetType=${targetType.simpleString}",
          e)
    }
  }

  private def toBigDecimal(value: Any): JBigDecimal = value match {
    case bd: JBigDecimal => bd
    case bi: BigInteger => new JBigDecimal(bi)
    case n: java.lang.Byte => JBigDecimal.valueOf(n.longValue())
    case n: java.lang.Short => JBigDecimal.valueOf(n.longValue())
    case n: java.lang.Integer => JBigDecimal.valueOf(n.longValue())
    case n: java.lang.Long => JBigDecimal.valueOf(n.longValue())
    case n: java.lang.Float => JBigDecimal.valueOf(n.doubleValue())
    case n: java.lang.Double => JBigDecimal.valueOf(n.doubleValue())
    case s: String if s.trim.nonEmpty => new JBigDecimal(s.trim)
    case other => throw new IllegalArgumentException(s"Cannot convert ${other.getClass.getName} to BigDecimal")
  }

  private def toSqlDate(value: Any): SqlDate = value match {
    case d: SqlDate => d
    case d: java.util.Date =>
      val localDate = d.toInstant.atZone(ZoneId.systemDefault()).toLocalDate
      SqlDate.valueOf(localDate)
    case ld: LocalDate => SqlDate.valueOf(ld)
    case other => throw new IllegalArgumentException(s"Cannot convert ${other.getClass.getName} to Date")
  }

  private def toTimestamp(value: Any): Timestamp = value match {
    case ts: Timestamp => ts
    case d: java.util.Date => new Timestamp(d.getTime)
    case other => throw new IllegalArgumentException(s"Cannot convert ${other.getClass.getName} to Timestamp")
  }
}

