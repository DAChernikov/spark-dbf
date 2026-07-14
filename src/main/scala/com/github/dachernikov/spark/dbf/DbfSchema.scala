package com.github.dachernikov.spark.dbf

import java.io.IOException

import com.linuxense.javadbf.{DBFDataType, DBFField, DBFReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types._

/** Schema inference and validation for DBF metadata. */
object DbfSchema {
  final case class FieldMeta(
      originalName: String,
      sparkName: String,
      dbfType: DBFDataType,
      length: Int,
      scale: Int,
      sparkType: DataType)
      extends Serializable

  final case class Inferred(schema: StructType, fields: Seq[FieldMeta]) extends Serializable

  def infer(files: Seq[DbfFileDiscovery.DbfFile], options: DbfOptions, conf: Configuration): Inferred = {
    val schemas = files.map(file => file -> readFileSchema(file, options, conf))
    val first = schemas.head._2
    schemas.tail.foreach { case (file, inferred) => validateCompatible(schemas.head._1, first, file, inferred) }
    first
  }

  def readFileSchema(file: DbfFileDiscovery.DbfFile, options: DbfOptions, conf: Configuration): Inferred = {
    val path = new Path(file.uri)
    val fs = path.getFileSystem(conf)
    val input = fs.open(path)
    var reader: DBFReader = null
    try {
      reader = new DBFReader(input, options.charset)
      reader.setTrimRightSpaces(options.trimStrings)
      val fields = (0 until reader.getFieldCount).map { index =>
        val field = reader.getField(index)
        toFieldMeta(file.uri, field, options)
      }
      val duplicates = fields.groupBy(_.sparkName).collect { case (name, values) if values.size > 1 => name }.toSeq.sorted
      if (duplicates.nonEmpty) {
        throw new DbfException(s"Duplicate DBF column names after columnNameCase in ${file.uri}: ${duplicates.mkString(", ")}")
      }
      Inferred(StructType(fields.map(f => StructField(f.sparkName, f.sparkType, nullable = true))), fields)
    } catch {
      case e: DbfException => throw e
      case e: IOException => throw new DbfException(s"I/O error while reading DBF header: ${file.uri}", e)
      case e: RuntimeException => throw new DbfException(s"Corrupt or unsupported DBF header: ${file.uri}", e)
    } finally {
      if (reader != null) reader.close() else input.close()
    }
  }

  def applyExplicitSchema(inferred: Inferred, userSchema: StructType, options: DbfOptions): Inferred = {
    val fieldsByName = inferred.fields.map(f => f.sparkName -> f).toMap
    val duplicates = userSchema.fieldNames.groupBy(identity).collect { case (name, values) if values.length > 1 => name }
    if (duplicates.nonEmpty) {
      throw new DbfException(s"Duplicate fields in explicit schema: ${duplicates.mkString(", ")}")
    }

    val outputFields = userSchema.fields.flatMap { field =>
      if (field.name == DbfOptions.SourceFileColumn && options.addSourceFile) None
      else {
        val normalized = options.normalizeColumnName(field.name)
        val meta = fieldsByName.getOrElse(
          normalized,
          throw new DbfException(s"Explicit schema field '${field.name}' does not exist in DBF schema."))
        validateExplicitType(meta, field.dataType)
        Some(meta.copy(sparkName = field.name, sparkType = field.dataType))
      }
    }
    Inferred(StructType(outputFields.map(f => StructField(f.sparkName, f.sparkType, nullable = true))), outputFields)
  }

  def toFieldMeta(file: String, field: DBFField, options: DbfOptions): FieldMeta = {
    val dbfType = field.getType
    val name = field.getName
    val length = field.getLength
    val scale = field.getDecimalCount
    FieldMeta(name, options.normalizeColumnName(name), dbfType, length, scale, toSparkType(file, name, dbfType, length, scale))
  }

  def toSparkType(file: String, fieldName: String, dbfType: DBFDataType, length: Int, scale: Int): DataType =
    dbfType match {
      case DBFDataType.CHARACTER | DBFDataType.VARCHAR => StringType
      case DBFDataType.LOGICAL => BooleanType
      case DBFDataType.DATE => DateType
      case DBFDataType.TIMESTAMP | DBFDataType.TIMESTAMP_DBASE7 => TimestampType
      case DBFDataType.LONG | DBFDataType.AUTOINCREMENT => LongType
      case DBFDataType.NUMERIC =>
        if (scale == 0 && length > 0 && length <= 18) LongType else decimalType(file, fieldName, length, scale)
      case DBFDataType.FLOATING_POINT | DBFDataType.DOUBLE => DoubleType
      case DBFDataType.CURRENCY => DecimalType(19, 4)
      case DBFDataType.BINARY | DBFDataType.VARBINARY | DBFDataType.BLOB | DBFDataType.GENERAL_OLE | DBFDataType.PICTURE =>
        BinaryType
      case DBFDataType.MEMO =>
        throw new DbfException(
          s"Unsupported DBF memo field '$fieldName' in $file. Memo companion files are not supported in this release.")
      case other =>
        throw new DbfException(s"Unsupported DBF field type '$other' for field '$fieldName' in $file.")
    }

  private def decimalType(file: String, fieldName: String, length: Int, scale: Int): DecimalType = {
    val precision = if (length > 0) length else 38
    if (precision > 38) {
      throw new DbfException(
        s"DBF numeric field '$fieldName' in $file requires precision $precision, but Spark Decimal precision is limited to 38.")
    }
    DecimalType(precision, math.max(scale, 0))
  }

  private def validateExplicitType(meta: FieldMeta, target: DataType): Unit = {
    val ok = target match {
      case StringType => meta.sparkType == StringType
      case BooleanType => meta.sparkType == BooleanType
      case DateType => meta.sparkType == DateType
      case TimestampType => meta.sparkType == TimestampType || meta.sparkType == DateType
      case LongType | IntegerType | ShortType | ByteType => isIntegral(meta.sparkType)
      case _: DecimalType => isNumeric(meta.sparkType)
      case DoubleType | FloatType => meta.sparkType == DoubleType
      case BinaryType => meta.sparkType == BinaryType
      case _ => false
    }
    if (!ok) {
      throw new DbfException(
        s"Unsafe explicit schema conversion for field '${meta.originalName}': DBF type ${meta.dbfType} (${meta.sparkType.simpleString}) cannot be read as ${target.simpleString}.")
    }
  }

  private def isIntegral(dataType: DataType): Boolean = dataType match {
    case LongType | IntegerType | ShortType | ByteType => true
    case _ => false
  }

  private def isNumeric(dataType: DataType): Boolean = dataType match {
    case LongType | IntegerType | ShortType | ByteType | DoubleType | FloatType | _: DecimalType => true
    case _ => false
  }

  private def validateCompatible(
      leftFile: DbfFileDiscovery.DbfFile,
      left: Inferred,
      rightFile: DbfFileDiscovery.DbfFile,
      right: Inferred): Unit = {
    val leftFields = left.fields
    val rightFields = right.fields
    if (leftFields.map(_.sparkName) != rightFields.map(_.sparkName)) {
      throw new DbfException(
        s"DBF schema mismatch between ${leftFile.uri} and ${rightFile.uri}: column names or order differ. " +
          s"Left=${leftFields.map(_.sparkName).mkString("[", ", ", "]")}, right=${rightFields.map(_.sparkName).mkString("[", ", ", "]")}.")
    }
    leftFields.zip(rightFields).foreach { case (l, r) =>
      if (l.dbfType != r.dbfType || l.length != r.length || l.scale != r.scale || l.sparkType != r.sparkType) {
        throw new DbfException(
          s"DBF schema mismatch between ${leftFile.uri} and ${rightFile.uri}: field '${l.sparkName}' differs. " +
            s"Left type=${l.dbfType} length=${l.length} scale=${l.scale} spark=${l.sparkType.simpleString}; " +
            s"right type=${r.dbfType} length=${r.length} scale=${r.scale} spark=${r.sparkType.simpleString}.")
      }
    }
  }
}

