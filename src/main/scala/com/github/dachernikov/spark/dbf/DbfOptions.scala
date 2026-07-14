package com.github.dachernikov.spark.dbf

import java.nio.charset.{Charset, IllegalCharsetNameException, UnsupportedCharsetException}
import java.util.Locale

import org.apache.spark.sql.types.{StringType, StructType}

/** Parsed options supported by the DBF datasource. */
final case class DbfOptions(
    path: String,
    encoding: String,
    recursiveFileLookup: Boolean,
    addSourceFile: Boolean,
    columnNameCase: String,
    trimStrings: Boolean,
    ignoreDeleted: Boolean,
    corruptRecordMode: String)
    extends Serializable {

  def charset: Charset = Charset.forName(encoding)

  def normalizeColumnName(name: String): String = columnNameCase match {
    case "preserve" => name
    case "lower" => name.toLowerCase(Locale.ROOT)
    case "upper" => name.toUpperCase(Locale.ROOT)
  }
}

object DbfOptions {
  val SourceFileColumn = "_source_file"

  /** Build options from Spark DataSource parameters. */
  def apply(parameters: Map[String, String]): DbfOptions = {
    val normalized = parameters.map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }
    val path = normalized
      .get("path")
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new DbfException("DBF path is missing. Pass .load(path) or option(\"path\", path)."))

    val encoding = normalized.getOrElse("encoding", "UTF-8")
    try Charset.forName(encoding)
    catch {
      case e @ (_: IllegalCharsetNameException | _: UnsupportedCharsetException) =>
        throw new DbfException(s"Invalid DBF encoding '$encoding'. Use a JVM-supported charset name.", e)
    }

    val columnNameCase = normalized.getOrElse("columnnamecase", "preserve").toLowerCase(Locale.ROOT)
    if (!Set("preserve", "lower", "upper").contains(columnNameCase)) {
      throw new DbfException("Invalid columnNameCase. Supported values are preserve, lower and upper.")
    }

    val corruptRecordMode = normalized.getOrElse("corruptrecordmode", "FAILFAST").toUpperCase(Locale.ROOT)
    if (corruptRecordMode != "FAILFAST") {
      throw new DbfException("Invalid corruptRecordMode. This release supports FAILFAST only.")
    }

    DbfOptions(
      path = path,
      encoding = encoding,
      recursiveFileLookup = parseBoolean(normalized, "recursivefilelookup", default = false),
      addSourceFile = parseBoolean(normalized, "addsourcefile", default = false),
      columnNameCase = columnNameCase,
      trimStrings = parseBoolean(normalized, "trimstrings", default = true),
      ignoreDeleted = parseBoolean(normalized, "ignoredeleted", default = true),
      corruptRecordMode = corruptRecordMode)
  }

  def withSourceFile(schema: StructType, add: Boolean): StructType =
    if (!add || schema.fieldNames.contains(SourceFileColumn)) schema
    else schema.add(SourceFileColumn, StringType, nullable = false)

  private def parseBoolean(options: Map[String, String], key: String, default: Boolean): Boolean =
    options.get(key).map(_.trim.toLowerCase(Locale.ROOT)) match {
      case None => default
      case Some("true") => true
      case Some("false") => false
      case Some(value) => throw new DbfException(s"Invalid boolean value '$value' for option '$key'.")
    }
}

