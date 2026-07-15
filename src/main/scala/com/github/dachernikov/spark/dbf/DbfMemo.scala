package com.github.dachernikov.spark.dbf

import java.io.{Closeable, File, FileInputStream, IOException, RandomAccessFile}
import java.nio.charset.Charset
import java.nio.file.{DirectoryNotEmptyException, Files, Paths, StandardCopyOption}
import java.util.{Locale, UUID}

import com.linuxense.javadbf.DBFDataType
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** Hadoop companion discovery and task-attempt-local staging for JavaDBF memo files. */
object DbfMemo {
  val SupportedExtensions: Seq[String] = Seq(".dbt", ".fpt")

  final case class Companion(path: Path, extension: String) extends Serializable {
    def uri(fs: FileSystem): String = fs.makeQualified(path).toUri.toString
  }

  final class Staged private[dbf] (
      val dbfFile: File,
      val memoFile: File,
      directory: java.nio.file.Path,
      cleanupRoot: java.nio.file.Path)
      extends Closeable {
    private var closed = false

    override def close(): Unit = synchronized {
      if (!closed) {
        closed = true
        if (Files.exists(directory)) {
          val paths = Files.walk(directory)
          try paths.iterator().asScala.toSeq.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
          finally paths.close()
        }
        removeEmptyParents(directory.getParent, cleanupRoot)
      }
    }

    private def removeEmptyParents(start: java.nio.file.Path, stop: java.nio.file.Path): Unit = {
      var current = start
      var continue = current != null && current != stop
      while (continue) {
        try Files.deleteIfExists(current)
        catch {
          case _: DirectoryNotEmptyException => return
          case _: IOException => return
        }
        current = current.getParent
        continue = current != null && current != stop
      }
    }
  }

  final class Validator private[dbf] (
      staged: Staged,
      allFields: Seq[DbfSchema.FieldMeta],
      memoFields: Seq[DbfSchema.FieldMeta],
      charset: Charset,
      dbfUri: String,
      memoUri: String)
      extends Closeable {
    private val dbf = new RandomAccessFile(staged.dbfFile, "r")
    private val memo = new RandomAccessFile(staged.memoFile, "r")
    private lazy val fpt = staged.memoFile.getName.toLowerCase(Locale.ROOT).endsWith(".fpt")
    private lazy val offsets = allFields.scanLeft(1) { case (offset, field) => offset + field.length }.dropRight(1)
    private lazy val byName = allFields.zip(offsets).map { case (field, offset) => field.originalName -> offset }.toMap
    private lazy val headerLength = {
      dbf.seek(8L)
      dbf.readUnsignedByte() | (dbf.readUnsignedByte() << 8)
    }
    private lazy val recordLength = dbf.readUnsignedByte() | (dbf.readUnsignedByte() << 8)
    private lazy val blockSize = {
      if (fpt) {
        memo.seek(6L)
        memo.readUnsignedShort()
      } else {
        memo.seek(20L)
        val size = memo.readUnsignedByte() | (memo.readUnsignedByte() << 8)
        if (size == 0) 512 else size
      }
    }

    def validate(recordIndex: Long): Map[String, Long] = memoFields.map { field =>
      val pointer = readPointer(field, recordIndex)
      if (pointer > 0) {
        val offset = pointer * blockSize.toLong
        if (offset < 0 || offset >= memo.length()) fail(recordIndex, field, pointer, "pointer is outside companion file")
        if (fpt) {
          if (offset + 8L > memo.length()) fail(recordIndex, field, pointer, "FPT block header is truncated")
          memo.seek(offset)
          memo.readInt()
          val length = memo.readInt()
          if (length < 0 || offset + 8L + length > memo.length()) {
            fail(recordIndex, field, pointer, s"FPT block payload is truncated (length=$length)")
          }
        }
      }
      val normalizedPointer =
        if (!fpt && pointer > 0 && isEmptyDbtBlock(pointer)) 0L
        else pointer
      field.originalName -> normalizedPointer
    }.toMap

    override def close(): Unit = {
      try dbf.close()
      finally memo.close()
    }

    private def readPointer(field: DbfSchema.FieldMeta, recordIndex: Long): Long = {
      val bytes = new Array[Byte](field.length)
      dbf.seek(headerLength.toLong + recordIndex * recordLength + byName(field.originalName))
      dbf.readFully(bytes)
      if (field.length == 4) {
        (bytes(0) & 0xffL) |
          ((bytes(1) & 0xffL) << 8) |
          ((bytes(2) & 0xffL) << 16) |
          ((bytes(3) & 0xffL) << 24)
      } else {
        val value = new String(bytes, charset).trim
        if (value.isEmpty) 0L
        else {
          try value.toLong
          catch {
            case e: NumberFormatException =>
              throw new DbfException(
                s"Invalid DBF memo pointer. dbf=$dbfUri memo=$memoUri record=$recordIndex " +
                  s"field=${field.originalName} pointer=$value",
                e)
          }
        }
      }
    }

    private def isEmptyDbtBlock(pointer: Long): Boolean = {
      val offset = pointer * blockSize.toLong
      memo.seek(offset)
      memo.length() >= offset + 2L && memo.readUnsignedByte() == 0x1a && memo.readUnsignedByte() == 0x1a
    }

    private def fail(recordIndex: Long, field: DbfSchema.FieldMeta, pointer: Long, reason: String): Nothing =
      throw new DbfException(
        s"Corrupt DBF memo. dbf=$dbfUri memo=$memoUri record=$recordIndex " +
          s"field=${field.originalName} pointer=$pointer: $reason")
  }

  private val logger = LoggerFactory.getLogger(getClass)

  def isMemoBacked(dataType: DBFDataType): Boolean =
    dataType == DBFDataType.MEMO ||
      dataType == DBFDataType.BLOB ||
      dataType == DBFDataType.GENERAL_OLE ||
      dataType == DBFDataType.PICTURE

  def findCompanion(dbfPath: Path, fs: FileSystem): Option[Companion] = {
    val name = dbfPath.getName
    val dot = name.lastIndexOf('.')
    val base = if (dot >= 0) name.substring(0, dot) else name
    val parent = Option(dbfPath.getParent).getOrElse(new Path("."))
    val matches = fs.listStatus(parent).toSeq.flatMap { status =>
      val candidate = status.getPath.getName
      val candidateDot = candidate.lastIndexOf('.')
      if (!status.isFile || candidateDot < 0 || candidate.substring(0, candidateDot) != base) None
      else {
        val extension = candidate.substring(candidateDot).toLowerCase(Locale.ROOT)
        if (SupportedExtensions.contains(extension)) Some(Companion(status.getPath, extension)) else None
      }
    }
    if (matches.size > 1) {
      throw new DbfException(
        s"Multiple memo companion files found for ${fs.makeQualified(dbfPath).toUri}: " +
          matches.map(_.uri(fs)).sorted.mkString(", "))
    }
    matches.headOption
  }

  def expectedNames(dbfPath: Path): String = {
    val name = dbfPath.getName
    val dot = name.lastIndexOf('.')
    val base = if (dot >= 0) name.substring(0, dot) else name
    Seq(s"$base.dbt", s"$base.DBT", s"$base.fpt", s"$base.FPT").mkString(", ")
  }

  def missingMessage(dbfUri: String, dbfPath: Path, fieldNames: Seq[String]): String =
    s"DBF memo companion file is required but missing. dbf=$dbfUri expected=${expectedNames(dbfPath)} " +
      s"supportedExtensions=${SupportedExtensions.mkString(",")} fields=${fieldNames.mkString(",")}"

  def stage(
      dbfPath: Path,
      companion: Companion,
      fs: FileSystem,
      applicationId: String,
      context: TaskContext): Staged = {
    val cleanupRoot = Paths.get(System.getProperty("java.io.tmpdir"), "spark-dbf")
    val root = cleanupRoot.resolve(Paths.get(
      sanitize(applicationId),
      context.stageId().toString,
      context.partitionId().toString,
      s"${context.taskAttemptId()}-${UUID.randomUUID().toString}"))
    Files.createDirectories(root)
    val staged = new Staged(
      root.resolve(dbfPath.getName).toFile,
      root.resolve(companion.path.getName).toFile,
      root,
      cleanupRoot)
    try {
      copy(fs, dbfPath, staged.dbfFile)
      copy(companion.path.getFileSystem(fs.getConf), companion.path, staged.memoFile)
      logger.debug(s"Staged DBF memo input for task attempt ${context.taskAttemptId()} in ${root.toString}")
      staged
    } catch {
      case e: Exception =>
        staged.close()
        throw e
    }
  }

  def validator(
      staged: Staged,
      allFields: Seq[DbfSchema.FieldMeta],
      memoFields: Seq[DbfSchema.FieldMeta],
      charset: Charset,
      dbfUri: String,
      memoUri: String): Validator =
    new Validator(staged, allFields, memoFields, charset, dbfUri, memoUri)

  def pointerSummary(
      stagedDbf: File,
      allFields: Seq[DbfSchema.FieldMeta],
      memoFields: Seq[DbfSchema.FieldMeta],
      recordIndex: Long,
      charset: Charset): String = {
    if (recordIndex < 0 || !stagedDbf.isFile) return "unavailable"
    val offsets = allFields.scanLeft(1) { case (offset, field) => offset + field.length }.dropRight(1)
    val byName = allFields.zip(offsets).map { case (field, offset) => field.originalName -> offset }.toMap
    val raf = new RandomAccessFile(stagedDbf, "r")
    try {
      raf.seek(8L)
      val headerLength = raf.readUnsignedByte() | (raf.readUnsignedByte() << 8)
      val recordLength = raf.readUnsignedByte() | (raf.readUnsignedByte() << 8)
      memoFields.map { field =>
        val bytes = new Array[Byte](field.length)
        raf.seek(headerLength.toLong + recordIndex * recordLength + byName(field.originalName))
        raf.readFully(bytes)
        val pointer =
          if (field.length == 4) {
            (bytes(0) & 0xff) | ((bytes(1) & 0xff) << 8) | ((bytes(2) & 0xff) << 16) | ((bytes(3) & 0xff) << 24)
          } else new String(bytes, charset).trim
        s"${field.originalName}=$pointer"
      }.mkString(",")
    } catch {
      case _: Exception => "unavailable"
    } finally raf.close()
  }

  private def copy(fs: FileSystem, source: Path, target: File): Unit = {
    val input = fs.open(source)
    try Files.copy(input, target.toPath, StandardCopyOption.REPLACE_EXISTING)
    finally input.close()
  }

  private def sanitize(value: String): String = value.replaceAll("[^A-Za-z0-9._-]", "_")
}
