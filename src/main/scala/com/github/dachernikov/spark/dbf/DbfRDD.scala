package com.github.dachernikov.spark.dbf

import java.io.{FileInputStream, InputStream}

import com.linuxense.javadbf.{DBFReader, DBFRow}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

/** One Spark partition per DBF file. Records are opened and read on executors. */
final class DbfRDD(
    sc: SparkContext,
    files: Seq[DbfFileDiscovery.DbfFile],
    fields: Seq[DbfSchema.FieldMeta],
    sourceFields: Seq[DbfSchema.FieldMeta],
    memoFields: Seq[DbfSchema.FieldMeta],
    outputSchema: org.apache.spark.sql.types.StructType,
    options: DbfOptions,
    hadoopConfiguration: Map[String, String])
    extends RDD[Row](sc, Nil) {

  private val applicationId = sc.applicationId

  override protected def getPartitions: Array[Partition] =
    files.zipWithIndex.map { case (file, index) => DbfFilePartition(index, file.uri, file.pathUri) }.toArray

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val filePartition = split.asInstanceOf[DbfFilePartition]
    val path = new Path(filePartition.pathUri)
    val conf = new Configuration(false)
    hadoopConfiguration.foreach { case (key, value) => conf.set(key, value) }
    val fs = path.getFileSystem(conf)
    var staged: DbfMemo.Staged = null
    var input: InputStream = null
    var reader: DBFReader = null
    var memoValidator: DbfMemo.Validator = null
    var memoUri: Option[String] = None

    try {
      val companion =
        if (memoFields.nonEmpty && options.memoFileMode != "IGNORE") DbfMemo.findCompanion(path, fs)
        else None
      memoUri = companion.map(_.uri(fs))
      if (memoFields.nonEmpty && companion.isEmpty && options.memoFileMode == "REQUIRED") {
        throw new DbfException(DbfMemo.missingMessage(filePartition.uri, path, memoFields.map(_.originalName)))
      }

      companion match {
        case Some(memo) =>
          staged = DbfMemo.stage(path, memo, fs, applicationId, context)
          input = new FileInputStream(staged.dbfFile)
          reader = new DBFReader(input, options.charset, true)
          reader.setMemoFile(staged.memoFile)
          memoValidator = DbfMemo.validator(
            staged,
            sourceFields,
            memoFields,
            options.charset,
            filePartition.uri,
            memo.uri(fs))
        case None =>
          input = fs.open(path)
          reader = new DBFReader(input, options.charset, true)
      }
      reader.setTrimRightSpaces(options.trimStrings)
    } catch {
      case e: DbfException =>
        if (reader != null) reader.close() else if (input != null) input.close()
        if (memoValidator != null) memoValidator.close()
        if (staged != null) staged.close()
        throw e
      case e: Exception =>
        if (reader != null) reader.close() else if (input != null) input.close()
        if (memoValidator != null) memoValidator.close()
        if (staged != null) staged.close()
        val memo = memoUri.map(uri => s" memo=$uri").getOrElse("")
        throw new DbfException(
          s"Failed to open DBF memo input. dbf=${filePartition.uri}$memo fields=${memoFields.map(_.originalName).mkString(",")}",
          e)
    }

    var closed = false
    def close(): Unit =
      if (!closed) {
        closed = true
        try reader.close()
        finally {
          try if (memoValidator != null) memoValidator.close()
          finally if (staged != null) staged.close()
        }
      }
    context.addTaskCompletionListener[Unit](_ => close())

    new Iterator[Row] {
      private var recordIndex = -1L
      private var memoPointers = Map.empty[String, Long]
      private var nextValue: Row = readNext()

      override def hasNext: Boolean = nextValue != null

      override def next(): Row = {
        if (nextValue == null) throw new NoSuchElementException("No more DBF rows")
        val current = nextValue
        nextValue = readNext()
        current
      }

      private def readNext(): Row =
        try {
          var row: DBFRow = null
          do {
            recordIndex += 1
            if (recordIndex >= reader.getRecordCount) row = null
            else {
              memoPointers =
                if (memoValidator != null) memoValidator.validate(recordIndex)
                else Map.empty
              row = reader.nextRow()
            }
          } while (row != null && options.ignoreDeleted && row.isDeleted)

          if (row == null) {
            close()
            null
          } else {
            val values = fields.zip(outputSchema.fields).map { case (meta, sparkField) =>
              val raw =
                if (DbfMemo.isMemoBacked(meta.dbfType) && memoPointers.get(meta.originalName).contains(0L)) null
                else row.getObject(meta.originalName)
              DbfValueConverter.convert(raw, meta, sparkField.dataType, filePartition.uri, recordIndex)
            }
            val withSource =
              if (options.addSourceFile && outputSchema.fieldNames.contains(DbfOptions.SourceFileColumn)) values :+ filePartition.uri
              else values
            Row.fromSeq(withSource)
          }
        } catch {
          case e: DbfException =>
            close()
            throw e
          case e: Exception =>
            val memoDetails = memoUri.map { uri =>
              val pointers =
                if (staged == null) "unavailable"
                else DbfMemo.pointerSummary(staged.dbfFile, sourceFields, memoFields, recordIndex, options.charset)
              s" memo=$uri fields=${memoFields.map(_.originalName).mkString(",")} pointers=$pointers"
            }.getOrElse("")
            close()
            throw new DbfException(
              s"Corrupt DBF record, MEMO value or I/O error. dbf=${filePartition.uri}$memoDetails record=$recordIndex original=${e.getMessage}",
              e)
        }
    }
  }
}

final case class DbfFilePartition(index: Int, uri: String, pathUri: java.net.URI) extends Partition
