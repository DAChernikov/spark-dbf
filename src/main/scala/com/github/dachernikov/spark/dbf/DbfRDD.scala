package com.github.dachernikov.spark.dbf

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
    outputSchema: org.apache.spark.sql.types.StructType,
    options: DbfOptions,
    hadoopConfiguration: Map[String, String])
    extends RDD[Row](sc, Nil) {

  override protected def getPartitions: Array[Partition] =
    files.zipWithIndex.map { case (file, index) => DbfFilePartition(index, file.uri) }.toArray

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val filePartition = split.asInstanceOf[DbfFilePartition]
    val path = new Path(filePartition.uri)
    val conf = new Configuration(false)
    hadoopConfiguration.foreach { case (key, value) => conf.set(key, value) }
    val fs = path.getFileSystem(conf)
    val input = fs.open(path)
    val reader = new DBFReader(input, options.charset, !options.ignoreDeleted)
    reader.setTrimRightSpaces(options.trimStrings)

    var closed = false
    def close(): Unit =
      if (!closed) {
        closed = true
        reader.close()
      }
    context.addTaskCompletionListener[Unit](_ => close())

    new Iterator[Row] {
      private var recordIndex = -1L
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
            row = reader.nextRow()
            recordIndex += 1
          } while (row != null && options.ignoreDeleted && row.isDeleted)

          if (row == null) {
            close()
            null
          } else {
            val values = fields.zip(outputSchema.fields).map { case (meta, sparkField) =>
              DbfValueConverter.convert(row.getObject(meta.originalName), meta, sparkField.dataType, filePartition.uri, recordIndex)
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
            close()
            throw new DbfException(s"Corrupt DBF record or I/O error. file=${filePartition.uri} record=$recordIndex", e)
        }
    }
  }
}

final case class DbfFilePartition(index: Int, uri: String) extends Partition
