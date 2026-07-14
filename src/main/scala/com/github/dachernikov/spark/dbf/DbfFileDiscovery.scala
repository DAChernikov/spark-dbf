package com.github.dachernikov.spark.dbf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

/** Discovers DBF input files through Hadoop FileSystem APIs. */
object DbfFileDiscovery {
  private val logger = LoggerFactory.getLogger(getClass)

  final case class DbfFile(uri: String) extends Serializable

  def discover(options: DbfOptions, conf: Configuration): Seq[DbfFile] = {
    val input = new Path(options.path)
    val fs = input.getFileSystem(conf)
    if (!fs.exists(input)) {
      throw new DbfException(s"DBF path does not exist: ${input.toString}")
    }

    val status = fs.getFileStatus(input)
    val files =
      if (status.isFile) {
        if (isDbf(status.getPath) && !isHidden(status.getPath)) Seq(qualified(fs, status.getPath)) else Seq.empty
      } else {
        listDirectory(fs, status.getPath, options.recursiveFileLookup)
      }

    val sorted = files.distinct.sortBy(_.uri)
    if (sorted.isEmpty) {
      throw new DbfException(s"No DBF files found under path: ${input.toString}")
    }
    logger.info("Discovered {} DBF file(s) under {}", Int.box(sorted.size), input.toString)
    sorted
  }

  private def listDirectory(fs: FileSystem, dir: Path, recursive: Boolean): Seq[DbfFile] =
    fs.listStatus(dir).toSeq.flatMap { status =>
      val path = status.getPath
      if (isHidden(path)) Seq.empty
      else if (status.isFile && isDbf(path)) Seq(qualified(fs, path))
      else if (status.isDirectory && recursive) listDirectory(fs, path, recursive)
      else Seq.empty
    }

  private def qualified(fs: FileSystem, path: Path): DbfFile =
    DbfFile(fs.makeQualified(path).toUri.toString)

  private def isDbf(path: Path): Boolean =
    path.getName.toLowerCase(java.util.Locale.ROOT).endsWith(".dbf")

  private def isHidden(path: Path): Boolean = {
    val name = path.getName
    name.startsWith(".") || name.startsWith("_")
  }
}

