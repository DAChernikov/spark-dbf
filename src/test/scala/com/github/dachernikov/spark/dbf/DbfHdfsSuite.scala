package com.github.dachernikov.spark.dbf

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class DbfHdfsSuite extends AnyFunSuite {
  test("reads DBF and DBT through an actual HDFS client on executors") {
    val baseDir = Files.createTempDirectory("spark-dbf-minidfs")
    val hdfsConf = new HdfsConfiguration()
    hdfsConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toString)
    val cluster = new MiniDFSCluster.Builder(hdfsConf).numDataNodes(1).format(true).build()
    var spark: SparkSession = null
    try {
      cluster.waitClusterUp()
      val fs = cluster.getFileSystem
      fs.mkdirs(new Path("/memo"))
      copyResource(fs, "/memo/dbt.dbf", new Path("/memo/customers.dbf"))
      copyResource(fs, "/memo/dbt.dbt", new Path("/memo/customers.dbt"))

      spark = SparkSession.builder()
        .master("local[2]")
        .appName("spark-dbf-hdfs-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      copyConfiguration(cluster.getConfiguration(0), spark.sparkContext.hadoopConfiguration)
      val uri = fs.makeQualified(new Path("/memo/customers.dbf")).toUri.toString
      val df = spark.read.format("dbf").option("encoding", "cp866").load(uri)
      assert(df.count() == 80)
      assert(df.where("CUST_ID = 2").head().getAs[String]("HISTORY").contains("Расширенная история"))
    } finally {
      if (spark != null) spark.stop()
      cluster.shutdown(true)
    }
  }

  private def copyResource(fs: org.apache.hadoop.fs.FileSystem, resource: String, target: Path): Unit = {
    val input = Option(getClass.getResourceAsStream(resource)).getOrElse(
      throw new IllegalArgumentException(s"Missing test resource: $resource"))
    val output = fs.create(target, true)
    try org.apache.hadoop.io.IOUtils.copyBytes(input, output, 8192, false)
    finally {
      input.close()
      output.close()
    }
  }

  private def copyConfiguration(source: Configuration, target: Configuration): Unit = {
    val entries = source.iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      target.set(entry.getKey, entry.getValue)
    }
  }
}
