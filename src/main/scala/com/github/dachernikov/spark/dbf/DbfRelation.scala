package com.github.dachernikov.spark.dbf

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

/** Read-only Spark SQL relation for DBF files. */
final class DbfRelation(
    override val sqlContext: SQLContext,
    files: Seq[DbfFileDiscovery.DbfFile],
    fields: Seq[DbfSchema.FieldMeta],
    sourceFields: Seq[DbfSchema.FieldMeta],
    memoFields: Seq[DbfSchema.FieldMeta],
    outputSchema: StructType,
    options: DbfOptions,
    hadoopConfiguration: Map[String, String])
    extends BaseRelation
    with TableScan {

  override def schema: StructType = outputSchema

  override def buildScan(): RDD[Row] =
    new DbfRDD(
      sqlContext.sparkContext,
      files,
      fields,
      sourceFields,
      memoFields,
      outputSchema,
      options,
      hadoopConfiguration)
}
