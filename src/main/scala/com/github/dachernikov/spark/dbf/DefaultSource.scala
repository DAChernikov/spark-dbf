package com.github.dachernikov.spark.dbf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/** Spark DataSource V1 entry point registered under the short name `dbf`. */
final class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
  private val logger = LoggerFactory.getLogger(getClass)

  override def shortName(): String = "dbf"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    create(sqlContext, parameters, None)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation =
    create(sqlContext, parameters, Some(schema))

  private def create(sqlContext: SQLContext, parameters: Map[String, String], userSchema: Option[StructType]): BaseRelation = {
    val options = DbfOptions(parameters)
    val conf = new org.apache.hadoop.conf.Configuration(sqlContext.sparkContext.hadoopConfiguration)
    val files = DbfFileDiscovery.discover(options, conf)
    val inferred = DbfSchema.infer(files, options, conf)
    val selected = userSchema match {
      case Some(schema) => DbfSchema.applyExplicitSchema(inferred, schema, options)
      case None => inferred
    }
    val outputSchema = DbfOptions.withSourceFile(selected.schema, options.addSourceFile)

    logger.info(
      "DBF datasource options: encoding={}, recursiveFileLookup={}, addSourceFile={}, columnNameCase={}, trimStrings={}, ignoreDeleted={}, memoFileMode={}",
      options.encoding,
      Boolean.box(options.recursiveFileLookup),
      Boolean.box(options.addSourceFile),
      options.columnNameCase,
      Boolean.box(options.trimStrings),
      Boolean.box(options.ignoreDeleted),
      options.memoFileMode)
    logger.info("DBF datasource schema: {}", outputSchema.treeString)

    val serializableConf = conf.iterator().asScala.map(entry => entry.getKey -> entry.getValue).toMap
    new DbfRelation(
      sqlContext,
      files,
      selected.fields,
      selected.sourceFields,
      selected.memoFields,
      outputSchema,
      options,
      serializableConf)
  }
}
