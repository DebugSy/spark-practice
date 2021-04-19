package com.shiy.spark.cases.case03

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object HBaseSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBase Sink Null")
      .master("local[1]")
      .getOrCreate();

    val catalog =
      s"""{
         |"table":{"namespace":"default", "name":"shiy_spark_write_table"},
         |"rowkey":"key",
         |"columns":{
         |"int_col":{"cf":"rowkey", "col":"key", "type":"int"},
         |"string_col":{"cf":"cf1", "col":"string_col", "type":"string"},
         |"boolean_col":{"cf":"cf1", "col":"boolean_col", "type":"boolean"},
         |"byte_col":{"cf":"cf1", "col":"byte_col", "type":"tinyint"},
         |"timestamp_col":{"cf":"cf1", "col":"timestamp_col", "type":"bigint"},
         |"date_col":{"cf":"cf1", "col":"date_col", "type":"bigint"},
         |"decimal_col":{"cf":"cf1", "col":"decimal_col", "type":"double"},
         |"double_col":{"cf":"cf1", "col":"double_col", "type":"double"},
         |"float_col":{"cf":"cf1", "col":"float_col", "type":"float"},
         |"long_col":{"cf":"cf1", "col":"long_col", "type":"bigint"},
         |"short_col":{"cf":"cf1", "col":"short_col", "type":"smallint"}
         |}
         |}""".stripMargin

    val dataFrame = spark
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    dataFrame.printSchema()
    dataFrame.show(10)

  }

}
