package com.shiy.spark.cases.case03

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, TimestampType}

object HBaseSourceFromFlink {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBase Sink Null")
      .master("local[1]")
      .getOrCreate();

    spark.udf.register("long_to_timestamp", new UDF1[Long, Timestamp] {
      override def call(t1: Long): Timestamp = new Timestamp(t1)
    }, TimestampType)

    spark.udf.register("long_to_date", new UDF1[Long, Date] {
      override def call(t1: Long): Date = new Date(t1)
    }, DateType)

    val catalog =
      s"""{
         |"table":{"namespace":"default", "name":"shiy-flink-hbase-sink"},
         |"rowkey":"key",
         |"columns":{
         |"int_col":{"cf":"rowkey", "col":"key", "type":"int"},
         |"string_col":{"cf":"cf1", "col":"string_col", "type":"string"},
         |"boolean_col":{"cf":"cf2", "col":"boolean_col", "type":"boolean"},
         |"byte_col":{"cf":"cf1", "col":"byte_col", "type":"tinyint"},
         |"timestamp_col":{"cf":"cf3", "col":"timestamp_col", "type":"bigint"},
         |"date_col":{"cf":"cf1", "col":"date_col", "type":"bigint"},
         |"decimal_col":{"cf":"cf4", "col":"decimal_col", "type":"double"},
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
    dataFrame.show(1000, false)

    val df2 = dataFrame.selectExpr(
      "long_to_timestamp(timestamp_col) as timestamp_col_new",
      "long_to_date(timestamp_col) as current_date"
    )
    df2.printSchema()
    df2.show(1000, false)

    val df3 = dataFrame.select(
      expr("long_to_timestamp(timestamp_col) as timestamp_col_new"),
      expr("long_to_date(timestamp_col) as current_date")
    )
    df3.show(1000, false)

    val df4 = dataFrame.select(
      callUDF("long_to_timestamp", col("timestamp_col")).as("timestamp_col_new"),
      callUDF("long_to_date", col("timestamp_col")).as("current_date")
    )
    df4.show(1000, false)

  }

}
