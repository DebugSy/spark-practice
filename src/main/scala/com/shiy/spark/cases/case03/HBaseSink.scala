package com.shiy.spark.cases.case03

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types._

/**
 * Created by DebugSy on 2019/5/14.
 * 验证往HBase写入null
 */
object HBaseSink {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBase Sink Null")
      .master("local[1]")
      .getOrCreate();

    val schema = StructType(
      StructField("int_col", IntegerType, true) ::
        StructField("string_col", StringType, true) ::
        StructField("boolean_col", BooleanType, true) ::
        StructField("byte_col", ByteType, true) ::
        StructField("timestamp_col", TimestampType, true) ::
        StructField("date_col", DateType, true) ::
        StructField("decimal_col", DecimalType(10, 10), true) ::
        StructField("double_col", DoubleType, true) ::
        StructField("float_col", FloatType, true) ::
        StructField("long_col", LongType, true) ::
        StructField("short_col", ShortType, true) :: Nil
    )

    import spark.implicits._
    val timestamp = Timestamp.valueOf("2020-08-11 15:32:15.009")
    val date = Date.valueOf("2020-08-11")
    val decimal = BigDecimal("0.899")
    val df = Seq(
      (0, "string_col_value_0", false, Byte.MaxValue, timestamp, date, decimal, 0.25410955584989936, 0.424855f, -7933786137407511011L, Short.MaxValue),
      (1, "string_col_value_1", true, Byte.MinValue, timestamp, date, decimal, 0.35410955584989936, 0.524855f, -6933786137407511012L, Short.MinValue)
    ).toDF("int_col", "string_col", "boolean_col", "byte_col", "timestamp_col", "date_col", "decimal_col", "double_col", "float_col", "long_col", "short_col")

    val dataFrame = spark.createDataFrame(df.rdd, schema)

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

    val convertedDF = dataFrame.selectExpr(
      "int_col",
      "string_col",
      "boolean_col",
      "byte_col",
      "cast(timestamp_col as bigint) as timestamp_col",
      "cast(cast(date_col as timestamp) as bigint) as date_col",
      "cast(decimal_col as double) as decimal_col",
      "double_col",
      "float_col",
      "long_col",
      "short_col")

    convertedDF.show(10)

    convertedDF.write
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
