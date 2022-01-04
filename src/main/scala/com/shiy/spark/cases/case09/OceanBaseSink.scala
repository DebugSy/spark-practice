package com.shiy.spark.cases.case09

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.JDBC_TXN_ISOLATION_LEVEL
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}
import java.util.Properties

object OceanBaseSink {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OceanBase Sink Training")
      .master("local[1]")
      .getOrCreate()

    val schema = StructType(
      StructField("int_col", IntegerType, true) ::
        StructField("string_col", StringType, true) ::
        StructField("boolean_col", BooleanType, true) ::
        StructField("timestamp_col", TimestampType, true) ::
        StructField("date_col", DateType, true) ::
        StructField("decimal_col", DecimalType(18,10), true) ::
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
      (0, "string_col_value_0", false, timestamp, date, decimal, 0.25410955584989936, 0.424855f, -7933786137407511011L, Short.MaxValue),
      (1, "string_col_value_1", true, timestamp, date, decimal, 0.35410955584989936, 0.524855f, -6933786137407511012L, Short.MinValue)
    ).toDF("int_col", "string_col", "boolean_col", "timestamp_col", "date_col", "decimal_col", "double_col", "float_col", "long_col", "short_col")



    val dataFrame = spark.createDataFrame(df.rdd, schema)
    dataFrame.show()

    val prop = new Properties
    prop.put("user", "root")
    prop.put("password", "")
    prop.put(JDBC_TXN_ISOLATION_LEVEL, "READ_COMMITTED")
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://192.168.1.17:2881/test", "url_click_sink_idea", prop)

  }

}
