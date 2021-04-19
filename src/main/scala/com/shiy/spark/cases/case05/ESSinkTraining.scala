package com.shiy.spark.cases.case05

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object ESSinkTraining {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("elasticsearch sink")
      .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")

    val schema = StructType(
      StructField("int_col", IntegerType, true) ::
        StructField("string_col", StringType, true) ::
        StructField("boolean_col", BooleanType, true) ::
        StructField("byte_col", ByteType, true) ::
        StructField("timestamp_col", TimestampType, true) ::
        StructField("date_col", DateType, true) ::
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
      (0, "string_col_value_0", false, Byte.MaxValue, timestamp, date,  0.25410955584989936, 0.424855f, -7933786137407511011L, Short.MaxValue),
      (1, "string_col_value_1", true, Byte.MinValue, timestamp, date,  0.35410955584989936, 0.524855f, -6933786137407511012L, Short.MinValue)
    ).toDF("int_col", "string_col", "boolean_col", "byte_col", "timestamp_col", "date_col","double_col", "float_col", "long_col", "short_col")

    val dataFrame = spark.createDataFrame(df.rdd, schema)

    val options = Map("pushdown" -> "true",
      ConfigurationOptions.ES_INDEX_AUTO_CREATE -> "true",
      ConfigurationOptions.ES_NODES -> "192.168.1.82:9204",
      ConfigurationOptions.ES_PORT -> "9204",
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER -> "admin",
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS -> "admin"
    )

    import org.elasticsearch.spark.sql._
    dataFrame.saveToEs("shiy-test-spark-es-sink/type1", options)
  }

}
