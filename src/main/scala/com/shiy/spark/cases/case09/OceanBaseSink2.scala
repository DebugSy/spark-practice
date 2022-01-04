package com.shiy.spark.cases.case09

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}
import java.util.Properties

object OceanBaseSink2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OceanBase Sink Training")
      .master("local[1]")
      .getOrCreate()

    val schema = StructType(
      StructField("int_col", IntegerType, true) :: Nil
    )

    import spark.implicits._
    val df = Seq(
      0,
      1
    ).toDF("int_col")

    val dataFrame = spark.createDataFrame(df.rdd, schema)
    dataFrame.show()

    val prop = new Properties
    prop.put("user", "root")
    prop.put("password", "")
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://192.168.1.17:2881/test", "url_click_sink_idea", prop)

  }

}
