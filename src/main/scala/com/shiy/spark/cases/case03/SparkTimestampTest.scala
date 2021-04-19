package com.shiy.spark.cases.case03

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, TimestampType}

object SparkTimestampTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("timestamp test")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq((1, 1598844061L, 1598844075491L)).toDF("id", "t1", "t2")
    df.printSchema()
    df.show(false)

    spark.udf.register("long_to_timestamp", new UDF1[Long, Timestamp] {
      override def call(t1: Long): Timestamp = new Timestamp(t1)
    }, TimestampType)

    spark.udf.register("long_to_date", new UDF1[Long, Date] {
      override def call(t1: Long): Date = new Date(t1)
    }, DateType)

    val df2 = df.select(

      col("t1").cast(TimestampType).as("timestamp_cast_t1"),
      col("t2").cast(TimestampType).substr(0, 10).as("timestamp_cast_t2"),

      callUDF("long_to_timestamp", col("t1")).as("timestamp_t1"),
      callUDF("long_to_timestamp", col("t2")).as("timestamp_t2"),
      callUDF("long_to_date", col("t1")).as("date_t1"),
      callUDF("long_to_date", col("t1")).as("date_t2")
    )
    df2.printSchema()
    df2.show(false)
  }

}
