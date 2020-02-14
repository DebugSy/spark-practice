package com.shiy.spark.cases.case01

import java.nio.{ByteBuffer, ByteOrder}
import java.sql.Timestamp

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SparkParquetSink {

  private val timestampBuffer = new Array[Byte](12)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //      .config("spark.sql.parquet.int96AsTimestamp", "true")
      .appName("spark data type test")
      .master("local[1]")
      .getOrCreate()

    val schema = new StructType()
      .add("int_col", IntegerType, nullable = true)
      .add("binary_col", BinaryType, nullable = true)
      .add("string_col", StringType, nullable = true)
      .add("boolean_col", BooleanType, nullable = true)
      .add("byte_col", ByteType, nullable = true)
      .add("date_col", DateType, nullable = true)
      .add("decimal_col1", DecimalType(9, 4), nullable = true)
      .add("decimal_col2", DecimalType(18, 4), nullable = true)
      .add("decimal_col3", DecimalType(38, 4), nullable = true)
      .add("double_col", DoubleType, nullable = true)
      .add("float_col", FloatType, nullable = true)
      .add("long_col", LongType, nullable = true)
      .add("short_col", ShortType, nullable = true)
      .add("timestamp_col", TimestampType, nullable = true)
      .add("null_col", StringType, nullable = true)

    val timestamp = Timestamp.valueOf("2020-02-11 19:52:21.223")
    val (julianDay, timeOfDayNanos) = DateTimeUtils.toJulianDay(timestamp.getTime)
    val buf = ByteBuffer.wrap(timestampBuffer)
    buf.order(ByteOrder.LITTLE_ENDIAN).putLong(timeOfDayNanos).putInt(julianDay)

    val date = DateTimeUtils.toJavaDate((timestamp.getTime / DateTimeUtils.MILLIS_PER_DAY).toInt)

    val data = Seq(
      Row(
        Int.MaxValue, // int_col
        timestampBuffer, // binary_col
        "string_value", // string_col
        false, // boolean_col
        0.toByte, // byte_col
        date, // date_col
        Decimal(9.9), // decimal_col1
        Decimal(99.99), // decimal_col2
        Decimal(999.999), // decimal_col3
        9.9999, // double_col
        9.99f, // float_col
        1234567890L, // long_col
        9.toShort, // short_col
        timestamp, // timestamp_col
        null)
    )

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema).coalesce(1)

    val catalogStr = df.schema.catalogString
    print(catalogStr)

    df.write.mode(SaveMode.Overwrite).parquet("spark_parquet_sink_1")
  }

}
