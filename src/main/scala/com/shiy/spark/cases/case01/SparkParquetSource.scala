package com.shiy.spark.cases.case01

import org.apache.spark.sql.SparkSession

object SparkParquetSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .config("spark.sql.parquet.int96AsTimestamp", "true")
      .appName("HBase Sink Null")
      .master("local[1]")
      .getOrCreate()

    val dataFrame = spark.read.parquet("/tmp/stest/filter_sink_8/part-0-22")

    dataFrame.show()
  }

}
