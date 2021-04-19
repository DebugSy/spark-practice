package com.shiy.spark.cases.case04

import org.apache.spark.sql.SparkSession

object ParquetReadTraining {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Parquet Read Training")
      .master("local[1]")
      .getOrCreate();

    val df = spark.read.parquet("E:\\tmp\\stest\\filter_sink_8\\2020-08-28-15-36")
    df.printSchema()
    df.show()

    val cnt = df.count()
    print(cnt)
  }

}
