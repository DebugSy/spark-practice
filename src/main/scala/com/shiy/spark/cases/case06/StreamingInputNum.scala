package com.shiy.spark.cases.case06

import org.apache.spark.sql.SparkSession

object StreamingInputNum {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("")
      .master("local[1]")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate();

    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
