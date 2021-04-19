package com.shiy.spark.cases.case01

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by P0007 on 2020/3/13.
  */
object TestDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("timestamp type write into mysql")
      .master("local[1]")
      .getOrCreate()

    val dataFrame = spark.read.option("header", "true").csv("file:///tmp/data/spark/2020-09-20-*/timestamp-data-*")
    dataFrame.show(false)


  }

}
