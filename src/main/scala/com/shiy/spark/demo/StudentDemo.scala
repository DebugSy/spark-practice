package com.shiy.spark.demo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object StudentDemo {

  def main(args: Array[String]): Unit = {

//    val sparkConf = new SparkConf().set("spark.testing.memory","2147480000")
    val sc = SparkSession.builder().master("local").appName("student demo").getOrCreate()

    val student: DataFrame = sc.sqlContext.read.option("header", "true").csv("/tmp/data/spark/students.txt")

    student.printSchema()

    student.selectExpr("case when ")


  }

}
