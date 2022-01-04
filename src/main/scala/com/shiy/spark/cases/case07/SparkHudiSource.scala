package com.shiy.spark.cases.case07

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object SparkHudiSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("spark hudi sink training")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val basePath = "hdfs:///tmp/shiy/flink/flink-hudi-sink/2021-04-28--14"

    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
    print("总数: " + tripsSnapshotDF.count())
    tripsSnapshotDF.show(false)
  }

}
