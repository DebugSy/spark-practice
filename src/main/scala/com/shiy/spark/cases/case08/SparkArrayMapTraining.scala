package com.shiy.spark.cases.case08

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkArrayMapTraining {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("spark array and map type training")
      .getOrCreate();

    val schema = StructType(
      StructField("col1", DataTypes.IntegerType, false) ::
        StructField("col2", DataTypes.StringType, false) ::
        StructField("col3", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false) ::
        StructField("col4", DataTypes.createArrayType(DataTypes.StringType), false) ::
        Nil
    )

    //    val dataFrame = readDummyData(spark, schema)
    val dataFrame = readJson(spark, schema)
    dataFrame.printSchema()
    dataFrame.show(false)
  }

  def readDummyData(spark: SparkSession, schema: StructType): DataFrame = {
    val seq = Seq(
      Seq(1, "A", Map(("k1", "v1"), ("k2", "v2"))),
      Seq(2, "B", Map(("k1", "v1"), ("k2", "v2"))),
      Seq(3, "C", Map(("k1", "v1"), ("k2", "v2")))
    )
    val rdd = spark.sparkContext.parallelize(seq)
    val rowRDD = rdd.map(s => Row.fromSeq(s))
    val dataFrame = spark.createDataFrame(rowRDD, schema)
    dataFrame
  }

    def readCsv(spark: SparkSession): DataFrame = {
      val seq = Seq(
        "1,\"A\","
      )

      spark.read.csv()
    }

  def readJson(spark: SparkSession, schema: StructType): DataFrame = {
    val seq = Seq(
      "{\"col1\": 1,\"col2\": \"A\",\"col3\":{\"k1\":\"v3A\",\"k2\":\"v4A\"}, \"col4\": [\"v5A\",\"v6A\"]}",
      "{\"col1\": 2,\"col2\": \"B\",\"col3\":{\"k1\":\"v3B\",\"k2\":\"v4B\"}, \"col4\": [\"v5B\",\"v6B\"]}",
      "{\"col1\": 3,\"col2\": \"C\",\"col3\":{\"k1\":\"v3C\",\"k2\":\"v4C\"}, \"col4\": [\"v5C\",\"v6A\"]}"
    )
    val rdd: RDD[String] = spark.sparkContext.parallelize(seq)
    val dataFrame = spark.read
      .schema(schema)
      .json(rdd)
    dataFrame
  }

}
