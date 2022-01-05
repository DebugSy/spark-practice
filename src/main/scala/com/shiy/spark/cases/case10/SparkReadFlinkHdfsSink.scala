package com.shiy.spark.cases.case10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

object SparkReadFlinkHdfsSink {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkReadFlinkHDFSSink")
      .master("local[1]")
      .getOrCreate()

    val schema = StructType(
      StructField("col1", StringType, true) ::
        StructField("col2", StringType, true) ::
        StructField("col3", StringType, true) ::
        StructField("col4", StringType, true) ::
        StructField("col5", StringType, true) ::
        StructField("col6", StringType, true) ::
        StructField("col7", StringType, true) ::
        StructField("col8", ShortType, true) :: Nil
    )

    val dataFrame = spark.read
      .schema(schema)
      .csv("hdfs://mycluster/tmp/shiy/flink/shiy-url-click-hdfs-sink/2021-08-23--21/")
    print(dataFrame.count())
  }

}
