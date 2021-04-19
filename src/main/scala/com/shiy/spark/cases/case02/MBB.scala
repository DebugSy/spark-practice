package com.shiy.spark.cases.case02

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MBB {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ma bao bao zhuan shu ce shi")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      Seq("2019", "08", ""),
      Seq("2019", "09", ""),
      Seq("2019", "10", ""),
      Seq("2019", "11", ""),
      Seq("2019", "12", "")
    )
    val rdd = spark.sparkContext.parallelize(data)
    val rowRDD = rdd.map(d => Row.fromSeq(d))
    val dataFrame = spark.createDataFrame(rowRDD, schema)
    dataFrame.show()

    val resultRDD = dataFrame.rdd.map(row => Row.apply(row.apply(0), row.apply(1), row.apply(0) + "-" + row.apply(1)))
    val resultDF = spark.createDataFrame(resultRDD, schema)
    resultDF.show()

    dataFrame.createOrReplaceTempView("tab")
    val dataFrame1 = spark.sql(
      "select batch_date_time, month, concat(batch_date_time, '-', month) as uuid_num from tab")
    dataFrame1.show()
  }

  val schema = StructType(
    StructField("batch_date_time", DataTypes.StringType, false) ::
      StructField("month", DataTypes.StringType, false) ::
      StructField("uuid_num", DataTypes.StringType, false) :: Nil
  )

}
