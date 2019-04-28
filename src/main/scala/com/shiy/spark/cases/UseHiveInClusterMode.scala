package com.shiy.spark.cases

import org.apache.spark.sql.SparkSession

/**
  * Created by DebugSy on 2019/4/28.
  */
object UseHiveInClusterMode {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql("SELECT * FROM shiy_student").show()

    spark.stop()
  }

}
