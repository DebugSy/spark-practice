package com.shiy.spark.cases.case05

import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object ESSourceTraining {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("elasticsearch sink")
      .getOrCreate()

    val options = Map("pushdown" -> "true",
      ConfigurationOptions.ES_INDEX_AUTO_CREATE -> "true",
      ConfigurationOptions.ES_NODES -> "192.168.1.82:9204",
      ConfigurationOptions.ES_PORT -> "9204",
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER -> "admin",
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS -> "admin"
    )

    import org.elasticsearch.spark.sql._
    val dataFrame = spark.esDF("shiy-flink-sink-test/_doc", options)
    dataFrame.printSchema()
    dataFrame.show()
  }

}
