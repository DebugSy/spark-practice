package com.shiy.spark.cases

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

/**
  * Created by DebugSy on 2019/5/14.
  * 验证往HBase写入null
  */
object HBaseSinkNull {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBase Sink Null")
      .master("local[*]")
      .getOrCreate();

    import spark.implicits._
    val df = Seq(
      (1,"a","a"),
      (2,"","b"),
      (3,"",""),
      (4,null,null)
    ).toDF("id","name","addr")

    val catalog = s"""{
                     |"table":{"namespace":"default", "name":"shiy_table"},
                     |"rowkey":"key",
                     |"columns":{
                     |"id":{"cf":"rowkey", "col":"key", "type":"int"},
                     |"name":{"cf":"cf1", "col":"name", "type":"string"},
                     |"addr":{"cf":"cf1", "col":"addr", "type":"string"}
                     |}
                     |}""".stripMargin

    df.write
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
