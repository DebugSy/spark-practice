package com.shiy.spark.cases.case03

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, TimestampType}

object HBaseSourceFromFlink_DEBUG {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HBase Sink Null")
      .master("local[1]")
      .getOrCreate();

    spark.udf.register("long_to_timestamp", new UDF1[Long, Timestamp] {
      override def call(t1: Long): Timestamp = new Timestamp(t1)
    }, TimestampType)

    spark.udf.register("long_to_date", new UDF1[Long, Date] {
      override def call(t1: Long): Date = new Date(t1)
    }, DateType)

    val catalog =
      s"""{
         |  "rowkey" : "key",
         |  "columns" : {
         |    "Name" : {
         |      "cf" : "columns",
         |      "col" : "Name",
         |      "type" : "string"
         |    },
         |    "Sex" : {
         |      "cf" : "columns",
         |      "col" : "Sex",
         |      "type" : "string"
         |    },
         |    "Age" : {
         |      "cf" : "rowkey",
         |      "col" : "key",
         |      "type" : "int"
         |    },
         |    "Identity_code" : {
         |      "cf" : "columns",
         |      "col" : "Identity_code",
         |      "type" : "string"
         |    },
         |    "C_time" : {
         |      "cf" : "columns",
         |      "col" : "C_time",
         |      "type" : "string"
         |    },
         |    "Data_long" : {
         |      "cf" : "columns",
         |      "col" : "Data_long",
         |      "type" : "bigint"
         |    },
         |    "Data_double" : {
         |      "cf" : "columns",
         |      "col" : "Data_double",
         |      "type" : "double"
         |    },
         |    "Data_boolean" : {
         |      "cf" : "columns",
         |      "col" : "Data_boolean",
         |      "type" : "boolean"
         |    },
         |    "time_col" : {
         |      "cf" : "columns",
         |      "col" : "time_col",
         |      "type" : "bigint"
         |    },
         |    "Str_time" : {
         |      "cf" : "columns",
         |      "col" : "Str_time",
         |      "type" : "bigint"
         |    },
         |    "Salary" : {
         |      "cf" : "columns",
         |      "col" : "Salary",
         |      "type" : "string"
         |    },
         |    "Null_data" : {
         |      "cf" : "columns",
         |      "col" : "Null_data",
         |      "type" : "string"
         |    },
         |    "City" : {
         |      "cf" : "columns",
         |      "col" : "City",
         |      "type" : "string"
         |    },
         |    "data1" : {
         |      "cf" : "columns",
         |      "col" : "data1",
         |      "type" : "string"
         |    },
         |    "data2" : {
         |      "cf" : "columns",
         |      "col" : "data2",
         |      "type" : "string"
         |    },
         |    "data3" : {
         |      "cf" : "columns",
         |      "col" : "data3",
         |      "type" : "string"
         |    },
         |    "data4" : {
         |      "cf" : "columns",
         |      "col" : "data4",
         |      "type" : "string"
         |    },
         |    "data5" : {
         |      "cf" : "columns",
         |      "col" : "data5",
         |      "type" : "string"
         |    },
         |    "data6" : {
         |      "cf" : "columns",
         |      "col" : "data6",
         |      "type" : "string"
         |    },
         |    "data7" : {
         |      "cf" : "columns",
         |      "col" : "data7",
         |      "type" : "string"
         |    },
         |    "data8" : {
         |      "cf" : "columns",
         |      "col" : "data8",
         |      "type" : "string"
         |    },
         |    "data9" : {
         |      "cf" : "columns",
         |      "col" : "data9",
         |      "type" : "string"
         |    },
         |    "age_sal" : {
         |      "cf" : "columns",
         |      "col" : "age_sal",
         |      "type" : "int"
         |    },
         |    "sex_sal" : {
         |      "cf" : "columns",
         |      "col" : "sex_sal",
         |      "type" : "int"
         |    },
         |    "code1" : {
         |      "cf" : "columns",
         |      "col" : "code1",
         |      "type" : "string"
         |    },
         |    "sal_long" : {
         |      "cf" : "columns",
         |      "col" : "sal_long",
         |      "type" : "bigint"
         |    },
         |    "test_partion_pouduct_zengchang" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_zengchang",
         |      "type" : "int"
         |    },
         |    "test_partion_pouduct_jiangpin" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_jiangpin",
         |      "type" : "string"
         |    },
         |    "shiqu" : {
         |      "cf" : "columns",
         |      "col" : "shiqu",
         |      "type" : "string"
         |    },
         |    "ALIVE" : {
         |      "cf" : "columns",
         |      "col" : "ALIVE",
         |      "type" : "int"
         |    },
         |    "code" : {
         |      "cf" : "columns",
         |      "col" : "code",
         |      "type" : "string"
         |    },
         |    "hour_p_id" : {
         |      "cf" : "columns",
         |      "col" : "hour_p_id",
         |      "type" : "bigint"
         |    },
         |    "csrq" : {
         |      "cf" : "columns",
         |      "col" : "csrq",
         |      "type" : "string"
         |    },
         |    "z_sal" : {
         |      "cf" : "columns",
         |      "col" : "z_sal",
         |      "type" : "int"
         |    },
         |    "test_partion_pouduct_sf" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_sf",
         |      "type" : "int"
         |    },
         |    "test_partion_pouduct_xb" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_xb",
         |      "type" : "int"
         |    },
         |    "test_partion_pouduct_gx" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_gx",
         |      "type" : "int"
         |    },
         |    "test_partion_pouduct_dx" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_dx",
         |      "type" : "int"
         |    },
         |    "test_partion_pouduct_zx" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_zx",
         |      "type" : "int"
         |    },
         |    "test_partion_pouduct_sfz" : {
         |      "cf" : "columns",
         |      "col" : "test_partion_pouduct_sfz",
         |      "type" : "int"
         |    }
         |  },
         |  "table" : {
         |    "namespace" : "default",
         |    "tableCoder" : "PrimitiveType",
         |    "name" : "sink_hbase_rtc_d_new21"
         |  }
         |}""".stripMargin

    val dataFrame = spark
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    dataFrame.printSchema()
    dataFrame.select(col("z_sal")).show(1000, false)

  }

}
