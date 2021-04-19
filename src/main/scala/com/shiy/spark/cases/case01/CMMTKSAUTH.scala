package com.shiy.spark.cases.case01

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by P0007 on 2020/3/11.
  */
object CMMTKSAUTH {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("活体认证数据分析")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    val rawDataset = spark.read.textFile("/tmp/data/flink/path1/", "/tmp/data/flink/path2/", "/tmp/data/flink/path3/")
    rawDataset.count()
    val lineDataset = rawDataset.map(str => str.split("(?<!\\\\),"))
    val lengthDS = lineDataset.map(arr => arr.length)
    lengthDS.groupBy("value").count().show(100)
    val s = lengthDS.cache()

    lineDataset.map(arr => arr(1).equals(""))

    val value = lineDataset.map(arr => arr.length)
    lineDataset.filter(arr => arr.length != 40).show(10)
    val rdd = lineDataset.rdd.map(arr => Row.fromSeq(arr))
    val df = spark.createDataFrame(rdd, schema)
    df.show(10)

    spark.close()
  }

  val schema = StructType(
    StructField("HR_JRN_NO", DataTypes.StringType, false) ::
      StructField("SCORE", DataTypes.StringType, false) ::
      StructField("THOU_VTH", DataTypes.StringType, false) ::
      StructField("TEN_THOU_VTH", DataTypes.StringType, false) ::
      StructField("HUN_THOU_VTH", DataTypes.StringType, false) ::
      StructField("MILLION_VTH", DataTypes.StringType, false) ::
      StructField("PIC_URL", DataTypes.StringType, false) ::
      StructField("PRV_RMK", DataTypes.StringType, false) ::
      StructField("CHK_FLG", DataTypes.StringType, false) ::
      StructField("USR_NO", DataTypes.StringType, false) ::
      StructField("MBL_NO", DataTypes.StringType, false) ::
      StructField("CUS_NM", DataTypes.StringType, false) ::
      StructField("CTR_NO", DataTypes.StringType, false) ::
      StructField("CERT_VALID_DATE", DataTypes.StringType, false) ::
      StructField("CERT_VALID_TIME", DataTypes.StringType, false) ::
      StructField("DATA", DataTypes.StringType, false) ::
      StructField("AUTH_TYP", DataTypes.StringType, false) ::
      StructField("TM_SMP", DataTypes.StringType, false) ::
      StructField("REQUEST_ID", DataTypes.StringType, false) ::
      StructField("ERR_MSG", DataTypes.StringType, false) ::
      StructField("USED_TM", DataTypes.StringType, false) ::
      StructField("REQ_TM", DataTypes.StringType, false) ::
      StructField("ATTA_FLG", DataTypes.StringType, false) ::
      StructField("MONO_FLG", DataTypes.StringType, false) ::
      StructField("AUTH_BUS_TYP", DataTypes.StringType, false) ::
      StructField("PLAT", DataTypes.StringType, false) ::
      StructField("FACE_URL", DataTypes.StringType, false) ::
      StructField("DELTA", DataTypes.StringType, false) ::
      StructField("SYN_FACE_CFDC", DataTypes.StringType, false) ::
      StructField("SYN_FACE_THR", DataTypes.StringType, false) ::
      StructField("MASK_CFDC", DataTypes.StringType, false) ::
      StructField("MASK_THR", DataTypes.StringType, false) ::
      StructField("SCRN_REP_CFDC", DataTypes.StringType, false) ::
      StructField("SCRN_REP_THR", DataTypes.StringType, false) ::
      StructField("FACE_REP", DataTypes.StringType, false) :: Nil
  )

}
