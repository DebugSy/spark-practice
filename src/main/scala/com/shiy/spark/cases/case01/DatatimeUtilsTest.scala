package com.shiy.spark.cases.case01

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.util.DateTimeUtils

object DatatimeUtilsTest {

  def main(args: Array[String]): Unit = {
    val timestamp = Timestamp.valueOf("2020-02-11 19:52:21.223")
    val tuple = DateTimeUtils.toJulianDay(timestamp.getTime * 1000)
    print(tuple)
  }

}
