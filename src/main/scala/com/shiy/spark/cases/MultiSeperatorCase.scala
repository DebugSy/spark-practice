package com.shiy.spark.cases

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by DebugSy on 2019/4/26.
  */
object MultiSeperatorCase {

  def main(args: Array[String]): Unit = {
    val sc: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Multi-Seperator-Case")
      .getOrCreate()

    val date = Seq(("11", "12", "13"), ("21","","23"), ("31",null,"33"))
    import sc.implicits._
    val df: DataFrame = date.toDF("col1", "col2", "col3")
    val seperator = "#@"
    val partitions = df.rdd.mapPartitions(iter => {

      def format(row: Row): Row =  {
        val newArray = new Array[Any](row.length)
        var newData: StringBuilder = new StringBuilder("")
        for (i <- 0 until row.length) {
          val data = row.get(i)
          if (data == null) {
            newData.append(seperator)
          } else {
            newData.append(data.toString).append(seperator)
          }
        }
        Row.apply(newData)
      }

      new Iterator[Row]() {
        override def hasNext: Boolean = {
          iter.hasNext
        }

        override def next(): Row = {
          format(iter.next())
        }
      }
    })
    partitions.collect().map(println(_))
  }

}
