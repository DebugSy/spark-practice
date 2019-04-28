package com.shiy.spark.demo

import java.net.URI

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by DebugSy on 2019/2/19.
  */
object SqlSourceTest {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("SqlSourceTest").master("local[*]").getOrCreate()

    val df = sc.emptyDataFrame

    val schema = StructType(
      StructField("id", DataTypes.StringType, false) ::
        StructField("name", DataTypes.StringType, false) ::
        StructField("age", DataTypes.IntegerType, false) :: Nil
    )
    val data = sc.createDataFrame(df.rdd, schema)

//    data.createOrReplaceTempView("shiy_student_dataset")
    val sql = "select shiy_student_dataset1.id as id,shiy_student_dataset1.name as name,shiy_student_dataset2.age as age " +
      "from shiy_student_dataset1,shiy_student_dataset2 " +
      "where shiy_student_dataset1.id = shiy_student_dataset2.id and shiy_student_dataset1.age > 20"

      //提取tableName
//    val logicalPlan = sc.sessionState.sqlParser.parsePlan(sql)
//    val tableName = logicalPlan.collect{case r: UnresolvedRelation => r.tableName}
//    println(tableName)

    val conf = new SQLConf
    val parser = new SparkSqlParser(conf)
    val logicalPlan = parser.parsePlan(sql)
    val externalCatalog = new InMemoryCatalog(new SparkConf())
    val catalog = new SessionCatalog(externalCatalog, FunctionRegistry.builtin, conf)
    catalog.createTempView("shiy_student_dataset1", data.queryExecution.logical, false)
    catalog.createTempView("shiy_student_dataset2", data.queryExecution.logical, false)
    val analyzer = new Analyzer(catalog, conf)
    val plan = analyzer.execute(logicalPlan)
    println(plan.verboseString)
    analyzer.checkAnalysis(plan)
    sc.stop()
  }

}
