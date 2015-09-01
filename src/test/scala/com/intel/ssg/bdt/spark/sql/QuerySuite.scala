package com.intel.ssg.bdt.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, QueryTest}
import org.scalatest.BeforeAndAfterAll

class QuerySuite extends QueryTest with BeforeAndAfterAll {
  val sc = new SparkContext(
    "local[2]",
    "test-sql-context",
    new SparkConf().set("spark.sql.testkey", "true"))
  val sqlContext = new SQLContext(sc)
  override def beforeAll() = {
    sqlContext.setConf("spark.sql.dialect", classOf[CalciteDialect].getCanonicalName)
    val df100 = sqlContext.createDataFrame(sc.parallelize(
      (1 to 100).map(i => (i, i.toString))))
    df100.registerTempTable("testData100")
    val df = sqlContext.createDataFrame(Seq((1, 2)))
    df.registerTempTable("testData")
  }

  test("select") {
    assert(sqlContext.sql("SELECT _1 from testData").collect() === Array(Row(1)))
  }
}
