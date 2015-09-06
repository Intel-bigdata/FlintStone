/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.ssg.bdt.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, QueryTest}
import org.scalatest.BeforeAndAfterAll

class QuerySuite extends QueryTest with BeforeAndAfterAll {
  val sc = new SparkContext(
    "local[2]",
    "test-sql-context",
    new SparkConf().set("spark.sql.testkey", "true").set("spark.sql.caseSensitive", "false"))
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
