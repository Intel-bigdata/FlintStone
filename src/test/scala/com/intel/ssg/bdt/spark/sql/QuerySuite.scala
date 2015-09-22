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

import org.apache.spark.sql.types._
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
      (0 until 100).map(i => (i, i.toString))))
    df100.registerTempTable("testData100")
    val df = sqlContext.createDataFrame(Seq((1, 2)))
    df.registerTempTable("testData")

    val rddCustomers = sc.textFile("C:\\Users\\zhangc1\\SparkCal\\spark-calcite-parser\\src\\test\\customers.txt")
    val schemaStringCust = "customer_id name city state zip_code sal"
    val schemaCust = StructType(schemaStringCust.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDDCust = rddCustomers.map(_.split(",")).map(p => Row(p(0).trim,p(1).trim,p(2).trim,p(3).trim,p(4).trim,p(5).trim))
    // 将模式应用于RDD对象。
    val dfCustomers = sqlContext.createDataFrame(rowRDDCust, schemaCust)

    // 将DataFrame注册为表
    dfCustomers.registerTempTable("customers")

    val rddEmps = sc.textFile("C:\\Users\\zhangc1\\SparkCal\\spark-calcite-parser\\src\\test\\emp.txt")
    val schemaStringEmp = "id name type"
    val schemaEmp = StructType(schemaStringEmp.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDDEmp = rddEmps.map(_.split(",")).map(p => Row(p(0).trim,p(1).trim,p(2).trim))
    // 将模式应用于RDD对象。
    val dfEmps = sqlContext.createDataFrame(rowRDDEmp, schemaEmp)

    // 将DataFrame注册为表
    dfEmps.registerTempTable("emps")
  }

  test("select") {
    //val custNames = sqlContext.sql("SELECT city FROM customers where name = 'John'")
    // SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
    // 可以按照顺序访问结果行的各个列。
    //custNames.map(t => "city: " + t(0)).collect().foreach(println)

    //select from
    checkAnswer(sqlContext.sql("SELECT _1 from testData"), Array(Row(1)))

    //select from where
    checkAnswer(sqlContext.sql("SELECT distinct city FROM customers"), Array(Row("Austin"), Row("Dallas"), Row("Houston"), Row("San Antonio")))

    //select from where
    checkAnswer(sqlContext.sql("SELECT city FROM customers where name = 'John'"), Row("Austin"))

    //select from where group by
    checkAnswer(sqlContext.sql("SELECT sum(sal) from customers group by city"), Array(Row(1200.0), Row(200.0), Row(300.0), Row(400.0)))

    //select from where group by having
    checkAnswer(sqlContext.sql("SELECT sum(sal) from customers group by city having sum(sal) > 1000"), Row(1200.0))

    //select from where order by
    checkAnswer(sqlContext.sql("SELECT sal from customers order by sal"), Array(Row("100"), Row("200"), Row("300"), Row("400"), Row("500"), Row("600")))

    //select from where order by limit
    checkAnswer(sqlContext.sql("SELECT sal from customers order by sal limit 3"), Array(Row("100"), Row("200"), Row("300")))

    //select from and
    checkAnswer(sqlContext.sql("SELECT sal from customers where name = 'John' and city = 'Austin'"), Array(Row("100")))

    //select from or
    checkAnswer(sqlContext.sql("SELECT sal from customers where name = 'John' or city = 'Austin'"), Array(Row("100"), Row("500"), Row("600")))

    //like

    //select from in/between
    checkAnswer(sqlContext.sql("SELECT sal from customers where name in ('John', 'James')"), Array(Row("100"), Row("500")))

    //select from as(prjection)
    checkAnswer(sqlContext.sql("SELECT sal as s, city as c from customers"), Array(Row("100", "Austin"),Row("200", "Dallas"),Row("300", "Houston"),Row("400", "San Antonio"), Row("500", "Austin"), Row("600", "Austin")))

    //select join on
    checkAnswer(sqlContext.sql("SELECT customers.sal from customers join emps on customers.name = emps.name"), Array(Row("100"), Row("200"), Row("300"), Row("400"), Row("500"), Row("600")))

    //select join on
    checkAnswer(sqlContext.sql("SELECT c.sal from customers as c join emps as e on c.name = e.name"), Array(Row("100"), Row("200"), Row("300"), Row("400"), Row("500"), Row("600")))

    //select avg
    checkAnswer(sqlContext.sql("SELECT avg(sal) from customers"), Array(Row(350.0)))

    //select count
    checkAnswer(sqlContext.sql("SELECT count(distinct city) from customers"), Array(Row(4)))
    checkAnswer(sqlContext.sql("SELECT count(sal) as s from customers where name = 'kobe'"), Array(Row(1)))
    checkAnswer(sqlContext.sql("SELECT count(*) from customers"), Array(Row(6)))

    //select first
    checkAnswer(sqlContext.sql("SELECT first(name) from customers"), Array(Row("John")))

    //select last
    checkAnswer(sqlContext.sql("SELECT last(name) from customers"), Array(Row("kobe")))

    //select max
    checkAnswer(sqlContext.sql("SELECT max(sal) from customers"), Array(Row("600")))

    //select min
    checkAnswer(sqlContext.sql("SELECT min(sal) from customers"), Array(Row("100")))

    //select upper
    checkAnswer(sqlContext.sql("SELECT upper(name) from customers where name= 'kobe'"), Array(Row("KOBE")))

    //select lower
    checkAnswer(sqlContext.sql("SELECT lower(state) from customers where name = 'kobe'"), Array(Row("tx")))
  }
}