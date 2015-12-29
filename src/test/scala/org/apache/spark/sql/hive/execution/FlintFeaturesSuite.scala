/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.hive.test.TestFlint
import org.apache.spark.sql.hive.test.TestFlint._
import org.apache.spark.sql.hive.test.TestFlint.implicits._
import org.apache.spark.sql.{Row, SQLContext, QueryTest}
import org.apache.spark.sql.test.SQLTestUtils

class FlintFeaturesSuite extends QueryTest with SQLTestUtils {
  override def _sqlContext: SQLContext = TestFlint
  private val sqlContext = _sqlContext

  test("natural join") {
    val nt1 = sparkContext.parallelize(1 to 3).map(x => (x, x + 1)).toDF("x", "y")
    nt1.registerTempTable("nt1")
    val nt2 = sparkContext.parallelize(1 to 4).map(x => (x, 0 - x)).toDF("x", "z")
    nt2.registerTempTable("nt2")

    val expected =
      Row(1, 2, -1) ::
      Row(2, 3, -2) ::
      Row(3, 4, -3) :: Nil

    val actual = sql("SELECT * from nt1 natural join nt2")

    checkAnswer(actual, expected)

    dropTempTable("nt1")
    dropTempTable("nt2")
  }

  test("trim") {
    assert(true)
  }
}
