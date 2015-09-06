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

import org.apache.calcite.sql.SqlNode
import org.apache.calcite.tools.Frameworks
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.{SqlParser, ParserDialect}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.util.Try

class CalciteDialect extends ParserDialect with Logging {
  override def parse(sqlText: String): LogicalPlan = {
    getLogicalPlan(sqlText).getOrElse({
      log.warn("Calcite parse/transform failed")
      if (CalciteConf.strictMode) {
        sys.error("Parse failed.")
      } else {
        sqlParser.parse(sqlText)
      }
    })
  }

  @transient protected val sqlParser = new SqlParser

  def getLogicalPlan(sqlText: String): Option[LogicalPlan] = {
    val planner = Frameworks.newConfigBuilder().build()
    val tree: Option[SqlNode] =
      Try(Some(Frameworks.getPlanner(planner).parse(sqlText))).getOrElse(None)
    if (tree.isEmpty) {
      log.warn("failed with Calcite parser.")
      None
    } else {
      log.info("Calcite parsing passed, start to transform.")
      Try(Some(CatalystTransformer.sqlNodeToPlan(tree.get))).getOrElse(None)
    }
  }
}
