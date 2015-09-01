package com.intel.ssg.bdt.spark.sql

import org.apache.calcite.sql.SqlNode
import org.apache.calcite.tools.Frameworks
import org.apache.spark.sql.catalyst.{SqlParser, ParserDialect}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.util.Try

class CalciteDialect extends ParserDialect {
  override def parse(sqlText: String): LogicalPlan = {
    getLogicalPlan(sqlText).getOrElse(sqlParser.parse(sqlText))
  }

  @transient protected val sqlParser = new SqlParser

  def getLogicalPlan(sqlText: String): Option[LogicalPlan] = {
    val planner = Frameworks.newConfigBuilder().build()
    val tree: Option[SqlNode] =
      Try(Some(Frameworks.getPlanner(planner).parse(sqlText))).getOrElse(None)
    if (tree.isEmpty) {
      println("failed with Calcite parser, falling back")
      None
    } else {
      println("Calcite parsing passed, start to transform")
      Try(Some(CatalystTransformer.sqlNodeToPlan(tree.get))).getOrElse(None)
    }
  }
}
