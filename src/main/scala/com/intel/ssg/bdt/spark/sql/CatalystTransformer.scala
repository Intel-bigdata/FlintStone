package com.intel.ssg.bdt.spark.sql

import org.apache.calcite.sql.{SqlKind, SqlNode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

private[sql] object CatalystTransformer {
  def sqlNodeToPlan(sqlNode: SqlNode): LogicalPlan = sqlNode.getKind match {
    case SqlKind.SELECT =>
      println("hehe")
      sys.error("a")
    case _ =>
      println("failed to transform")
      sys.error("unsupported nodes")
  }
}
