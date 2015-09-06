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

import org.apache.calcite.sql._
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical._

private[sql] object CatalystTransformer extends Logging {
  def sqlNodeToPlan(sqlNode: SqlNode): LogicalPlan = sqlNode match {
    case ss: SqlSelect =>
      val from = nodeToRelation(ss.getFrom)
      Project(transformSelectList(ss.getSelectList), from)
    case _ =>
      sys.error("unsupported nodes")
  }

  def transformSelectList(sl: SqlNodeList): Seq[NamedExpression] = {
    sl.getList.toArray.toSeq.asInstanceOf[Seq[SqlNode]].map {
      case si: SqlIdentifier => UnresolvedAttribute(si.toString)
      case _ => sys.error("TODO")
    }
  }

  def nodeToRelation(sqlNode: SqlNode): LogicalPlan = sqlNode match {
    case relation: SqlIdentifier =>
      UnresolvedRelation(relation.names.toArray.toSeq.asInstanceOf[Seq[String]], None)
    case _ =>
      sys.error("TODO")
  }
}
