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
package com.intel.ssg.bdt.spark.sql.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LogicalPlan}
import org.apache.spark.sql.types.BooleanType

case class NaturalJoin(
  left: LogicalPlan,
  right: LogicalPlan,
  joinType: JoinType,
  condition: Option[Expression]) extends BinaryNode {

  override def output: Seq[Attribute] = {
    val leftNames = left.output.map(_.name)
    val rightNames = right.output.map(_.name)
    val commonNames = leftNames.intersect(rightNames)
    val commonOutput = left.output.filter(att => commonNames.contains(att.name))
    val lUniqueOutput = left.output.filterNot(att => commonNames.contains(att.name))
    val rUniqueOutput = right.output.filterNot(att => commonNames.contains(att.name))
    joinType match {
      case LeftOuter =>
        commonOutput ++ lUniqueOutput ++ rUniqueOutput.map(_.withNullability(true))
      case RightOuter =>
        commonOutput ++ lUniqueOutput.map(_.withNullability(true)) ++ rUniqueOutput
      case FullOuter =>
        commonOutput ++ (lUniqueOutput ++ rUniqueOutput).map(_.withNullability(true))
      case _ =>
        commonOutput ++ lUniqueOutput ++ rUniqueOutput
    }
  }

  def selfJoinResolved: Boolean = left.outputSet.intersect(right.outputSet).isEmpty

  // Joins are only resolved if they don't introduce ambiguous expression ids.
  override lazy val resolved: Boolean = {
    childrenResolved &&
      expressions.forall(_.resolved) &&
      selfJoinResolved &&
      condition.forall(_.dataType == BooleanType)
  }
}
