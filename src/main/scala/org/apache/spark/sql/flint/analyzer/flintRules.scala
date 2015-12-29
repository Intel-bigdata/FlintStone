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
package org.apache.spark.sql.flint.analyzer

import com.intel.ssg.bdt.spark.sql.plans.logical.NaturalJoin
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Removes natural joins.
  */
object ResolveNaturalJoin extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case p if !p.resolved => p // Skip unresolved nodes.
    case j @ NaturalJoin(left, right, joinType, condition) =>
      val joinNames = left.output.map(_.name).intersect(right.output.map(_.name))
      val leftKeys = joinNames.map(keyName => left.output.find(_.name == keyName).get)
      val rightKeys = joinNames.map(keyName => right.output.find(_.name == keyName).get)
      val joinPairs = leftKeys.zip(rightKeys)
      val newCondition = (condition ++ joinPairs.map {
        case (l, r) => EqualTo(l, r)
      }).reduceLeftOption(And)
      Project(j.outerProjectList, Join(left, right, joinType, newCondition))
  }
}
