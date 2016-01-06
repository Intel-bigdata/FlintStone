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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.spark.sql.types.{BooleanType, DataType}

trait SubQueryExpression extends Unevaluable {
  def subquery: LogicalPlan

  override def dataType: DataType = BooleanType
  override def foldable: Boolean = false
  override def nullable: Boolean = false

  /**
    * Replace the subquery with new one, usually will be used when resolving the subquery.
    */
  def withNewSubQuery(newSubquery: LogicalPlan): this.type
}

/**
  * Exist subquery expression, only used in subquery predicate only.
  *
  * positive: true means EXISTS, other wise means NOT EXISTS
  *
  * NOTICE: Exists is a LeafExpression, and we need to resolve the subquery
  * explicitly in analyzer rule.
  */
case class Exists(subquery: LogicalPlan, positive: Boolean)
  extends LeafExpression with SubQueryExpression {

  override def withNewSubQuery(newSubquery: LogicalPlan): this.type = {
    this.copy(subquery = newSubquery).asInstanceOf[this.type]
  }

  override lazy val resolved = true

  override def toString: String = if (positive) {
    s"Exists(${subquery.asCode})"
  } else {
    s"NotExists(${subquery.asCode})"
  }
}

/**
  * In subquery expression, only used in subquery predicate only.
  *
  * child: The referenced key in WHERE clause for IN / NOT IN
  *  e.g. SELECT value FROM src a WHERE a.key IN (SELECT key FROM src1 b WHERE b.key > 10)
  *  The child expression is the 'a.key'
  *
  * positive: true means EXISTS, other wise means NOT EXISTS
  *
  * NOTICE: InSubquery is a LeafExpression, and we need to resolve its subquery
  * explicitly in analyzer rule.
  */
case class InSubquery(child: Expression, subquery: LogicalPlan, positive: Boolean)
  extends UnaryExpression with SubQueryExpression {

  override def withNewSubQuery(newSubquery: LogicalPlan): this.type = {
    this.copy(subquery = newSubquery).asInstanceOf[this.type]
  }

  override lazy val resolved = child.resolved

  override def toString: String = if (positive) {
    s"InSubQuery($child, ${subquery.asCode})"
  } else {
    s"NotInSubQuery($child, ${subquery.asCode})"
  }
}
