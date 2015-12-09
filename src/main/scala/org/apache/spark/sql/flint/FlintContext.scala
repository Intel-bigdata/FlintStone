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
package org.apache.spark.sql.flint

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ExtractPythonUDFs
import org.apache.spark.sql.execution.datasources.{PreInsertCastAndRename, PreWriteCheck}
import org.apache.spark.sql.flint.analyzer._
import org.apache.spark.sql.hive.{HiveContext, ResolveHiveWindowFunction}

import com.intel.ssg.bdt.spark.sql.CalciteDialect

class FlintContext(sc: SparkContext) extends HiveContext(sc) {
  self =>
  // TODO: we need to check corresponding Spark version since we integrate with default analyzer
  // and optimizer.
  @transient
  override protected[sql] lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin

  val flintExtendedRules: List[Rule[LogicalPlan]] =
    if (conf.dialect == classOf[CalciteDialect].getCanonicalName) {
      ResolveNaturalJoin :: Nil
    } else Nil

  @transient
  /* An analyzer that uses the Hive metastore, with flint extensions */
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
          catalog.CreateTables ::
          catalog.PreInsertionCasts ::
          ExtractPythonUDFs ::
          ResolveHiveWindowFunction ::
          PreInsertCastAndRename ::
          flintExtendedRules

      override val extendedCheckRules = Seq(
        PreWriteCheck(catalog)
      )
    }

  @transient
  override protected[sql] lazy val optimizer: Optimizer = DefaultOptimizer

  /** Extends QueryExecution with flint specific features. */
  protected[sql] class QueryExecution(logicalPlan: LogicalPlan)
    extends super.QueryExecution(logicalPlan) {

  }
}
