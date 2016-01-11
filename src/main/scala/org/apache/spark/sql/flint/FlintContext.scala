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
package org.apache.spark.sql.flint

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{OverrideCatalog, FlintAnalyzer, Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.optimizer.{FlintDefaultOptimizer, Optimizer}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.hive.{FlintMetastoreCatalog, FlintStrategies, HiveContext}

class FlintContext(sc: SparkContext) extends HiveContext(sc) with FlintContextTrait

trait FlintContextTrait extends HiveContext {
  self =>

  // TODO: we need to check corresponding Spark version since we integrate with default analyzer
  // and optimizer.
  @transient
  override protected[sql] lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin

  @transient
  /* An analyzer that uses the Hive metastore, with flint extensions */
  override protected[sql] lazy val analyzer: Analyzer =
    new FlintAnalyzer(catalog, functionRegistry, conf)

  @transient
  override protected[sql] lazy val catalog =
    new FlintMetastoreCatalog(metadataHive, this) with OverrideCatalog

  @transient
  private val flintPlanner = new SparkPlanner with FlintStrategies {
    val hiveContext = self

    override def strategies: Seq[Strategy] = experimental.extraStrategies ++ Seq(
      DataSourceStrategy,
      HiveCommandStrategy(self),
      HiveDDLStrategy,
      DDLStrategy,
      TakeOrderedAndProject,
      InMemoryScans,
      HiveTableScans,
      DataSinks,
      Scripts,
      HashAggregation,
      Aggregation,
      FlintLeftSemiJoin,
      EquiJoinSelection,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin
    )
  }

  @transient
  override protected[sql] val planner = flintPlanner

  @transient
  override protected[sql] lazy val optimizer: Optimizer = FlintDefaultOptimizer
}
