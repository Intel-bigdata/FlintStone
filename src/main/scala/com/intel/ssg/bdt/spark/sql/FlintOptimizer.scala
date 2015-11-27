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

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.optimizer._

object FlintOptimizer extends Optimizer {
  val batches =
  // SubQueries are only needed for analysis and can be removed before execution.
    Batch("Remove SubQueries", FixedPoint(100),
      EliminateSubQueries) ::
      Batch("Aggregate", FixedPoint(100),
        ReplaceDistinctWithAggregate,
        RemoveLiteralFromGroupExpressions) ::
      Batch("Operator Optimizations", FixedPoint(100),
        // Operator push down
        SetOperationPushDown,
        SamplePushDown,
        PushPredicateThroughJoin,
        PushPredicateThroughProject,
        PushPredicateThroughGenerate,
        ColumnPruning,
        // Operator combine
        ProjectCollapsing,
        CombineFilters,
        CombineLimits,
        // Constant folding
        NullPropagation,
        OptimizeIn,
        ConstantFolding,
        LikeSimplification,
        BooleanSimplification,
        RemovePositive,
        SimplifyFilters,
        SimplifyCasts,
        SimplifyCaseConversionExpressions) ::
      Batch("Decimal Optimizations", FixedPoint(100),
        DecimalAggregates) ::
      Batch("LocalRelation", FixedPoint(100),
        ConvertToLocalRelation) :: Nil
}
