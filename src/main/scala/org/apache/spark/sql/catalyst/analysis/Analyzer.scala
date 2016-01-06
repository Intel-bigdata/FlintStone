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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.flint.analyzer.{FlintResolveOrderbyNumber, ResolveNaturalJoin}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, AggregateExpression2, AggregateFunction2}
import org.apache.spark.sql.catalyst.plans.{LeftSemi, LeftAnti}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, CatalystConf}
import org.apache.spark.sql.types._

/**
 * A trivial [[Analyzer]] with an [[EmptyCatalog]] and [[EmptyFunctionRegistry]]. Used for testing
 * when all relations are already filled in and the analyzer needs only to resolve attribute
 * references.
 */
object SimpleAnalyzer
  extends FlintAnalyzer(EmptyCatalog, EmptyFunctionRegistry, new SimpleCatalystConf(true))

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
 */
class FlintAnalyzer(
    catalog: Catalog,
    registry: FunctionRegistry,
    conf: CatalystConf,
    maxIterations: Int = 100)
  extends Analyzer(catalog, registry, conf, maxIterations) with FlintCheckAnalysis {

  override lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution ::
      WindowsSubstitution ::
      Nil : _*),
    Batch("Resolution", fixedPoint,
      RewriteFilterSubQuery ::
      ResolveRelations ::
      FlintResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolveSortReferences ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      ResolveNaturalJoin ::
      HiveTypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  /**
   * Rewrite the [[Exists]] [[In]] with left semi join or anti join.
   *
   * 1. Some of the key concepts:
   *  Correlated:
   *   References the attributes of the parent query within subquery, we call that Correlated.
   *  e.g. We reference the "a.value", which is the attribute in parent query, in the subquery.
   *
   *   SELECT a.value FROM src a
   *   WHERE a.key in (
   *     SELECT b.key FROM src1 b
   *     WHERE a.value > b.value)
   *
   *  Unrelated:
   *   Do not have any attribute reference to its parent query in the subquery.
   *  e.g.
   *   SELECT a.value FROM src a WHERE a.key IN (SELECT key FROM src WHERE key > 100);
   *
   * 2. Basic Logic for the Transformation
   *    EXISTS / IN => LEFT SEMI JOIN
   *    NOT EXISTS / NOT IN => LEFT ANTI JOIN
   *
   *    In logical plan demostration, we support the cases like below:
   *
   *    e.g. EXISTS / NOT EXISTS
   *    SELECT value FROM src a WHERE (NOT) EXISTS (SELECT 1 FROM src1 b WHERE a.key < b.key)
   *        ==>
   *    SELECT a.value FROM src a LEFT (ANTI) SEMI JOIN src1 b WHERE a.key < b.key
   *
   *    e.g. IN / NOT IN
   *    SELECT value FROM src a WHERE key (NOT) IN (SELECT key FROM src1 b WHERE a.value < b.value)
   *       ==>
   *    SELECT value FROM src a LEFT (ANTI) SEMI JOIN src1 b ON a.key = b.key AND a.value < b.value
   *
   *    e.g. IN / NOT IN with other conjunctions
   *    SELECT value FROM src a
   *    WHERE key (NOT) IN (
   *      SELECT key FROM src1 b WHERE a.value < b.value
   *    ) AND a.key > 10
   *       ==>
   *    SELECT value
   *      (FROM src a WHERE a.key > 10)
   *    LEFT (ANTI) SEMI JOIN src1 b ON a.key = b.key AND a.value < b.value
   *
   * 3. There are also some limitations:
   *   a. IN/NOT IN subqueries may only select a single column.
   *    e.g.(bad example)
   *    SELECT value FROM src a WHERE EXISTS (SELECT key, value FROM src1 WHERE key > 10)
   *   b. EXISTS/NOT EXISTS must have one or more correlated predicates.
   *    e.g.(bad example)
   *    SELECT value FROM src a WHERE EXISTS (SELECT 1 FROM src1 b WHERE b.key > 10)
   *   c. References to the parent query is only supported in the WHERE clause of the subquery.
   *    e.g.(bad example)
   *    SELECT value FROM src a WHERE key IN (SELECT a.key + b.key FROM src1 b)
   *   d. Only a single subquery can support in IN/EXISTS predicate.
   *    e.g.(bad example)
   *    SELECT value FROM src WHERE key IN (SELECT xx1 FROM xxx1) AND key in (SELECT xx2 FROM xxx2)
   *   e. Disjunction is not supported in the top level.
   *    e.g.(bad example)
   *    SELECT value FROM src WHERE key > 10 OR key IN (SELECT xx1 FROM xxx1)
   *   f. Implicit reference expression substitution to the parent query is not supported.
   *    e.g.(bad example)
   *    SELECT min(key) FROM src a HAVING EXISTS (SELECT 1 FROM src1 b WHERE b.key = min(a.key))
   *
   * 4. TODOs
   *   a. More pretty message to user why we failed in parsing.
   *   b. Support multiple IN / EXISTS clause in the predicates.
   *   c. Implicit reference expression substitution to the parent query
   *   d. ..
   */
  object RewriteFilterSubQuery extends Rule[LogicalPlan] with PredicateHelper {
    // This is to extract the SubQuery expression and other conjunction expressions.
    def unapply(condition: Expression): Option[(Expression, Seq[Expression])] = {
      if (condition.resolved == false) {
        return None
      }

      val conjunctions = splitConjunctivePredicates(condition).map(_ transformDown {
          // Remove the Cast expression for SubQueryExpression.
          case Cast(f: SubQueryExpression, BooleanType) => f
        }
      )

      val (subqueries, others) = conjunctions.partition(c => c.isInstanceOf[SubQueryExpression])
      if (subqueries.isEmpty) {
        None
      } else if (subqueries.length > 1) {
        // We don't support the cases with multiple subquery in the predicates now like:
        // SELECT value FROM src
        // WHERE
        //   key IN (SELECT key xxx) AND
        //   key IN (SELECT key xxx)
        // TODO support this case in the future since it's part of the `standard SQL`
        throw new AnalysisException(
          s"Only 1 SubQuery expression is supported in predicates, but we got $subqueries")
      } else {
        val subQueryExpr = subqueries(0).asInstanceOf[SubQueryExpression]
        // try to resolve the subquery

        val subquery = FlintAnalyzer.this.execute(subQueryExpr.subquery) match {
          case Distinct(child) =>
            // Distinct is useless for semi join, ignore it.
            // e.g. SELECT value FROM src WHERE key IN (SELECT DISTINCT key FROM src b)
            // which is equvilent to
            // SELECT value FROM src WHERE key IN (SELECT key FROM src b)
            // The reason we discard the DISTINCT keyword is we don't want to make
            // additional rule for DISTINCT operator in the `def apply(..)`
            child
          case other => other
        }
        Some((subQueryExpr.withNewSubQuery(subquery), others))
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case f if f.childrenResolved == false => f

      case f @ Filter(RewriteFilterSubQuery(subquery, others), left) =>
        subquery match {
          case Exists(Project(_, Filter(condition, right)), positive) =>
            checkAnalysis(right)
            if (condition.resolved) {
              // Apparently, it should be not resolved here, since EXIST should be correlated.
              throw new AnalysisException(
                s"Exists/Not Exists operator SubQuery must be correlated, but we got $condition")
            }
            val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
            Join(newLeft, right,
              if (positive) LeftSemi else LeftAnti,
              Some(FlintResolveReferences.tryResolveAttributes(condition, right)))

          case Exists(right, positive) =>
            throw new AnalysisException(s"Exists/Not Exists operator SubQuery must be Correlated," +
              s"but we got $right")

          case InSubquery(key, Project(projectList, Filter(condition, right)), positive) =>
            // we don't support nested correlation yet, so the `right` must be resolved.
            checkAnalysis(right)
            if (projectList.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In SubQuery Expression, but we got $projectList")
            } else {
              // This is a workaround to solve the ambiguous references issue like:
              // SELECT 'value FROM src WHERE 'key IN (SELECT 'key FROM src b WHERE 'key > 100)
              //
              // Literally, we will transform the SQL into:
              //
              // SELECT 'value FROM src
              // LEFT SEMI JOIN src b
              //   ON 'key = 'key and 'key > 100 -- this is reference ambiguous for 'key!
              //
              // The ResolveReferences.tryResolveAttributes will partially resolve the project
              // list and filter condition of the subquery, and then what we got looks like:
              //
              // SELECT 'value FROM src
              // LEFI SEMI JOIN src b
              //   ON 'key = key#123 and key#123 > 100
              //
              // And then we will leave the remaining unresolved attributes for the other rules
              // in Analyzer.
              val rightKey = FlintResolveReferences.tryResolveAttributes(projectList(0), right)

              if (!rightKey.resolved) {
                throw new AnalysisException(s"Cannot resolve the projection for SubQuery $rightKey")
              }

              // This is for the SQL with conjunction like:
              //
              // SELECT value FROM src a
              // WHERE key > 5 AND key IN (SELECT key FROM src1 b key >7) AND key < 10
              //
              // ==>
              // SELECT value FROM (src a
              //   WHERE key > 5 AND key < 10)
              // LEFT SEMI JOIN src1 b ON a.key = b.key AND b.key > 7
              //
              // Ideally, we should transform the original plan into
              // SELECT value FROM src a
              // LEFT SEMI JOIN src1 b
              // ON a.key = b.key AND b.key > 7 AND a.key > 5 AND a.key < 10
              //
              // However, the former one only requires few code change to support
              // the multiple subquery for IN clause, and less overhead for Optimizer
              val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
              val newCondition = Some(
                And(
                  FlintResolveReferences.tryResolveAttributes(condition, right),
                  EqualTo(rightKey, key)))

              Join(newLeft, right, if (positive) LeftSemi else LeftAnti, newCondition)
            }

          case InSubquery(key, Project(projectList, right), positive) =>
            // we don't support nested correlation yet, so the `right` must be resolved.
            checkAnalysis(right)
            if (projectList.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In SubQuery Expression, but we got $projectList")
            } else {
              if (!projectList(0).resolved) {
                // We don't support reference in the outer column in the subquery projection list.
                // e.g. SELECT value FROM src a WHERE key in (SELECT b.key + a.key FROM src b)
                // That means, the project list of the subquery MUST BE resolved already, otherwise
                // throws exception.
                throw new AnalysisException(s"Cannot resolve the projection ${projectList(0)}")
              }
              val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
              Join(newLeft, right,
                if (positive) LeftSemi else LeftAnti,
                Some(EqualTo(projectList(0), key)))
            }

          case InSubquery(key, right @ Aggregate(grouping, aggregations, child), positive) =>
            if (aggregations.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In SubQuery Expression, but we got $aggregations")
            } else {
              // we don't support nested correlation yet, so the `child` must be resolved.
              checkAnalysis(child)
              val rightKey =
                FlintResolveReferences.tryResolveAttributes(aggregations(0), child) match {
                case e if !e.resolved =>
                  throw new AnalysisException(
                    s"Cannot resolve the aggregation $e")
                case e: NamedExpression => e
                case other =>
                  // place a space before `in_subquery_key`, hopefully end user
                  // will not take that as the field name or alias.
                  Alias(other, " in_subquery_key")()
              }

              val newLeft = others.reduceOption(And).map(Filter(_, left)).getOrElse(left)
              val newRight = Aggregate(grouping, rightKey :: Nil, child)
              val newCondition = Some(EqualTo(rightKey.toAttribute, key))

              Join(newLeft, newRight, if (positive) LeftSemi else LeftAnti, newCondition)
            }

          case InSubquery(key,
            f @ Filter(condition, right @ Aggregate(grouping, aggregations, child)), positive) =>
            if (aggregations.length != 1) {
              throw new AnalysisException(
                s"Expect only 1 projection in In Subquery Expression, but we got $aggregations")
            } else {
              // we don't support nested correlation yet, so the `child` must be resolved.
              checkAnalysis(child)
              val rightKey =
                FlintResolveReferences.tryResolveAttributes(aggregations(0), child) match {
                case e if !e.resolved =>
                  throw new AnalysisException(
                    s"Cannot resolve the aggregation $e")
                case e: NamedExpression => e
                case other => Alias(other, " in_subquery_key")()
              }

              val newLeft =
                Filter(others.foldLeft(
                  FlintResolveReferences.tryResolveAttributes(condition(0), child))(And(_, _)),
                  left)
              val newRight = Aggregate(grouping, rightKey :: Nil, child)
              val newCondition = Some(EqualTo(rightKey.toAttribute, key))
              Join(newLeft, newRight, if (positive) LeftSemi else LeftAnti, newCondition)
            }
        }
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
   * a logical plan node's children.
   */
  object FlintResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p

      // If the projection list contains Stars, expand it.
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child.output, resolver)
            case UnresolvedAlias(f @ UnresolvedFunction(_, args, _)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(child = f.copy(children = expandedArgs)) :: Nil
            case UnresolvedAlias(c @ CreateArray(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case UnresolvedAlias(c @ CreateStruct(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case o => o :: Nil
          },
          child)
      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child.output, resolver)
            case o => o :: Nil
          }
        )

      // If the aggregate function argument contains Stars, expand it.
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        a.copy(
          aggregateExpressions = a.aggregateExpressions.flatMap {
            case s: Star => s.expand(a.child.output, resolver)
            case o => o :: Nil
          }
        )

      // Special handling for cases when self-join introduce duplicate expression ids.
      case j @ Join(left, right, _, _) if !j.selfJoinResolved =>
        val conflictingAttributes = left.outputSet.intersect(right.outputSet)
        logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

        right.collect {
          // Handle base relations that might appear more than once.
          case oldVersion: MultiInstanceRelation
              if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
            val newVersion = oldVersion.newInstance()
            (oldVersion, newVersion)

          // Handle projects that create conflicting aliases.
          case oldVersion @ Project(projectList, _)
              if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

          case oldVersion @ Aggregate(_, aggregateExpressions, _)
              if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))

          case oldVersion: Generate
              if oldVersion.generatedSet.intersect(conflictingAttributes).nonEmpty =>
            val newOutput = oldVersion.generatorOutput.map(_.newInstance())
            (oldVersion, oldVersion.copy(generatorOutput = newOutput))

          case oldVersion @ Window(_, windowExpressions, _, _, child)
              if AttributeSet(windowExpressions.map(_.toAttribute)).intersect(conflictingAttributes)
                .nonEmpty =>
            (oldVersion, oldVersion.copy(windowExpressions = newAliases(windowExpressions)))
        }
        // Only handle first case, others will be fixed on the next pass.
        .headOption match {
          case None =>
            /*
             * No result implies that there is a logical plan node that produces new references
             * that this rule cannot handle. When that is the case, there must be another rule
             * that resolves these conflicts. Otherwise, the analysis will fail.
             */
            j
          case Some((oldRelation, newRelation)) =>
            val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
            val newRight = right transformUp {
              case r if r == oldRelation => newRelation
            } transformUp {
              case other => other transformExpressions {
                case a: Attribute => attributeRewrites.get(a).getOrElse(a)
              }
            }
            j.copy(right = newRight)
        }

      // When resolve `SortOrder`s in Sort based on child, don't report errors as
      // we still have chance to resolve it based on grandchild
      case s @ Sort(ordering, global, child) if child.resolved =>
        val newOrdering = resolveSortOrders(ordering, child, throws = false)
        Sort(newOrdering, global, child)

      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, join, outer, qualifier, output, child)
        if child.resolved && !generator.resolved =>
        val newG = generator transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            withPosition(u) { child.resolve(nameParts, resolver).getOrElse(u) }
          case UnresolvedExtractValue(child, fieldExpr) =>
            ExtractValue(child, fieldExpr, resolver)
        }
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q transformExpressionsUp  {
          case u @ UnresolvedAlias(expr: NamedExpression) if expr.resolved => expr
          case u @ UnresolvedAttribute(nameParts) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            val result =
              withPosition(u) { q.resolveChildren(nameParts, resolver).getOrElse(u) }
            logDebug(s"Resolving $u to $result")
            result
          case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
            ExtractValue(child, fieldExpr, resolver)
        }
    }

    // Try to resolve the attributes from the given logical plan
    // TODO share the code with above rules? How?
    def tryResolveAttributes(expr: Expression, q: LogicalPlan): Expression = {
      checkAnalysis(q)
      val projection = Project(q.output, q)

      logTrace(s"Attempting to resolve ${expr.simpleString}")
      expr transformUp  {
        case u @ UnresolvedAlias(expr) => expr
        case u @ UnresolvedAttribute(nameParts) =>
          // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
          val result =
            withPosition(u) { projection.resolveChildren(nameParts, resolver).getOrElse(u) }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
          ExtractValue(child, fieldExpr, resolver)
      }
    }

    def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
      expressions.map {
        case a: Alias => Alias(a.child, a.name)()
        case other => other
      }
    }

    def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
      AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)
  }

  private def resolveSortOrders(ordering: Seq[SortOrder], plan: LogicalPlan, throws: Boolean) = {
    ordering.map { order =>
      // Resolve SortOrder in one round.
      // If throws == false or the desired attribute doesn't exist
      // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
      // Else, throw exception.
      try {
        val newOrder = order transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            plan.resolve(nameParts, resolver).getOrElse(u)
          case UnresolvedExtractValue(child, fieldName) if child.resolved =>
            ExtractValue(child, fieldName, resolver)
          case IntegerLiteral(index) if plan.resolved =>
            plan.output(index - 1)
        }
        newOrder.asInstanceOf[SortOrder]
      } catch {
        case a: AnalysisException if !throws => order
      }
    }
  }
}
