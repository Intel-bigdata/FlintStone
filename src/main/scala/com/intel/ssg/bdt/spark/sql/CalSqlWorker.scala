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
package com.intel.ssg.bdt.spark.sql

import java.sql.{Timestamp, Date}
import com.intel.ssg.bdt.spark.sql.plans.logical._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlCase
import org.apache.spark.unsafe.types.CalendarInterval
import scala.language.implicitConversions

import org.apache.calcite.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import com.intel.ssg.bdt.spark.sql.catalyst.expressions.{FlintLike, FlintStringTrim, FlintStringTrimLeft, FlintStringTrimRight}

// support select and insert both
class CalSqlWorker(sqlNode: SqlNode) {
  // val calparser: calParser = new calParser()
  // val sqlNode: SqlNode = calparser.getSqlNode(input)

  def getLogicalPlan: LogicalPlan = {
    nodeToPlan(sqlNode)
  }

  def nodeToPlan(subSqlNode: SqlNode) : LogicalPlan = {
    val sqlKindName = subSqlNode.getKind.name()
    sqlKindName match {
      case SELECT =>
        dealWithSelectNode(subSqlNode.asInstanceOf[SqlSelect])

      case JOIN =>
        dealWithJoinNode(subSqlNode.asInstanceOf[SqlJoin])

      case WITH =>
        val withNode = subSqlNode.asInstanceOf[SqlWith]
        val asNodeList = withNode.getOperandList.get(0).asInstanceOf[SqlNodeList].getList
        val bodyPlan = nodeToPlan(withNode.getOperandList.get(1))

        var withList = ListBuffer[Tuple2[String, Subquery]]()

        for (index <- 0 until asNodeList.size()){
          val Item = asNodeList.get(index).asInstanceOf[SqlWithItem]
          withList +=
            Tuple2(Item.name.getSimple, Subquery(Item.name.getSimple, nodeToPlan(Item.query)))
        }
        With(bodyPlan, withList.toMap)

      case AS =>
        // becasue it's the as type so that there must be some node in right node
        val as_sqlnode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = as_sqlnode.getOperandList.get(0)
        val rightNode = as_sqlnode.getOperandList.get(1).asInstanceOf[SqlIdentifier]

        if (rightNode.isSimple || rightNode.isStar) {
          leftNode.getKind.name() match {
            case IDENTIFIER =>
              UnresolvedRelation(
                leftNode.asInstanceOf[SqlIdentifier].names,
                if (rightNode == null) None else Some(rightNode.getSimple))
            case _ =>
              Subquery(rightNode.getSimple, nodeToPlan(leftNode))
          }
        } else {
          sys.error("wrong \'AS\' input.")
        }

      case ORDER_BY =>
        dealWithOrderByNode(subSqlNode.asInstanceOf[SqlOrderBy])

      case UNION =>
        val basicallnode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftnode = basicallnode.getOperandList.get(0)
        val rightnode = basicallnode.getOperandList.get(1)
        basicallnode.getOperator.getName match {
          case UNION => Distinct(Union(nodeToPlan(leftnode), nodeToPlan(rightnode)))
          case _ => Union(nodeToPlan(leftnode), nodeToPlan(rightnode))
        }

      case INTERSECT =>
        val basicallnode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftnode = basicallnode.getOperandList.get(0)
        val rightnode = basicallnode.getOperandList.get(1)
        Intersect(nodeToPlan(leftnode), nodeToPlan(rightnode))

      case EXCEPT =>
        val basicallnode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftnode = basicallnode.getOperandList.get(0)
        val rightnode = basicallnode.getOperandList.get(1)
        Except(nodeToPlan(leftnode), nodeToPlan(rightnode))

      case IDENTIFIER =>
        val identiNode = subSqlNode.asInstanceOf[SqlIdentifier]
        UnresolvedRelation(identiNode.names, None)

      case INSERT =>
        val insertNode = subSqlNode.asInstanceOf[SqlInsert]
        val tableDest = insertNode.getTargetTable
        val subSelectNode = insertNode.getSource
        InsertIntoTable(
          nodeToPlan(tableDest),
          Map.empty[String, Option[String]],
          nodeToPlan(subSelectNode), overwrite = false, ifNotExists = false)

      case _ =>
        sys.error("TODO")
    }
  }

  // sqlbasiccall && literal && identifier
  def nodeToExpr(subSqlNode: SqlNode) : Expression = {
    val nodeType = subSqlNode.getKind.name()

    nodeType match {
      case AS =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = nodeToExpr(basicCallNode.getOperandList.get(0)) // expr
        val rightNode = basicCallNode.getOperandList.get(1).asInstanceOf[SqlIdentifier] // ident

        if (rightNode.isSimple) {
          Alias(leftNode, rightNode.getSimple)()
        } else {
          sys.error("wrong as input.")
        }

      case TRIM =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        if (basicCallNode.getOperandList.size() == 3) {
          basicCallNode.getOperandList.get(0).asInstanceOf[SqlLiteral].toValue match {
            case BOTH =>
              FlintStringTrim(
                nodeToExpr(basicCallNode.getOperandList.get(2)),
                nodeToExpr(basicCallNode.getOperandList.get(1)))
            case LEADING =>
              FlintStringTrimLeft(
                nodeToExpr(basicCallNode.getOperandList.get(2)),
                nodeToExpr(basicCallNode.getOperandList.get(1)))
            case TRAILING =>
              FlintStringTrimRight(
                nodeToExpr(basicCallNode.getOperandList.get(2)),
                nodeToExpr(basicCallNode.getOperandList.get(1)))
            case _ =>
              sys.error("unsupported trim expression")
          }
        } else {
          sys.error("unsupported trim expression")
        }

      case TIMES =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)

        Multiply(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case DIVIDE =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)

        Divide(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case PLUS =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)

        Add(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case MINUS =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)

        Subtract(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case EQUALS =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)

        EqualTo(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case NOT_EQUALS =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        Not(EqualTo(nodeToExpr(leftNode), nodeToExpr(rightNode)))

      case GREATER_THAN =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        GreaterThan(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case LESS_THAN =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        LessThan(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case LESS_THAN_OR_EQUAL =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        LessThanOrEqual(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case GREATER_THAN_OR_EQUAL =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        GreaterThanOrEqual(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case BETWEEN =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator
        val operandList = basicCallNode.getOperandList

        if (operandList.size() == 3){
          val X = operandList.get(0)
          val Y = operandList.get(1)
          val Z = operandList.get(2)

          val andExpr = And(GreaterThanOrEqual(
            nodeToExpr(X), nodeToExpr(Y)), LessThanOrEqual(nodeToExpr(X), nodeToExpr(Z)))

          if (operator.getName.equals("BETWEEN")){
            andExpr
          } else {
            Not(andExpr)
          }
        } else {
          sys.error("wrong input, check \"BETWEEN\" usage.")
        }

      case OR =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        Or(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case AND =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        And(nodeToExpr(leftNode), nodeToExpr(rightNode))

      case OTHER =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator
        val leftNode = basicCallNode.getOperandList.get(0)
        val rightNode = basicCallNode.getOperandList.get(1)
        operator.getName match {
          case OTHER_OR => ConcatWs(Literal("") +: basicCallNode.getOperandList.map(nodeToExpr))
          case _ => sys.error("not support now.")
        }

      case LIKE =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator

        val likeExpr = basicCallNode.getOperandList.size match {
          case 2 =>
            // TODO use map here
            Like(
              nodeToExpr(basicCallNode.getOperandList.get(0)),
              nodeToExpr(basicCallNode.getOperandList.get(1)))
          case 3 =>
            FlintLike(
              nodeToExpr(basicCallNode.getOperandList.get(0)),
              nodeToExpr(basicCallNode.getOperandList.get(1)),
              nodeToExpr(basicCallNode.getOperandList.get(2)))
          case _ => sys.error("unsupported like usage")
        }
        if (operator.getName.equals("LIKE")) likeExpr else Not(likeExpr)

      case IN =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator

        val leftNode = basicCallNode.getOperandList.get(0)

        if (basicCallNode.getOperandList.get(1).getKind.name().equals(SELECT)) {
          if (operator.getName.equals("IN")){
            InSubquery(nodeToExpr(basicCallNode.getOperandList.get(0)),
                       nodeToPlan(basicCallNode.getOperandList.get(1)), true)
          }
          else {
            InSubquery(nodeToExpr(basicCallNode.getOperandList.get(0)),
                       nodeToPlan(basicCallNode.getOperandList.get(1)), false)
          }
        } else {
          val rightNodeList = basicCallNode.getOperandList.get(1).asInstanceOf[SqlNodeList]
          // must be a list
          val rightSeq = rightNodeList.map(nodeToExpr)

          val inExpr = In(nodeToExpr(leftNode), rightSeq.toSeq)
          if (operator.getName.equals("IN")) inExpr else Not(inExpr)
        }

      case EXISTS =>
        val ExistsList = subSqlNode.asInstanceOf[SqlBasicCall].getOperandList
        if (ExistsList.size() == 1 && ExistsList.get(0).getKind.name().equals(SELECT)) {
          Exists(nodeToPlan(ExistsList.get(0)), true)
        }
        else {
          sys.error("unsupport exists usage")
        }

      case IS_NOT_NULL =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        IsNotNull(nodeToExpr(basicCallNode.getOperandList.get(0)))

      case IS_NULL =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        IsNull(nodeToExpr(basicCallNode.getOperandList.get(0)))

      case NOT =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        Not(nodeToExpr(basicCallNode.getOperandList.get(0)))

      case CASE =>
        val caseNode = subSqlNode.asInstanceOf[SqlCase]
        val whenList = caseNode.getWhenOperands
        val thenList = caseNode.getThenOperands
        val elseLiter = caseNode.getElseOperand.asInstanceOf[SqlLiteral]

        if (whenList.size() ==  thenList.size()){
          var whenSeq = ListBuffer[Expression]()

          for (index <- 0 until whenList.size()){
            whenSeq += nodeToExpr(whenList.get(index))
            whenSeq += nodeToExpr(thenList.get(index))
          }

          val branchs =
            if (elseLiter.toValue == null) whenSeq else whenSeq += nodeToExpr(elseLiter)

          CaseWhen(branchs)
        } else {
          sys.error("wrong input, check \"CASE WHEN\" usage.")
        }

      case CAST =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        // val operator = basicCallNode.getOperator
        val left = basicCallNode.getOperandList.get(0)
        val right = basicCallNode.getOperandList.get(1)

        val datatype =
          right.asInstanceOf[SqlDataTypeSpec].getTypeName.getSimple match {
            case STRING => StringType
            case VARCHAR => StringType
            case CHAR => StringType
            case TIMESTAMP => TimestampType
            case DOUBLE => DoubleType
            // case FIXEDDECIMALTYPE =>
            case DECIMAL =>
              val precision = right.asInstanceOf[SqlDataTypeSpec].getPrecision
              var scale = right.asInstanceOf[SqlDataTypeSpec].getScale
              if (scale < 0) scale = 0
              DecimalType(precision, scale)
            case DATE => DateType
            case INTEGER => IntegerType
            case LONG => LongType
            case BOOL => BooleanType
            case BINARY => BinaryType
            case _ =>
              sys.error("TODO, other datatype may not be valided for calcite or spark now.")
          }

        Cast(nodeToExpr(left), datatype)

      case MINUS_PREFIX =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator
        val operand = basicCallNode.getOperandList.get(0)
        if (operator.getName equals "-") UnaryMinus(nodeToExpr(operand)) else nodeToExpr(operand)

      case PLUS_PREFIX =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator
        val operand = basicCallNode.getOperandList.get(0)
        nodeToExpr(operand)

      case OTHER_FUNCTION =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator.asInstanceOf[SqlFunction]
        val isdistinct = basicCallNode.getFunctionQuantifier
        val operand = basicCallNode.getOperandList
        val functionName = operator.getName

        if (functionName.equals(EXTRACT)) {
          val extraction = operand.get(0).asInstanceOf[SqlIntervalQualifier].timeUnitRange.name()
          val child = UnresolvedAttribute(operand.get(1).asInstanceOf[SqlIdentifier].names)
          // UnresolvedExtractValue(
          //   UnresolvedAttribute(child), UnresolvedAttribute.quoted(extraction))
          extraction match {
            case HOUR => Hour(child)
            case MINUTE => Minute(child)
            case SECOND => Second(child)
            case MONTH => Month(child)
            case YEAR => Year(child)
          }
        } else if (functionName.equals(NULLIF) && operand.size() == 2) {
          val whenele = EqualTo(nodeToExpr(operand.get(0)), nodeToExpr(operand.get(1)))
          val thenele = Literal.create(null, NullType)
          val elseele = nodeToExpr(operand.get(0))

          CaseWhen(Seq(whenele, thenele, elseele))
        } else if (operand.get(0).isInstanceOf[SqlIdentifier] && operand.get(
          0).asInstanceOf[SqlIdentifier].isStar) {
          if (functionName.equals(COUNT) && operand.size() == 1) {
            Count(Literal(1))
          } else {
            sys.error("invalid expression")
          }
        } else {
          if (isdistinct != null && isdistinct.getValue.toString.equals(DISTINCT)){
            functionName match {
              case SUM => SumDistinct(nodeToExpr(operand.get(0)))
              case COUNT => CountDistinct(operand.map(nodeToExpr))
              case _ => UnresolvedFunction(functionName, operand.map(nodeToExpr), isDistinct = true)
            }
          } else {
            UnresolvedFunction(functionName, operand.map(nodeToExpr), isDistinct = false)
          }
        }

      case IDENTIFIER =>
        val identiNode = subSqlNode.asInstanceOf[SqlIdentifier]
        if (identiNode.isStar) {
          if (identiNode.names.size() == 1) {
            UnresolvedStar(None)
          } else if (identiNode.names.size() == 2) {
            UnresolvedStar(Option(identiNode.names.get(0)))
          } else {
            sys.error("wrong select input.")
          }
        } else {
          if (identiNode.isSimple) {
            UnresolvedAttribute.quoted(identiNode.getSimple)
            // } else if (identiNode.names.size() == 2) {
            //   UnresolvedExtractValue(
            //     UnresolvedAttribute.quoted(identiNode.names.get(0)),
            //     Literal(identiNode.names.get(1)))
            //   UnresolvedAttribute(identiNode.names.mkString("."))
          } else {
            UnresolvedAttribute(identiNode.names.mkString("."))
          }
        }

      case _ => nodeToLiteral(subSqlNode)
    }
  }

  // literal
  def nodeToLiteral(literalSqlNode : SqlNode) : Expression = {
    val literalNode = literalSqlNode.asInstanceOf[SqlLiteral]
    literalNode.getTypeName match {
      case SqlTypeName.NULL => Literal.create(null, NullType)

      case SqlTypeName.BOOLEAN =>
        if (literalNode.booleanValue()) {
          Literal.create(true, BooleanType)
        } else {
          Literal.create(false, BooleanType)
        }

      case SqlTypeName.DECIMAL | SqlTypeName.DOUBLE =>
        val numeriLit = literalNode.asInstanceOf[SqlNumericLiteral]
        if (numeriLit.getScale == 0) {
          // int
          val intvalue =
            scala.math.BigDecimal(numeriLit.getValue.asInstanceOf[java.math.BigDecimal])
          val intresult = intvalue match {
            case v if intvalue.isValidInt => v.toIntExact
            case v if intvalue.isValidLong => v.toLongExact
            case v => v.underlying()
          }
          Literal(intresult)
        } else {
          val doublevalue =
            scala.math.BigDecimal(numeriLit.getValue.asInstanceOf[java.math.BigDecimal])
          Literal(doublevalue.underlying())
        }

      case SqlTypeName.CHAR =>
        Literal.create(literalNode.getStringValue, StringType)
      //            case SqlTypeName.BINARY => {}
      //            case SqlTypeName.TIME => {}

      case SqlTypeName.INTERVAL_YEAR_MONTH =>
        val year_or_month = literalNode.getValue.asInstanceOf[SqlIntervalLiteral.IntervalValue]
        val monthSum = year_or_month.getIntervalQualifier.timeUnitRange.name() match {
          case YEAR => year_or_month.getIntervalLiteral.toDouble.toInt * 12
          case MONTH => year_or_month.getIntervalLiteral.toDouble.toInt
        }
        Literal.create(new CalendarInterval(monthSum, 0), CalendarIntervalType)

      case SqlTypeName.INTERVAL_DAY_TIME =>
        val time_in_day = literalNode.getValue.asInstanceOf[SqlIntervalLiteral.IntervalValue]
        val secondSum = time_in_day.getIntervalQualifier.timeUnitRange.name() match {
          case DAY =>
            time_in_day.getIntervalLiteral.toDouble.toLong * CalendarInterval.MICROS_PER_DAY
          case HOUR =>
            time_in_day.getIntervalLiteral.toDouble.toLong * CalendarInterval.MICROS_PER_HOUR
          case MINUTE =>
            time_in_day.getIntervalLiteral.toDouble.toLong * CalendarInterval.MICROS_PER_MINUTE
          case SECOND =>
            time_in_day.getIntervalLiteral.toDouble.toLong * CalendarInterval.MICROS_PER_SECOND
        }
        Literal.create(new CalendarInterval(0, secondSum), CalendarIntervalType)

      case SqlTypeName.DATE =>
        val date = literalNode.asInstanceOf[SqlDateLiteral]
        val datestring = date.toString.substring(6, 16)
        Literal.create(Date.valueOf(datestring), DateType)

      case SqlTypeName.TIMESTAMP =>
        val timestamp = literalNode.asInstanceOf[SqlTimestampLiteral].toString
        val timestampparam = timestamp.substring(11, timestamp.size - 1)
        Literal.create(Timestamp.valueOf(timestampparam), TimestampType)

      case _ =>
        sys.error("TODO")
    }
  } // ok

  def  dealWithSelectNode(selectNode: SqlSelect) : LogicalPlan = {
    val withHaving = prepareSelect(selectNode)

    val limitNode = selectNode.getFetch // limit expression

    val withLimit = if (limitNode == null) withHaving else Limit(nodeToExpr(limitNode), withHaving)

    withLimit
  }

  def prepareSelect(selectNode: SqlSelect): LogicalPlan = {
    val isDistinct = selectNode.isDistinct // distinct
    val selectNodeList = selectNode.getSelectList // select projection(as) : SqlNodeList
    val fromNode = selectNode.getFrom // from relations
    val whereNode = selectNode.getWhere // where expression
    val groupByNodeList = selectNode.getGroup // group by expression
    val havingNode = selectNode.getHaving // having expresssion

    val base = nodeToPlan(fromNode)

    val withFilter = if (whereNode == null) base else Filter(nodeToExpr(whereNode), base)

    val withProjection =
      if (groupByNodeList == null) {
        // Project(assignAliases(selectNodeList.map(nodeToExpr).toSeq), withFilter)
        Project(selectNodeList.getList.map(nodeToExpr).map(UnresolvedAlias), withFilter)
      } else {
        val list = groupByNodeList.getList.map(nodeToExpr)
        Aggregate(list, selectNodeList.getList.map(nodeToExpr).map(UnresolvedAlias), withFilter)
      }

    val withDistinct = if (isDistinct) {
      Distinct(withProjection)
    } else {
      withProjection
    }

    val withHavnig =
      if (havingNode == null) withDistinct else Filter(nodeToExpr(havingNode), withDistinct)

    withHavnig
  }

  def assignAliases(exprs: Seq[Expression]) : Seq[NamedExpression] = {
    exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c$i")()
    }
  }

  def  dealWithJoinNode(sqlnode: SqlJoin) : LogicalPlan = {
    val left = sqlnode.getLeft
    val right = sqlnode.getRight
    val joinType = sqlnode.getJoinType
    val conditionType = sqlnode.getConditionType
    val isNatural = sqlnode.isNatural

    val jt =
      joinType.name() match {
        case COMMA_JOIN => Inner
        case INNER_JOIN => Inner
        case LEFT_JOIN => LeftOuter
        case RIGHT_JOIN => RightOuter
        case FULL => FullOuter
        case _ => Inner
      }

    if (conditionType.name().equals(USING)){
      val condition = sqlnode.getCondition.asInstanceOf[SqlNodeList]
      if (left.isInstanceOf[SqlIdentifier] && right.isInstanceOf[SqlIdentifier]) {
        var conditionBuf = ListBuffer[Expression]()
        for (ele <- condition){
          val elename = ele.asInstanceOf[SqlIdentifier].names.get(0)
          val leftname = left.asInstanceOf[SqlIdentifier].names.toList ++ List(elename)
          val rightname = right.asInstanceOf[SqlIdentifier].names.toList ++ List(elename)
          // scalastyle:off println
          println(leftname, rightname)
          // scalastyle:on println
          // first each make an equal
          val equalExpr = EqualTo(UnresolvedAttribute(leftname), UnresolvedAttribute(rightname))
          // add expr to buf
          conditionBuf += equalExpr
        }

        val expressionPara = combineAdd(conditionBuf)
        if (isNatural) {
          NaturalJoin(nodeToPlan(left), nodeToPlan(right), jt, Option(expressionPara))
        } else {
          Join(nodeToPlan(left), nodeToPlan(right), jt, Option(expressionPara))
        }
      } else {
        sys.error("subquery and using not support!")
      }
    } else {
      val condition = sqlnode.getCondition
      if (isNatural) {
        NaturalJoin(
          nodeToPlan(left),
          nodeToPlan(right),
          jt,
          if (condition != null) Some(nodeToExpr(condition)) else None)
      } else {
        Join(
          nodeToPlan(left),
          nodeToPlan(right),
          jt,
          if (condition != null) Some(nodeToExpr(condition)) else None)
      }
    }
  }

  def combineAdd(exprList: ListBuffer[Expression]) : Expression = {
    if (exprList.size == 1) {
      exprList.get(0)
    } else if (exprList.size == 2) {
      And(exprList.get(0), exprList.get(1))
    } else {
      val first = exprList.remove(0)
      // scalastyle:off println
      println(exprList)
      // scalastyle:on println
      And(first, combineAdd(exprList))
    }
  }

  def  dealWithOrderByNode(sqlnode: SqlOrderBy) : LogicalPlan = {
    // especially fetch to get limit
    val query = sqlnode.query.asInstanceOf[SqlSelect]
    val selectList = query.getSelectList
    val orderList = sqlnode.orderList

    val withHaving = prepareSelect(query)

    val orderSeq = ListBuffer[SortOrder]()
    for (ele <- orderList) {
      ele.getKind.name() match {
        case DESCENDING =>
          val descNode = ele.asInstanceOf[SqlBasicCall]
          val operand = descNode.getOperandList.get(0)
          if (operand.isInstanceOf[SqlIdentifier]) {
            orderSeq += SortOrder(nodeToExpr(operand), Descending)
          } else {
            val index = operand.asInstanceOf[SqlNumericLiteral].bigDecimalValue().intValue() - 1
            // if ident and else expr
            orderSeq += {selectList.get(index).getKind.name() match {
              case IDENTIFIER => SortOrder(nodeToExpr(selectList.get(index)), Descending)
              case OTHER_FUNCTION => SortOrder(UnresolvedAttribute(s"c$index"), Descending)
              case _ => SortOrder(UnresolvedAttribute(s"_c$index"), Descending)
            }}
          }

        case IDENTIFIER =>
          // val IdenNode = ele.asInstanceOf[SqlIdentifier]
          orderSeq += SortOrder(nodeToExpr(ele), Ascending)

        case LITERAL =>
          val index = ele.asInstanceOf[SqlNumericLiteral].bigDecimalValue().intValue() - 1
          // if ident and else expr
          orderSeq += {selectList.get(index).getKind.name() match {
            case IDENTIFIER => SortOrder(nodeToExpr(selectList.get(index)), Ascending)
            case OTHER_FUNCTION => SortOrder(UnresolvedAttribute(s"c$index"), Ascending)
            case _ => SortOrder(UnresolvedAttribute(s"_c$index"), Ascending)
          }}

        case _ =>
          sys.error("order by error. pls check.")
      }
    }

    val withOrder = Sort(orderSeq, true, withHaving)

    val limitNode = sqlnode.fetch // limit expression

    val withLimit = if (limitNode == null) withOrder else Limit(nodeToExpr(limitNode), withOrder)

    withLimit
  }
}
