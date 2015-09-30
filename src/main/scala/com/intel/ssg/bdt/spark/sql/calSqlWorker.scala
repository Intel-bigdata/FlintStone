package parser

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlCase

import scala.language.implicitConversions

import org.apache.calcite.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

//support select and insert both
class calSqlWorker(sqlNode: SqlNode){
  //val calparser: calParser = new calParser()
  //val sqlNode: SqlNode = calparser.getSqlNode(input)

  def getLogicalPlan(): LogicalPlan = {
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
          withList += Tuple2(Item.name.getSimple, Subquery(Item.name.getSimple, nodeToPlan(Item.query)))
        }
        With(bodyPlan, withList.toMap)

      case AS => //becasue it's the as type so that there must be some node in right node
        val as_sqlnode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = as_sqlnode.getOperandList.get(0)
        val rightNode = as_sqlnode.getOperandList.get(1).asInstanceOf[SqlIdentifier]

        if (rightNode.isSimple) {
          leftNode.getKind.name() match {
            case IDENTIFIER =>
              UnresolvedRelation(leftNode.asInstanceOf[SqlIdentifier].names, if (rightNode == null) None else Some(rightNode.getSimple))
            case _ =>
              Subquery(rightNode.getSimple, nodeToPlan(leftNode))
          }
        }else
          sys.error("wrong \'AS\' input.")

      case ORDER_BY =>
        dealWithOrderByNode(subSqlNode.asInstanceOf[SqlOrderBy])

      case UNION =>
        val basicallnode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftnode = basicallnode.getOperandList.get(0)
        val rightnode = basicallnode.getOperandList.get(1)
        Union(nodeToPlan(leftnode), nodeToPlan(rightnode))

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
        InsertIntoTable(nodeToPlan(tableDest), Map.empty[String, Option[String]], nodeToPlan(subSelectNode), false, false)

      case _ =>
        sys.error("TODO")
    }
  }

  //sqlbasiccall && literal && identifier
  def nodeToExpr(subSqlNode: SqlNode) : Expression = {
    val nodeType = subSqlNode.getKind.name()

    nodeType match {
      case AS =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val leftNode = nodeToExpr(basicCallNode.getOperandList.get(0))//expr
        val rightNode = basicCallNode.getOperandList.get(1).asInstanceOf[SqlIdentifier]//ident

        if (rightNode.isSimple)
          Alias(leftNode, rightNode.getSimple)()
        else
          sys.error("wrong as input.")

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

          val andExpr = And(GreaterThanOrEqual(nodeToExpr(X), nodeToExpr(Y)), LessThanOrEqual(nodeToExpr(X), nodeToExpr(Z)))

          if (operator.getName.equals("BETWEEN")){
            andExpr
          }else{
            Not(andExpr)
          }
        }else{
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

      case LIKE =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator

        val likeExpr = Like(nodeToExpr(basicCallNode.getOperandList.get(0)), nodeToExpr(basicCallNode.getOperandList.get(1)))
        if (operator.getName.equals("LIKE"))
          likeExpr
        else
          Not(likeExpr)

      case IN =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator

        val leftNode = basicCallNode.getOperandList.get(0)

        if (basicCallNode.getOperandList.get(1).getKind.name().equals(SELECT)){
          sys.error("TODO")
        }else{
          val rightNodeList = basicCallNode.getOperandList.get(1).asInstanceOf[SqlNodeList]//must be a list
          val rightSeq = rightNodeList.map(nodeToExpr)

          val inExpr = In(nodeToExpr(leftNode), rightSeq.toSeq)
          if (operator.getName.equals("IN"))
            inExpr
          else
            Not(inExpr)
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
            if (elseLiter.toValue == null)
              whenSeq
            else
              whenSeq += nodeToExpr(elseLiter)

          CaseWhen(branchs)
        }else
          sys.error("wrong input, check \"CASE WHEN\" usage.")

      case CAST =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        //val operator = basicCallNode.getOperator
        val left = basicCallNode.getOperandList.get(0)
        val right = basicCallNode.getOperandList.get(1)

        val datatype =
          right.asInstanceOf[SqlDataTypeSpec].getTypeName.asInstanceOf[SqlIdentifier].getSimple match {
            case STRING => StringType
            case TIMESTAMP => TimestampType
            case DOUBLE => DoubleType
            //case FIXEDDECIMALTYPE =>
            case DECIMAL => DecimalType.Unlimited
            case DATE => DateType
            case INTEGER => IntegerType
            case BOOL => BooleanType
            case _ => sys.error("TODO, other datatype may not be valided for calcite or spark now.")
          }

        Cast(nodeToExpr(left), datatype)

      case MINUS_PREFIX =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator
        val operand = basicCallNode.getOperandList.get(0)
        if (operator.getName equals "-")
          UnaryMinus(nodeToExpr(operand))
        else
          nodeToExpr(operand)

      case OTHER_FUNCTION =>
        val basicCallNode = subSqlNode.asInstanceOf[SqlBasicCall]
        val operator = basicCallNode.getOperator
        val operand = basicCallNode.getOperandList

        operator.getName match {
          case ITEM_FUNCTION =>
            UnresolvedExtractValue(nodeToExpr(operand.get(0)), nodeToExpr(operand.get(1)))

          case SUM =>
            if (basicCallNode.getFunctionQuantifier == null)
              Sum(nodeToExpr(operand.get(0)))
            else
              SumDistinct(nodeToExpr(operand.get(0)))

          case COUNT =>
            //count(*), count(a), count(distinct a, b)
            if (basicCallNode.getFunctionQuantifier == null){
              val distinctExpr = operand.get(0).asInstanceOf[SqlIdentifier]
              if (distinctExpr.isStar && distinctExpr.names.size() == 1)
                Count(Literal(1))
              //                                if (distinctExpr.names.size() == 1)
              //                                    Count(Literal(1))
              //                                else
              //                                    sys.error("TODO")
              else
                Count(nodeToExpr(distinctExpr))
            }else {
              var distinctSeq = operand.map(nodeToExpr)
              CountDistinct(distinctSeq)
            }

          case FIRST =>
            First(nodeToExpr(operand.get(0)))

          case LAST =>
            Last(nodeToExpr(operand.get(0)))

          case AVG =>
            Average(nodeToExpr(operand.get(0)))

          case MIN =>
            Min(nodeToExpr(operand.get(0)))

          case MAX =>
            Max(nodeToExpr(operand.get(0)))

          case UPPER =>
            Upper(nodeToExpr(operand.get(0)))

          case LOWER =>
            Lower(nodeToExpr(operand.get(0)))

          case IF =>
            if (operand.size() == 3)
              If(nodeToExpr(operand.get(0)), nodeToExpr(operand.get(1)), nodeToExpr(operand.get(2)))
            else
              sys.error("YOUR INPUT IS INVALID, PLEASE CHECK.")

          case SUBSTR | SUBSTRING =>
            if (operand.size() == 2)
              Substring(nodeToExpr(operand.get(0)), nodeToExpr(operand.get(1)), Literal(Integer.MAX_VALUE))
            else if(operand.size() == 3)
              Substring(nodeToExpr(operand.get(0)), nodeToExpr(operand.get(1)), nodeToExpr(operand.get(2)))
            else
              sys.error("YOUR INPUT IS INVALID, PLEASE CHECK.")

          case COALESCE =>
            if (operand.size() > 0)
              Coalesce(operand.map(nodeToExpr))
            else
              sys.error("YOUR INPUT IS NOT SUITABLE, PLEASE CHECK.")

          case SQRT =>
            Sqrt(nodeToExpr(operand.get(0)))

          case ABS =>
            Abs(nodeToExpr(operand.get(0)))

          case _ =>
            println("user-defined function.")
            //here maybe distinct
            UnresolvedFunction(operator.getName, operand.map(nodeToExpr), isDistinct = true)
          /*case ROW_TYPE =>
              nodeToLiteral()*/
        }

      case IDENTIFIER =>
        val identiNode = subSqlNode.asInstanceOf[SqlIdentifier]
        if (identiNode.isStar)
          if (identiNode.names.size() == 1)
            UnresolvedStar(None)
          else if (identiNode.names.size() == 2)
            UnresolvedStar(Option(identiNode.names.get(0)))
          else
            sys.error("wrong select input.")
        else{
          if (identiNode.isSimple)
            UnresolvedAttribute.quoted(identiNode.getSimple)
//          else if (identiNode.names.size() == 2)
//            UnresolvedExtractValue(UnresolvedAttribute.quoted(identiNode.names.get(0)), Literal(identiNode.names.get(1)))
//            UnresolvedAttribute(identiNode.names.mkString("."))
          else
            UnresolvedAttribute(identiNode.names.mkString("."))
        }

      case _ => nodeToLiteral(subSqlNode)
    }
  }

  //literal
  def nodeToLiteral(literalSqlNode : SqlNode) : Expression = {
    val literalNode = literalSqlNode.asInstanceOf[SqlLiteral]
    literalNode.getTypeName match {
      case SqlTypeName.NULL => Literal.create(null, NullType)

      case SqlTypeName.BOOLEAN =>
        if (literalNode.booleanValue())
          Literal.create(true, BooleanType)
        else
          Literal.create(false, BooleanType)

      case SqlTypeName.DECIMAL | SqlTypeName.DOUBLE =>
        val scalavalue = scala.math.BigDecimal(literalNode.getValue.asInstanceOf[java.math.BigDecimal])
        val tempDecimal = scalavalue match {
          case v if scalavalue.isValidInt => v.toIntExact
          case v if scalavalue.isValidLong => v.toLongExact
          case v => v.underlying()
        }
        Literal(tempDecimal)

      case SqlTypeName.CHAR =>
        Literal.create(literalNode.getStringValue, StringType)
      //            case SqlTypeName.DATE => {}
      //            case SqlTypeName.BINARY => {}
      //            case SqlTypeName.TIME => {}

      case _ =>
        sys.error("TODO")
    }
  }//ok

  def  dealWithSelectNode(selectNode: SqlSelect) : LogicalPlan = {
    val withHaving = prepareSelect(selectNode)

    val limitNode = selectNode.getFetch//limit expression

    val withLimit =
      if (limitNode == null)
        withHaving
      else
        Limit(nodeToExpr(limitNode), withHaving)

    withLimit
  }

  def prepareSelect(selectNode: SqlSelect): LogicalPlan ={
    val isDistinct = selectNode.isDistinct//distinct
    val selectNodeList = selectNode.getSelectList// select projection(as) : SqlNodeList
    val fromNode = selectNode.getFrom// from relations
    val whereNode = selectNode.getWhere// where expression
    val groupByNodeList = selectNode.getGroup// group by expression
    val havingNode = selectNode.getHaving// having expresssion

    val base = nodeToPlan(fromNode)

    val withFilter =
      if (whereNode == null)
        base
      else
        Filter(nodeToExpr(whereNode), base)

    val withProjection =
      if (groupByNodeList == null)
        Project(assignAliases(selectNodeList.map(nodeToExpr).toSeq), withFilter)
      else{
        val list = groupByNodeList.getList.map(nodeToExpr)
        Aggregate(list, assignAliases(selectNodeList.map(nodeToExpr).toSeq), withFilter)
      }

    val withDistinct =
      if (isDistinct)
        Distinct(withProjection)
      else
        withProjection

    val withHavnig =
      if (havingNode == null)
        withDistinct
      else
        Filter(nodeToExpr(havingNode), withDistinct)

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
    val condition = sqlnode.getCondition

    val jt =
      joinType.name() match {
        case COMMA_JOIN => Inner
        case INNER_JOIN => Inner
        case LEFT_JOIN => LeftOuter
        case RIGHT_JOIN => RightOuter
        case FULL => FullOuter
      }

    Join(nodeToPlan(left), nodeToPlan(right), jt, if (condition != null) Some(nodeToExpr(condition)) else None)
  }

  def  dealWithOrderByNode(sqlnode: SqlOrderBy) : LogicalPlan = {
    //especially fetch to get limit
    val query = sqlnode.query.asInstanceOf[SqlSelect]
    val orderList = sqlnode.orderList

    val withHaving = prepareSelect(query)

    val orderSeq = ListBuffer[SortOrder]()
    for (ele <- orderList) {
      ele.getKind.name() match {
        case DESCENDING =>
          val descNode = ele.asInstanceOf[SqlBasicCall]
          orderSeq += SortOrder(nodeToExpr(descNode.getOperandList.get(0)), Descending)

        case IDENTIFIER =>
          //val IdenNode = ele.asInstanceOf[SqlIdentifier]
          orderSeq += SortOrder(nodeToExpr(ele), Ascending)
      }
    }

    val withOrder = Sort(orderSeq, true, withHaving)

    val limitNode = sqlnode.fetch//limit expression

    val withLimit =
      if (limitNode == null)
        withOrder
      else
        Limit(nodeToExpr(limitNode), withOrder)

    withLimit
  }
}
