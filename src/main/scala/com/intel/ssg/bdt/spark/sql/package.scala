/**
 * Created by zhangc1 on 8/26/2015.
 */
package object parser {
  //sqlcall type
  val SELECT = "SELECT"
  val ORDER_BY = "ORDER_BY"
  val JOIN = "JOIN"

  //join type
  val COMMA_JOIN = "COMMA"
  val INNER_JOIN = "INNER"
  val LEFT_JOIN = "LEFT"
  val RIGHT_JOIN = "RIGHT"
  val FULL = "FULL"
  val USING = "USING"

  //join condition type
  val ON = "ON"

  //sqlcall
  val UNION = "UNION"
  val INTERSECT = "INTERSECT"
  val EXCEPT = "EXCEPT"

  //isnert
  val INSERT = "INSERT"

  //sqlbasiccall
  val WITH = "WITH"
  val AS = "AS"

  val TIMES = "TIMES"
  val DIVIDE = "DIVIDE"
  val PLUS = "PLUS"
  val MINUS = "MINUS"

  val EQUALS = "EQUALS"
  val LESS_THAN = "LESS_THAN"
  val LESS_THAN_OR_EQUAL = "LESS_THAN_OR_EQUAL"
  val GREATER_THAN = "GREATER_THAN"
  val GREATER_THAN_OR_EQUAL = "GREATER_THAN_OR_EQUAL"
  val NOT_EQUALS = "NOT_EQUALS"
  val BETWEEN = "BETWEEN"

  //val OVER = "OVER"
  //val FILTER = "FILTER"
  val OR = "OR"
  val AND = "AND"
  val OTHER = "OTHER" // like ||
  val OTHER_OR = "||"
  val LIKE = "LIKE"
  val IN = "IN"
  val IS_NULL = "IS_NULL"
  val NOT = "NOT"
  val IS_NOT_NULL = "IS_NOT_NULL"
  val NULLIF = "NULLIF"

  val CASE = "CASE"
  val CAST = "CAST"
  val STRING = "STRING"
  val TIMESTAMP = "TIMESTAMP"
  val DOUBLE = "DOUBLE"
  //val FIXEDDECIMALTYPE = "FIXEDDECIMALTYPE"
  val DECIMAL = "DECIMAL"
  val DATE = "DATE "
  val INTEGER = "INTEGER"
  val BOOL = "BOOL"
  val CHAR = "CHAR"
  val VARCHAR = "VARCHAR"
  val LONG = "LONG"
  val BINARY = "BINARY"

  val DISTINCT = "DISTINCT"
  val PLUS_PREFIX = "PLUS_PREFIX"
  val MINUS_PREFIX = "MINUS_PREFIX"
  val OTHER_FUNCTION = "OTHER_FUNCTION"
  val ITEM_FUNCTION = "ITEM"
  //val ROW_TYPE = "ROW"

  val SUM = "SUM"
  val COUNT = "COUNT"
  val FIRST = "FIRST"
  val LAST = "LAST"
  val AVG = "AVG"
  val MIN = "MIN"
  val MAX = "MAX"
  val UPPER = "UPPER"
  val LOWER = "LOWER"
  val IF = "IF"
  val SUBSTR = "SUBSTR"
  val SUBSTRING = "SUBSTRING"
  val COALESCE = "COALESCE"
  val SQRT = "SQRT"
  val ABS = "ABS"
  val EXTRACT = "EXTRACT"

  //sqlnode type
  val LITERAL = "LITERAL"
  val IDENTIFIER = "IDENTIFIER"

  val YEAR = "YEAR"
  val MONTH = "MONTH"
  val DAY = "DAY"
  val HOUR = "HOUR"
  val MINUTE = "MINUTE"
  val SECOND = "SECOND"

  val DESCENDING = "DESCENDING"
}
