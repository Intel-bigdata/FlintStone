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
package com.intel.ssg.bdt.spark.sql.catalyst.expressions

import java.util.regex.Pattern

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that trims the given character(s) (by default space) from both ends for given string.
 */
case class FlintStringTrim(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  def this(token: Expression) = {
    this(token, Literal(" "))
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(token: Any, ch: Any): Any =
    UTF8StringHelper.trim(token.asInstanceOf[UTF8String], ch.asInstanceOf[UTF8String])

  override def prettyName: String = "trim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val utf8Helper = classOf[UTF8StringHelper].getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (t, c) => s"$utf8Helper.trim($t, $c)")
  }
}

/**
 * A function that trims the given character(s) (by default space) from left end for given string.
 */
case class FlintStringTrimLeft(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  def this(token: Expression) = {
    this(token, Literal(" "))
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(token: Any, ch: Any): Any =
    UTF8StringHelper.trimLeft(token.asInstanceOf[UTF8String], ch.asInstanceOf[UTF8String])

  override def prettyName: String = "ltrim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val utf8Helper = classOf[UTF8StringHelper].getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (t, c) => s"$utf8Helper.trimLeft($t, $c)")
  }
}

/**
 * A function that trims the given character(s) (by default space) from right end for given string.
 */
case class FlintStringTrimRight(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  def this(token: Expression) = {
    this(token, Literal(" "))
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  protected override def nullSafeEval(token: Any, ch: Any): Any =
    UTF8StringHelper.trimRight(token.asInstanceOf[UTF8String], ch.asInstanceOf[UTF8String])

  override def prettyName: String = "rtrim"

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val utf8Helper = classOf[UTF8StringHelper].getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (t, c) => s"$utf8Helper.trimRight($t, $c)")
  }
}

/**
 * Simple RegEx pattern matching function
 */
case class FlintLike(str: Expression, like: Expression, esc: Expression)
  extends Expression with CodegenFallback with ImplicitCastInputTypes {
  def this(str: Expression, like: Expression) = {
    this(str, like, Literal.create('\\'.toByte, ByteType))
  }
  override def children: Seq[Expression] = Seq(str, like, esc)
  override def foldable: Boolean = children.forall(_.foldable)
  override def nullable: Boolean = children.exists(_.nullable)

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  def escape(v: String, esc: String): String = escapeLikeRegex(v, esc)

  def escapeLikeRegex(v: String, esc: String): String = {
    assert(esc.length == 1)
    val ec: Char = esc.charAt(0)
    if (!v.isEmpty) {
      "(?s)" + (' ' +: v.init).zip(v).flatMap {
        case (prev, c) if c == ec => ""
        case (prev, c) if prev == ec =>
          c match {
            case '_' => "_"
            case '%' => "%"
            case _ => Pattern.quote("\\" + c)
          }
        case (prev, c) =>
          c match {
            case '_' => "."
            case '%' => ".*"
            case _ => Pattern.quote(Character.toString(c))
          }
      }.mkString
    } else {
      v
    }
  }

  def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()

  // try cache the pattern for Literal
  private lazy val cache: Pattern = (like, esc) match {
    case (Literal(value1: String, StringType), Literal(value2: String, StringType)) =>
      compile(value1, value2)
    case _ => null
  }

  protected def compile(str: String, esc: String): Pattern = if (str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str, esc))
  }

  protected def pattern(str: String, esc: String) = if (cache == null) compile(str, esc) else cache

  override def eval(input: InternalRow): Any = {
    val value1 = str.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = like.eval(input)
      if (value2 == null) {
        null
      } else {
        val value3 = esc.eval(input)
        if (value3 == null) {
          null
        } else {
          nullSafeEval(value1, value2, value3)
        }
      }
    }
  }

  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    val regex =
      pattern(input2.asInstanceOf[UTF8String].toString, input3.asInstanceOf[UTF8String].toString)
    if(regex == null) {
      null
    } else {
      matches(regex, input1.asInstanceOf[UTF8String].toString)
    }
  }

  override def toString: String = s"$str LIKE $like ESCAPE $esc"
}
