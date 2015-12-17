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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
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