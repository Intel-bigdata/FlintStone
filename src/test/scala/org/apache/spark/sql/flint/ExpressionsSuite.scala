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

import com.intel.ssg.bdt.spark.sql.catalyst.expressions._
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Literal, ExpressionEvalHelper}
import org.apache.spark.sql.types._

class ExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("TRIM/LTRIM/RTRIM") {
    val s = 'a.string.at(0)
    checkEvaluation(FlintStringTrim(Literal("\taa\t\t"), Literal("\t")), "aa", create_row("\tabdef\t"))
    checkEvaluation(FlintStringTrim(s, Literal("x")), "abdef", create_row("xabdefx"))

    checkEvaluation(FlintStringTrimLeft(Literal("laall"), Literal("l")), "aall", create_row("labdefl"))
    checkEvaluation(FlintStringTrimLeft(s, Literal("\t")), "abdef\t", create_row("\tabdef\t"))

    checkEvaluation(FlintStringTrimRight(Literal("haahh"), Literal("h")), "haa", create_row("habdefh"))
    checkEvaluation(FlintStringTrimRight(s, Literal("h")), "habdef", create_row("habdefh"))

    // TODO add scalastye and turn it off for non-Ascii
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    checkEvaluation(
      FlintStringTrimRight(s, Literal("ok")), "okoko花花世界k", create_row("okoko花花世界kokok"))
    checkEvaluation(FlintStringTrimLeft(s, Literal("c")), "花花世界c", create_row("cc花花世界c"))
    checkEvaluation(FlintStringTrim(s, Literal("mm")), "m花花世界", create_row("mmm花花世界mm"))

    checkEvaluation(
      FlintStringTrim(Literal.create(null, StringType), Literal.create(null, StringType)), null)
    checkEvaluation(FlintStringTrimLeft(Literal.create(null, StringType), Literal("abc")), null)
    checkEvaluation(FlintStringTrimRight(Literal("abcde"), Literal.create(null, StringType)), null)
  }
}
