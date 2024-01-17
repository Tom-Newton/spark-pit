/*
 * MIT License
 *
 * Copyright (c) 2022 Axel Pettersson
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.github.ackuq.pit

import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, JoinType}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

import execution.CustomStrategy
import logical.PITJoin

object EarlyStopSortMerge {
  implicit class applyPITJoin(df: DataFrame) {
    def pitJoin(
        right: DataFrame,
        leftPitExpression: Column,
        rightPitExpression: Column,
        joinType: String,
        tolerance: Long
    ): DataFrame = pitJoin(
      right,
      leftPitExpression,
      rightPitExpression,
      None,
      joinType,
      tolerance,
    )

    def pitJoin(
        right: DataFrame,
        leftPitExpression: Column,
        rightPitExpression: Column,
        joinExprs: Column,
        joinType: String,
        tolerance: Long
    ): DataFrame = pitJoin(
      right,
      leftPitExpression,
      rightPitExpression,
      Some(joinExprs),
      joinType,
      tolerance,
    )

    def pitJoin(
        right: DataFrame,
        leftPitExpression: Column,
        rightPitExpression: Column,
        joinExprs: Option[Column],
        joinType: String,
        tolerance: Long
    ): DataFrame = {

      val parsedJoinType = JoinType(joinType)
      parsedJoinType match {
        case LeftOuter | Inner => ()
        case x =>
          throw new IllegalArgumentException(
            s"Join type $x not supported for PIT joins"
          )
      }

      val logicalPlan = PITJoin(
        df.queryExecution.analyzed,
        right.queryExecution.analyzed,
        leftPitExpression.expr,
        rightPitExpression.expr,
        parsedJoinType == LeftOuter,
        tolerance,
        joinExprs.map(_.expr)
      )
      new DataFrame(
        df.sparkSession,
        logicalPlan,
        ExpressionEncoder(logicalPlan.schema)
      )
    }
  }
}

class YourExtensions extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // extensions.injectResolutionRule { session => }
    // extensions.injectFunction()
    // extensions.injectPostHocResolutionRule(session => PITRule)
    extensions.injectPlannerStrategy(session => CustomStrategy)
    // extensions.injectResolutionRule(session => PITRule)
  }
}
