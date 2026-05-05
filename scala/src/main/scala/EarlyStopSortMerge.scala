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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.SparkSessionExtensionsProvider
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.types.NumericType
import execution.CustomStrategy
import logical.PITJoin


object EarlyStopSortMerge {
  def joinPIT(
      left: DataFrame,
      right: DataFrame,
      leftPitColumn: Column,
      rightPitColumn: Column,
      joinType: String,
      tolerance: Long
  ): DataFrame = joinPIT(
    left,
    right,
    leftPitColumn,
    rightPitColumn,
    None,
    joinType,
    tolerance
  )

  def joinPIT(
      left: DataFrame,
      right: DataFrame,
      leftPitColumn: Column,
      rightPitColumn: Column,
      joinExprs: Column,
      joinType: String,
      tolerance: Long
  ): DataFrame = joinPIT(
    left,
    right,
    leftPitColumn,
    rightPitColumn,
    Some(joinExprs),
    joinType,
    tolerance
  )

  def joinPIT(
      left: DataFrame,
      right: DataFrame,
      leftPitColumn: Column,
      rightPitColumn: Column,
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


    val sparkSession = left.sparkSession
    def toExpression(column: Column) = sparkSession.expression(column)

    val leftPitExpression = toExpression(leftPitColumn)
    val rightPitExpression = toExpression(rightPitColumn)

    Seq("left" -> leftPitExpression.dataType, "right" -> rightPitExpression.dataType).foreach {
      case (side, dataType) =>
        if (!dataType.isInstanceOf[NumericType]) {
          throw new AnalysisException(
            message = s"PIT key on $side side must be a numeric type, got $dataType",
            line = None,
            startPosition = None,
            cause = None,
            errorClass = None,
            messageParameters = Map.empty,
            context = Array.empty
          )
        }
    }

    val logicalPlan = PITJoin(
      left.queryExecution.analyzed,
      right.queryExecution.analyzed,
      leftPitExpression,
      rightPitExpression,
      parsedJoinType == LeftOuter,
      tolerance,
      joinExprs.map(toExpression(_))
    )
    // Copying `Dataset.ofRows()`, but using a public constructor for DataFrame (Dataset[Row]).
    new DataFrame(
      sparkSession,
      logicalPlan,
      RowEncoder.encoderFor(logicalPlan.schema)
    )
  }
}

class SparkPIT extends SparkSessionExtensionsProvider {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(session => CustomStrategy)
  }
}
