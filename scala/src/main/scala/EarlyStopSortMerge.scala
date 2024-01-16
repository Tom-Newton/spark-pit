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

import execution.CustomStrategy
import logical.PITRule
import logical.PITJoin

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.functions.udf
// import org.apache.spark.sql.{
//   functions,
//   Column,
//   SparkSession,
//   Dataset,
//   DataFrame,
//   Encoder,
//   SparkSessionExtensionsProvider,
//   SparkSessionExtensions
// }
import org.apache.spark.sql._

import scala.collection.mutable.{ArrayBuffer, HashSet}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.Row

object EarlyStopSortMerge {
  private final val PIT_FUNCTION = (
      _: Column,
      _: Column,
      _: Long
  ) => false
  final val PIT_UDF_NAME = "PIT"
  final val pit: UserDefinedFunction = udf(PIT_FUNCTION).withName(PIT_UDF_NAME)

  def init(spark: SparkSession): Unit = {
    // if (!spark.catalog.functionExists(PIT_UDF_NAME)) {
    //   spark.udf.register(PIT_UDF_NAME, pit)
    // }
    // if (!spark.experimental.extraStrategies.contains(CustomStrategy)) {
    //   spark.experimental.extraStrategies =
    //     spark.experimental.extraStrategies :+ CustomStrategy
    // }
    // if (!spark.experimental.extraOptimizations.contains(PITRule)) {
    //   spark.experimental.extraOptimizations =
    //     spark.experimental.extraOptimizations :+ PITRule
    // }
  }

  // For the PySpark API
  def getPit: UserDefinedFunction = pit

  // implicit class AlreadySortedWrapper(df: DataFrame) {
  //   def alreadySorted(columnNames: Seq[String]): DataFrame = {
  //     val columns = columnNames.map(col(_).expr.asInstanceOf[Attribute])

  //     val l = AlreadySorted(columns, df.queryExecution.analyzed)
  //     val e = RowEncoder(df.schema)

  //     new DataFrame(df.sparkSession, l, e)
  //   }
  // }

  implicit class applyPITJoin(df: DataFrame) {
    def pitJoin(
        right: DataFrame,
        pitCondition: Expression
    ): DataFrame = {
      // val leftColumns = leftColumnNames.map(
      //   col(_).expr.asInstanceOf[Attribute])
      // val rightColumns = rightColumnNames.map(
      //   col(_).expr.asInstanceOf[Attribute])

      val logicalPlan = PITJoin(
        df.queryExecution.analyzed,
        right.queryExecution.analyzed,
        pitCondition,
        false,
        0L,
        None,
      )
      // // df.sparkSession.withActive {
      //   val qe = df.sparkSession.sessionState.executePlan(logicalPlan)
      //   qe.assertAnalyzed()
        new DataFrame(df.sparkSession, logicalPlan, ExpressionEncoder(logicalPlan.schema))
      // }
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
