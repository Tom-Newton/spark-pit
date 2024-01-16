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
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, SparkSession, Dataset, Encoder}
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import scala.collection.mutable.{ArrayBuffer, HashSet}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

// Copy of private object
object Dataset {
  val curId = new java.util.concurrent.atomic.AtomicLong()
  val DATASET_ID_KEY = "__dataset_id"
  val COL_POS_KEY = "__col_position"
  val DATASET_ID_TAG = TreeNodeTag[HashSet[Long]]("dataset_id")

  def apply[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] = {
    val dataset = new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
    // Eagerly bind the encoder so we verify that the encoder matches the underlying
    // schema. The user will get an error if this is not the case.
    // optimization: it is guaranteed that [[InternalRow]] can be converted to [[Row]] so
    // do not do this check in that case. this check can be expensive since it requires running
    // the whole [[Analyzer]] to resolve the deserializer
    if (dataset.exprEnc.clsTag.runtimeClass != classOf[Row]) {
      dataset.resolvedEnc
    }
    dataset
  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    sparkSession.withActive {
      val qe = sparkSession.sessionState.executePlan(logicalPlan)
      qe.assertAnalyzed()
      new Dataset[Row](qe, RowEncoder(qe.analyzed.schema))
  }

  /** A variant of ofRows that allows passing in a tracker so we can track query parsing time. */
  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan, tracker: QueryPlanningTracker)
    : DataFrame = sparkSession.withActive {
    val qe = new QueryExecution(sparkSession, logicalPlan, tracker)
    qe.assertAnalyzed()
    new Dataset[Row](qe, RowEncoder(qe.analyzed.schema))
  }
}

object EarlyStopSortMerge {
  private final val PIT_FUNCTION = (
      _: Column,
      _: Column,
      _: Long
  ) => false
  final val PIT_UDF_NAME = "PIT"
  final val pit: UserDefinedFunction = udf(PIT_FUNCTION).withName(PIT_UDF_NAME)

  def init(spark: SparkSession): Unit = {
    if (!spark.catalog.functionExists(PIT_UDF_NAME)) {
      spark.udf.register(PIT_UDF_NAME, pit)
    }
    if (!spark.experimental.extraStrategies.contains(CustomStrategy)) {
      spark.experimental.extraStrategies =
        spark.experimental.extraStrategies :+ CustomStrategy
    }
    if (!spark.experimental.contains(PITRule)) {
      spark.experimental.extraOptimizations =
        spark.experimental.extraOptimizations :+ PITRule
    }
  }

  // For the PySpark API
  def getPit: UserDefinedFunction = pit

  implicit class PITDataset[T](ds: Dataset[T]) {
    @inline private def withPlan(logicalPlan: LogicalPlan): DataFrame = {
      Dataset.ofRows(ds.sparkSession, logicalPlan)
    }

    def pitJoin(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = {
      val plan = Dataset.ofRows(sparkSession, PITJoin(logicalPlan, right.logicalPlan, JoinType(joinType), Some(joinExprs.expr)))
        .queryExecution.analyzed.asInstanceOf[PITJoin]
  
      // If auto self join alias is disabled, return the plan.
      if (!ds.sparkSession.sessionState.conf.dataFrameSelfJoinAutoResolveAmbiguity) {
        return ds.withPlan(plan)
      }
  
      // If left/right have no output set intersection, return the plan.
      val lanalyzed = ds.queryExecution.analyzed
      val ranalyzed = right.queryExecution.analyzed
      if (lanalyzed.outputSet.intersect(ranalyzed.outputSet).isEmpty) {
        return withPlan(plan)
      }
  
      // Otherwise, find the trivially true predicates and automatically resolves them to both sides.
      // By the time we get here, since we have already run analysis, all attributes should've been
      // resolved and become AttributeReference.
  
      withPlan {
        resolveSelfJoinCondition(plan)
      }
    }
  }
}
