package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{ColumnNodeToExpressionConverter, Dataset => ClassicDataset}

object SparkBridge {
  def ofRows(session: SparkSession, plan: LogicalPlan): DataFrame =
    ClassicDataset.ofRows(session.asInstanceOf[classic.SparkSession], plan)

  def columnToExpression(col: Column): Expression =
    ColumnNodeToExpressionConverter(col.node)
}
