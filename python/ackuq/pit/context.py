#
# MIT License
#
# Copyright (c) 2022 Axel Pettersson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

from typing import List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession


class PITContext:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._jvm = spark.sparkContext._jvm

    def _to_scala_seq(self, lst):
        return self._jvm.org.apache.spark.api.python.PythonUtils.toSeq(lst)

    def _to_scala_option(self, value):
        if value is None:
            return self._jvm.scala.Option.empty()
        return self._jvm.scala.Some(value)

    def union(
        self,
        left: DataFrame,
        right: DataFrame,
        left_ts_column: str = "ts",
        right_ts_column: str = "ts",
        left_prefix: Optional[str] = None,
        right_prefix: str = "",
        partition_cols: Optional[List[str]] = None,
    ) -> DataFrame:
        jdf = self._jvm.io.github.ackuq.pit.Union.join(
            left._jdf,
            right._jdf,
            left_ts_column,
            right_ts_column,
            self._to_scala_option(left_prefix),
            right_prefix,
            self._to_scala_seq(partition_cols or []),
        )
        return DataFrame(jdf, self.spark)

    def exploding(
        self,
        left: DataFrame,
        right: DataFrame,
        left_ts_column: Column,
        right_ts_column: Column,
        partition_cols: Optional[Sequence[Tuple[Column, Column]]] = None,
    ) -> DataFrame:
        scala_tuples = [
            self._jvm.scala.Tuple2(left_col._jc, right_col._jc)
            for left_col, right_col in (partition_cols or [])
        ]
        jdf = self._jvm.io.github.ackuq.pit.Exploding.join(
            left._jdf,
            right._jdf,
            left_ts_column._jc,
            right_ts_column._jc,
            self._to_scala_seq(scala_tuples),
        )
        return DataFrame(jdf, self.spark)
