#!/bin/bash
# Build the jar on a Databricks cluster of the target runtime, against that runtime's own Spark and Scala jars.
# Usage: scala/build_databricks.sh <output jar>
set -euo pipefail
out_jar=$(realpath "$1")
cd "$(dirname "$0")"
rm -rf out
mkdir out
mapfile -t sources < <(find src/main/scala -name '*.scala')
java -Xmx8g -cp "/databricks/jars/*" scala.tools.nsc.Main -nowarn -classpath "/databricks/jars/*" -d out "${sources[@]}"
jar cf "$out_jar" -C out .
