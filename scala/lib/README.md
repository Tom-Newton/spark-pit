These jars are built from a hacked version of Spark 3.3.2. Importantly `CodegenSupport.needCopyResult` has been changed
to match Databricks 12.2. That includes changing the return type to String and adding the argument ctx: CodegenContext.

Databricks is so much harder to maintain than open source spark running on kubernetes.... 
