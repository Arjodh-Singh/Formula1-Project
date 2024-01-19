# Databricks notebook source
# MAGIC %run ../Include/Config

# COMMAND ----------

race_results_df2=spark.read.parquet(f"{gold_path_folder}/races_results")

# COMMAND ----------

display(race_results_df2)

# COMMAND ----------

from pyspark.sql.functions import count,col,sum,when

# COMMAND ----------

constructor_standing=race_results_df2.groupBy("race_year","team")\
                .agg(sum("points").alias("total_points"),
                     count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(constructor_standing)

# COMMAND ----------

from pyspark.sql.functions import rank,desc,asc

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

constructor_rank_spec=Window.partitionBy("race_year").orderBy(constructor_standing.total_points.desc(),constructor_standing.wins.desc())
final_df=constructor_standing.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter(final_df.race_year==2020))

# COMMAND ----------

final_df.write.parquet(f"{gold_path_folder}/constructor_standing")

# COMMAND ----------

display(spark.read.parquet(f"{gold_path_folder}/constructor_standing"))

# COMMAND ----------


