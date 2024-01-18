# Databricks notebook source
# MAGIC %run ../Include/Config

# COMMAND ----------

race_results_df2=spark.read.parquet(f"{gold_path_folder}/races_results")

# COMMAND ----------

display(race_results_df2)

# COMMAND ----------

from pyspark.sql.functions import count,col,sum,when

# COMMAND ----------

drivers_standing=race_results_df2.groupBy("race_year","driver_name","driver_nationality","team")\
                .agg(sum("points").alias("total_points"),
                     count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(drivers_standing)

# COMMAND ----------

from pyspark.sql.functions import rank,desc,asc

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

driver_rank_spec=Window.partitionBy("race_year").orderBy(drivers_standing.total_points.desc(),drivers_standing.wins.desc())
final_df=drivers_standing.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter(final_df.race_year==2020))

# COMMAND ----------

final_df.write.parquet(f"{gold_path_folder}/drivers_standings")

# COMMAND ----------

display(spark.read.parquet(f"{gold_path_folder}/drivers_standings"))

# COMMAND ----------


