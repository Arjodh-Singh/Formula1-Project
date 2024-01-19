# Databricks notebook source
# MAGIC %run ./Include/Config

# COMMAND ----------

delta_temp=spark.read.parquet(f'{gold_path_folder}/drivers_standings')

# COMMAND ----------

display(delta_temp)

# COMMAND ----------

delta_temp.write.format('delta').save(f'{delta_path_folder}/delta_table')

# COMMAND ----------

display(spark.read.format('delta').load(f'{delta_path_folder}/delta_table'))

# COMMAND ----------

# MAGIC %sql
# MAGIC create table temp 
# MAGIC using DELTA 
# MAGIC location '/mnt/devarjsa/delta/delta_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp;

# COMMAND ----------


