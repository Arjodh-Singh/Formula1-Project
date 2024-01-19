# Databricks notebook source
# MAGIC %run ../Include/Config
# MAGIC

# COMMAND ----------

# MAGIC %run ../Include/Common_Functions

# COMMAND ----------

driver_df=spark.read.parquet(f"{silver_path_folder}/drivers")\
    .withColumnRenamed("number","driver_number")\
        .withColumnRenamed("name","driver_name")\
            .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df=spark.read.parquet(f"{silver_path_folder}/constructors")\
    .withColumnRenamed("name","team")

# COMMAND ----------

circuits_df=spark.read.parquet(f"{silver_path_folder}/circuits")\
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df=spark.read.parquet(f"{silver_path_folder}/races")\
    .withColumnRenamed("name","race_name")\
        .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df=spark.read.parquet(f"{silver_path_folder}/results")\
    .withColumnRenamed("time","race_time")

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df,races_df.circuit_Id==circuits_df.circuit_id)\
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

race_results_df=results_df.join(race_circuits_df,results_df.race_id==race_circuits_df.race_id)\
                .join(driver_df,results_df.driver_id==driver_df.driver_id)\
                    .join(constructors_df,results_df.constructor_id==constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

final_df=race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality",
                                "team","grid","fastest_lap","race_time","points","position")\
                                    .withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{gold_path_folder}/races_results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/devarjsa/gold

# COMMAND ----------

display(spark.read.parquet(f"{gold_path_folder}/races_results"))

# COMMAND ----------


