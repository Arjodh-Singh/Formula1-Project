# Databricks notebook source
# MAGIC %run ../Include/Config

# COMMAND ----------

# MAGIC %run ../Include/Common_Functions

# COMMAND ----------

results_df=spark.read\
    .json(f"{bronze_path_folder}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType,FloatType

# COMMAND ----------

results_schema=StructType([StructField("constructorId",IntegerType(),False),
                                   StructField("driverId",IntegerType(),False),
                                   StructField("fastestLap",IntegerType(),False),
                                   StructField("fastestLapSpeed",FloatType(),False),
                                   StructField("fastestLapTime",StringType(),False),
                                   StructField("grid",IntegerType(),False),
                                   StructField("laps",IntegerType(),False),
                                   StructField("milliseconds",IntegerType(),False),
                                   StructField("number",IntegerType(),False),
                                   StructField("points",FloatType(),False),
                                   StructField("position",IntegerType(),False),
                                   StructField("positionOrder",StringType(),False),
                                   StructField("positionText",StringType(),False),
                                   StructField("raceId",IntegerType(),False),
                                   StructField("rank",IntegerType(),False),
                                   StructField("resultId",IntegerType(),False),
                                   StructField("statusId",StringType(),False),
                                   StructField("time",StringType(),False)])

# COMMAND ----------

results_df_schema=spark.read\
    .schema(results_schema)\
    .json(f"{bronze_path_folder}/results.json")

# COMMAND ----------

results_df_schema.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

results_df_selected=results_df_schema.withColumnRenamed("constructorId","constructor_id")\
                    .withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("fastestLap","fastest_lap")\
                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                    .withColumnRenamed("positionOrder","position_order")\
                    .withColumnRenamed("positionText","position_text")\
                     .withColumnRenamed("raceId","race_id")\
                    .withColumnRenamed("resultId","result_id")\
                    .withColumnRenamed("statusId","status_id") 

# COMMAND ----------

results_df_selected=ingestion_date(results_df_selected)

# COMMAND ----------

results_df_final=results_df_selected.drop(col("status_id"))

# COMMAND ----------

display(results_df_final)

# COMMAND ----------

results_df_final.write.mode("overwrite").partitionBy("race_id").parquet(f'{silver_path_folder}/results')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/results

# COMMAND ----------

display(spark.read.parquet(f"{silver_path_folder}/results"))

# COMMAND ----------


