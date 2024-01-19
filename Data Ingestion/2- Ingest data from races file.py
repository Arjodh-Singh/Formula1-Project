# Databricks notebook source
# MAGIC %run ../Include/Config

# COMMAND ----------

# MAGIC %run ../Include/Common_Functions

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

races_df=spark.read\
    .option("header",True)\
    .option("infer_schema",True)\
    .csv(f"{bronze_path_folder}/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType

# COMMAND ----------

races_schema=StructType([StructField("raceId",IntegerType(),False),
                                   StructField("year",IntegerType(),False),
                                   StructField("round",IntegerType(),False),
                                   StructField("circuitId",IntegerType(),False),
                                   StructField("name",StringType(),False),
                                   StructField("date",DateType(),False),
                                   StructField("time",StringType(),False),
                                   StructField("url",StringType(),False)])

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df=spark.read\
    .option("header",True)\
    .schema(races_schema)\
    .csv(f"{bronze_path_folder}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

races_df_timestamp=races_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_df_timestamp=ingestion_date(races_df_timestamp)

# COMMAND ----------

display(races_df_timestamp)

# COMMAND ----------

races_selected_df=races_df_timestamp.select(col("raceId").alias("race_id"),col("year").alias("race_year"),
                                            col("round"),col("circuitId").alias("circuit_Id"),col("name"),
                                            col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode("overwrite").parquet(f'{silver_path_folder}/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/races

# COMMAND ----------

display(spark.read.parquet(f"{silver_path_folder}/races"))
