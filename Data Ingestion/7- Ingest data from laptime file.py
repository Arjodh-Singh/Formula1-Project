# Databricks notebook source
lap_times_df=spark.read\
    .csv("/mnt/devarjsa/bronze/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType,FloatType

# COMMAND ----------

lap_times_schema=StructType([StructField("raceId",IntegerType(),False),
                             StructField("driverId",IntegerType(),False),
                            StructField("position",StringType(),False),
                            StructField("lap",IntegerType(),False),
                            StructField("time",StringType(),False),
                            StructField("milliseconds",IntegerType(),False) ])

# COMMAND ----------

lap_times_df_schema=spark.read\
    .schema(lap_times_schema)\
    .csv("/mnt/devarjsa/bronze/lap_times")

# COMMAND ----------

display(lap_times_df_schema)

# COMMAND ----------

lap_times_df_schema.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

lap_times_df_final=lap_times_df_schema.withColumn("ingestion_date",current_timestamp())\
                    .withColumnRenamed("raceId","race_id")\
                    .withColumnRenamed("driverId","driver_id")
                     

# COMMAND ----------

display(lap_times_df_final)

# COMMAND ----------

lap_times_df_final.write.mode("overwrite").parquet('/mnt/devarjsa/silver/lap_times')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/lap_times

# COMMAND ----------

display(spark.read.parquet("/mnt/devarjsa/silver/lap_times"))

# COMMAND ----------


