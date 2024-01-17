# Databricks notebook source
pitstop_df=spark.read\
    .option('multiLine',True)\
    .json("/mnt/devarjsa/bronze/pit_stops.json")

# COMMAND ----------

display(pitstop_df)

# COMMAND ----------

pitstop_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType,FloatType

# COMMAND ----------

pitstop_schema=StructType([StructField("driverId",IntegerType(),False),
                                   StructField("duration",StringType(),False),
                                   StructField("lap",IntegerType(),False),
                                   StructField("milliseconds",FloatType(),False),
                                   StructField("raceId",IntegerType(),False),
                                   StructField("stop",StringType(),False),
                                   StructField("time",StringType(),False)
                                   ])

# COMMAND ----------

pitstop_df_schema=spark.read\
    .option('multiLine',True)\
    .schema(pitstop_schema)\
    .json("/mnt/devarjsa/bronze/pit_stops.json")

# COMMAND ----------

pitstop_df_schema.printSchema()

# COMMAND ----------

display(pitstop_df_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

pitstop_df_selected=pitstop_df_schema.withColumn("ingestion_date",current_timestamp())\
                    .withColumnRenamed("raceId","race_id")\
                    .withColumnRenamed("driverId","driver_id")
                     

# COMMAND ----------

display(pitstop_df_selected)

# COMMAND ----------

pitstop_df_selected.write.mode("overwrite").parquet('/mnt/devarjsa/silver/pit_stops')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/pit_stops

# COMMAND ----------

display(spark.read.parquet("/mnt/devarjsa/silver/pit_stops"))

# COMMAND ----------


