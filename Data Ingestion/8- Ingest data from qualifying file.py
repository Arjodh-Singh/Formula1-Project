# Databricks notebook source
# MAGIC %fs
# MAGIC ls 

# COMMAND ----------

qualify_times_df=spark.read\
    .option("multiline",True)\
    .json("/mnt/devarjsa/bronze/qualifying")

# COMMAND ----------

display(qualify_times_df)

# COMMAND ----------

qualify_times_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType,FloatType

# COMMAND ----------

qualify_times_schema=StructType([StructField("constructorId",IntegerType(),False),
                             StructField("driverId",IntegerType(),False),
                            StructField("number",IntegerType(),False),
                            StructField("position",IntegerType(),False),
                            StructField("q1",StringType(),False),
                            StructField("q2",StringType(),False),
                            StructField("q3",StringType(),False),
                            StructField("qualifyId",IntegerType(),False),
                            StructField("raceId",IntegerType(),False)  ])

# COMMAND ----------

qualify_times_df_schema=spark.read\
    .schema(qualify_times_schema)\
    .option("multiline",True)\
    .json("/mnt/devarjsa/bronze/qualifying")

# COMMAND ----------

display(qualify_times_df_schema)

# COMMAND ----------

qualify_times_df_schema.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

qualify_times_df_final=qualify_times_df_schema.withColumn("ingestion_date",current_timestamp())\
                    .withColumnRenamed("constructorId","constructor_id")\
                    .withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("qualifyId","qualify_id")\
                    .withColumnRenamed("raceId","race_id")
                     

# COMMAND ----------

display(qualify_times_df_final)

# COMMAND ----------

qualify_times_df_final.write.mode("overwrite").parquet('/mnt/devarjsa/silver/qualifying')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/qualifying

# COMMAND ----------

display(spark.read.parquet("/mnt/devarjsa/silver/qualifying"))

# COMMAND ----------


