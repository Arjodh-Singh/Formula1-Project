# Databricks notebook source
circuits_df=spark.read\
    .option("header",True)\
    .option("infer_schema",True)\
    .csv("/mnt/devarjsa/bronze/circuits.csv")
   

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

circuits_schema=StructType([StructField("circuitid",IntegerType(),False),
                                   StructField("circuitref",IntegerType(),False),
                                   StructField("name",StringType(),False),
                                   StructField("location",StringType(),False),
                                   StructField("country",StringType(),False),
                                   StructField("lat",DoubleType(),False),
                                   StructField("lng",DoubleType(),False),
                                   StructField("alt",IntegerType(),False),
                                   StructField("url",StringType(),False)])
                                   

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/devarjsa/bronze"))

# COMMAND ----------

circuits_df=spark.read\
    .option("header",True)\
    .schema(circuits_schema)\
    .csv("/mnt/devarjsa/bronze/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_df_selected=circuits_df.select(col("circuitid"),col('circuitref'),col("name"),col("location"),
                   col("country"),col("lat"),col("lng"),col("alt"))
                       
circuits_df_selected.show()

# COMMAND ----------

circuits_df_renamed=circuits_df_selected.withColumnRenamed("circuitid","circuit_id")\
                    .withColumnRenamed("circuitref","circuit_ref")\
                    .withColumnRenamed("lat","latitude")\
                    .withColumnRenamed("lng","longitude")    

# COMMAND ----------

display(circuits_df_renamed)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_df_final=circuits_df_renamed.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(circuits_df_final)

# COMMAND ----------

circuits_df_final.printSchema()

# COMMAND ----------

circuits_df_final.write.mode("overwrite").parquet('/mnt/devarjsa/silver/circuits')

# COMMAND ----------


