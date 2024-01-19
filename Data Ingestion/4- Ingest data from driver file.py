# Databricks notebook source
# MAGIC %run ../Include/Config
# MAGIC

# COMMAND ----------

# MAGIC %run ../Include/Common_Functions

# COMMAND ----------

drivers_df=spark.read\
    .json(f"{bronze_path_folder}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType

# COMMAND ----------

name_schema=StructType([StructField("forename",StringType(),False),
                                   StructField("surname",StringType(),False)
                                   ])

# COMMAND ----------

driver_schema=StructType([StructField("code",StringType(),False),
                          StructField("dob",DateType(),False),
                          StructField("driverId",IntegerType(),False),
                            StructField("driverRef",StringType(),False),
                            StructField("name",name_schema),
                            StructField("nationality",StringType(),False),
                            StructField("number",IntegerType(),False),
                            StructField("url",StringType(),False)])

# COMMAND ----------

drivers_df_schema=spark.read\
    .schema(driver_schema)\
    .json(f"{bronze_path_folder}/drivers.json")

# COMMAND ----------

drivers_df_schema.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

drivers_df_selected=drivers_df_schema.withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("driverRef","driver_ref")\
                    .withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname")))

# COMMAND ----------

drivers_df_selected=ingestion_date(drivers_df_selected)

# COMMAND ----------

drivers_df_final=drivers_df_selected.drop("url")

# COMMAND ----------

display(drivers_df_final)

# COMMAND ----------

drivers_df_final.write.mode("overwrite").parquet(f'{silver_path_folder}/drivers')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/drivers

# COMMAND ----------

display(spark.read.parquet(f"{silver_path_folder}/drivers"))

# COMMAND ----------


