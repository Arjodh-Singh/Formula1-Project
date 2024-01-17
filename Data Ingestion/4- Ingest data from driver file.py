# Databricks notebook source
drivers_df=spark.read\
    .json("/mnt/devarjsa/bronze/drivers.json")

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
    .json("/mnt/devarjsa/bronze/drivers.json")

# COMMAND ----------

drivers_df_schema.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

drivers_df_selected=drivers_df_schema.withColumn("ingestion_date",current_timestamp())\
                    .withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("driverRef","driver_ref")\
                    .withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname")))

# COMMAND ----------

drivers_df_final=drivers_df_selected.drop("url")

# COMMAND ----------

display(drivers_df_final)

# COMMAND ----------

drivers_df_final.write.mode("overwrite").parquet('/mnt/devarjsa/silver/drivers')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/drivers

# COMMAND ----------

display(spark.read.parquet("/mnt/devarjsa/silver/drivers"))

# COMMAND ----------


