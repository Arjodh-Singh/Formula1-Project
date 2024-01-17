# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/devarjsa/bronze

# COMMAND ----------

constructors_df=spark.read\
    .json("/mnt/devarjsa/bronze/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType

# COMMAND ----------

constructors_schema=StructType([StructField("constructorId",IntegerType(),False),
                                   StructField("constructorRef",StringType(),False),
                                   StructField("name",StringType(),False),
                                   StructField("nationality",StringType(),False),
                                   StructField("url",StringType(),False)])

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

constructor_df=spark.read\
    .schema(constructors_schema)\
    .json("/mnt/devarjsa/bronze/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

constructors_df_timestamp=constructors_df.withColumn("ingestion_date",current_timestamp())\
                    .withColumnRenamed("constructorId","constructor_id")\
                    .withColumnRenamed("constructorRef","constructor_ref")

# COMMAND ----------

constructors_df_final=constructors_df_timestamp.drop("url")

# COMMAND ----------

display(constructors_df_final)

# COMMAND ----------

constructors_df_final.write.mode("overwrite").parquet('/mnt/devarjsa/silver/constructors')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/constructors

# COMMAND ----------


