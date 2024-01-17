# Databricks notebook source
# MAGIC %run ../Include/Config
# MAGIC

# COMMAND ----------

# MAGIC %run ../Include/Common_Functions

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/bronze

# COMMAND ----------

constructors_df=spark.read\
    .json(f"{bronze_path_folder}/constructors.json")

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
    .json(f"{bronze_path_folder}/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,concat,col,lit

# COMMAND ----------

constructors_df_timestamp=constructors_df.withColumnRenamed("constructorId","constructor_id")\
                    .withColumnRenamed("constructorRef","constructor_ref")

# COMMAND ----------

constructors_df_timestamp=ingestion_date(constructors_df_timestamp)

# COMMAND ----------

constructors_df_final=constructors_df_timestamp.drop("url")

# COMMAND ----------

display(constructors_df_final)

# COMMAND ----------

constructors_df_final.write.mode("overwrite").parquet(f'{silver_path_folder}/constructors')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/devarjsa/silver/constructors

# COMMAND ----------

display(spark.read.parquet(f"{silver_path_folder}/constructors"))
