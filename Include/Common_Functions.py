# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def ingestion_date(input_data):
    output_data=input_data.withColumn("ingestion_date",current_timestamp())
    return(output_data)

# COMMAND ----------


