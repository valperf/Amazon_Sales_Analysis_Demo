# Databricks notebook source
from pyspark.sql.functions import input_file_name, element_at, split, current_timestamp, regexp_replace,col,monotonically_increasing_id

raw_path = "abfss://raw-layer@amazonproductsalesucstrg.dfs.core.windows.net/raw-layer-16-01-2026-10-47-00/"
dgold_layerelta_path = "abfss://bronze-layer@amazonproductsalesucstrg.dfs.core.windows.net/"

df_with_source = spark.read.format("csv")\
    .option("header", "True") \
    .option("inferSchema","True") \
    .option("mergeSchema", "True") \
    .load(raw_path)

df_cleaned = df_with_source.drop("id")

df_final = df_cleaned \
    .withColumn("id", monotonically_increasing_id() + 1 ) \
    .withColumn("Source_File", regexp_replace(element_at(split(input_file_name(), "/"), -1), "%20", " ")) \
    .withColumn("inserted_at", current_timestamp())

cols = ["id"] + [c for c in df_cleaned.columns] + ["Source_File", "inserted_at"]
df_final = df_final.select(*cols)

df_final.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_layer.amazon_sales_product_schema.bronze_amazon_products_data")