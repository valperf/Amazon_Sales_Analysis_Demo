# Databricks notebook source
from pyspark.sql.functions import *

#load silver table
df_silver = spark.read.table("silver_layer.amazon_sales_product_schema.silver_amazon_products_data_cleanse")

# rating fact table ----------
df_rating_fact = df_silver.select(
    col("productID").alias("productID_NK"),col("ratings").cast("double").alias("rating"),col("no_of_ratings").cast("int").alias("rating_count")
)

# Inserted at time ----------
df_rating_fact = df_rating_fact.withColumn(
    "Inserted_At",
    current_timestamp()
)

# Inserted by who ----------
df_rating_fact = df_rating_fact.withColumn(
    "Inserted_By",
    lit("Gold_Fact_Script")
)

# save as delta ----------
df_rating_fact.write.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "gold_layer.amazon_sales_product_schema.gold_rating_fact"
)

rating_df = spark.read.table("gold_layer.amazon_sales_product_schema.gold_rating_fact")

# price fact ----------
df_price_fact = df_silver.select(
    col("productID").alias("productID_NK"),regexp_replace(col("actual_price"),"[^0-9.]","").cast("double").alias("actual_price"), regexp_replace(col("discount_price"),"[^0-9.]","").cast("double").alias("discount_price")
)

# discount percentage ----------
df_price_fact = df_price_fact.withColumn("discount_percentage",round(((col("actual_price") - col("discount_price"))/
col("actual_price")) * 100,2))

# inserted at time----------
df_price_fact = df_price_fact.withColumn(
    "Inserted_At",
    current_timestamp()
)

# inserted by who----------
df_price_fact = df_price_fact.withColumn(
    "Inserted_By",
    lit("Gold_Fact_Script")
)

# save as delta ----------
df_price_fact.write.format("delta") \
.mode("overwrite") \
.saveAsTable(
    "gold_layer.amazon_sales_product_schema.gold_price_fact"
)

# display ----------
price_df = spark.read.table("gold_layer.amazon_sales_product_schema.gold_price_fact")
display(rating_df)
display(price_df)