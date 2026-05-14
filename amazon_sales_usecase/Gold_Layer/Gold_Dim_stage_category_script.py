# Databricks notebook source
from pyspark.sql.functions import col, monotonically_increasing_id, current_timestamp, lit

# initialiaze dataframe
initial_df = spark.read.table("silver_layer.amazon_sales_product_schema.silver_amazon_products_data_cleanse")

# select unique main and sub category
category_df = initial_df.select("main_category","sub_category").distinct().orderBy(["main_category","sub_category"], ascending = True)

# category_id as a surrogate key, inserted_At and Inserted_By as audit column add
category_df = category_df.withColumn("Category_id",monotonically_increasing_id()+1).withColumn("Inserted_At",current_timestamp()).withColumn("Inserted_By",lit("gold_Dim_stage_category_script"))

# reordeing list of column `
reorder_clmn = ['Category_id','main_category','sub_category','Inserted_At','Inserted_By']
fnl_category_df = category_df.select(*reorder_clmn) 


# COMMAND ----------

# load data into gold_Dim_stage_category delta table
fnl_category_df.write.format("delta").mode('overwrite').saveAsTable("gold_layer.amazon_sales_product_schema.gold_Dim_stage_category")