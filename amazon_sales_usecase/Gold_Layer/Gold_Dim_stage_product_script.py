# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, lit

# initilize dataframe 
initilize_df = spark.read.table("silver_layer.amazon_sales_product_schema.silver_amazon_products_data_cleanse")

# selected column for product dataframe
product_df  = initilize_df.select("productID","name","main_category","sub_category")

# natural key renamed with proper name
product_df = product_df.withColumnRenamed("productID","productID_NK")

# surrogate key column added - productId, also audit column added -- inserted_At, inserted_By
product_df = product_df.withColumn("productID",monotonically_increasing_id()+1).withColumn("inserted_At",current_timestamp()).withColumn("inserted_by",lit('gold_Dim_stage_product_script'))

# ctegory dataframe initiize
categry_df  = spark.read.table("gold_layer.amazon_sales_product_schema.gold_dim_stage_category")

# left join between product_df and category_df on basis of main_category and sub_category
prod_df_with_cat = product_df.join(categry_df, on=[ product_df["main_category"] == categry_df["main_category"], product_df["sub_category"] == categry_df["sub_category"] ], how = "left")
prod_df_with_cat = prod_df_with_cat.select(product_df["*"], categry_df["Category_id"])

# main_category and sub_category column drop from product_df
prod_df_with_cat = prod_df_with_cat.drop("main_category","sub_category")

# column reorder 
clmn_reorder = ['productID','productID_NK','name','Category_id','inserted_At','inserted_by']
Fnl_df  = prod_df_with_cat.select(*clmn_reorder)


# COMMAND ----------

Fnl_df.write.format("delta").mode("overwrite").saveAsTable("gold_layer.amazon_sales_product_schema.gold_Dim_stage_product")