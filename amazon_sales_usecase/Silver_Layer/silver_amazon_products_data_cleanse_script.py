# Databricks notebook source
from pyspark.sql.functions import col, regexp_extract, trim, regexp_replace

# Delta table load in dataframe 
df_base =  spark.read.table("bronze_layer.amazon_sales_product_schema.bronze_amazon_products_data")

# multiple copy in source side so here excluded one copy to remove duplicate
df_base_rm_dup_src =  df_base.filter(col("source_file") != "Amazon-Products.csv")

# productID column add
df_product_id =  df_base_rm_dup_src.withColumn("productID",regexp_extract(col("link"), r"/(?:dp|gp/product)/([A-Z0-9]{10})", 1))

# Still there are duplicate so removed duplicate value 
df_product_id_deDup = df_product_id.dropDuplicates(["productID"])

# removing null records 
df_product_id_deDup_NotNull = df_product_id_deDup.filter(col("productID").isNotNull() & col("name").isNotNull() & col("main_category").isNotNull() & col("sub_category").isNotNull() & col("image").isNotNull() & col("link").isNotNull() & col("ratings").isNotNull() & col("no_of_ratings").isNotNull() & col("discount_price").isNotNull() & col("actual_price").isNotNull())

# ID column drop as we have now productID
df_product_id_deDup_NotNull_removeID =  df_product_id_deDup_NotNull.drop("id")

# Re- Ordering columns 
new_order = ["productID","name","main_category","sub_category","ratings","no_of_ratings","discount_price","actual_price","Source_File","inserted_at"]
df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID.select(*new_order)

# sapce remove through trim function for column - ratings, no_of_ratings, discount_price, actual_price 

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("ratings", trim(col("ratings")))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("discount_price", trim(col("discount_price")))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("no_of_ratings", trim(col("no_of_ratings")))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("actual_price", trim(col("actual_price")))

# inr and comma symbol remove for column - discount_price, actual_price

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("discount_price", regexp_replace(col("discount_price"),"[₹,]",""))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("actual_price", regexp_replace(col("actual_price"),"[₹,]",""))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("no_of_ratings", regexp_replace(col("no_of_ratings"),"[,]",""))

# removing raw which has string or non numerical number for column -- ratings, no_of_ratings, discount_price, actual_price

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.filter(col("ratings").rlike("^[0-9]+(\\.[0-9]+)?$"))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.filter(col("no_of_ratings").rlike("^[0-9]+$"))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.filter(col("discount_price").rlike("^[0-9]+$"))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.filter(col("actual_price").rlike("^[0-9]+$"))

# casting rate, discount_price, actual_price to double and no_of_ratings to int

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("ratings", col("ratings").cast("double"))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("no_of_ratings", col("no_of_ratings").cast("int"))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("discount_price", col("discount_price").cast("double"))

df_product_id_deDup_NotNull_removeID_reordered = df_product_id_deDup_NotNull_removeID_reordered.withColumn("actual_price", col("actual_price").cast("double"))



# COMMAND ----------

df_product_id_deDup_NotNull_removeID_reordered.write.format("delta").mode("overwrite").saveAsTable("silver_layer.amazon_sales_product_schema.Silver_amazon_products_data_cleanse")

display(spark.read.table("silver_layer.amazon_sales_product_schema.Silver_amazon_products_data_cleanse"))