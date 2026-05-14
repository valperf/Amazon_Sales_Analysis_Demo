# Databricks notebook source
from pyspark.sql.functions import count, sum 

# duplicate data validation 
df_product_id_deDup_NotNull_removeID_reordered.groupBy('productID').agg(count("*").alias("cnt")).filter(col("cnt") > 1).display()

# null validation check 
df_product_id_deDup_NotNull_removeID_reordered.filter( (col("productID").isNull()) | (col("name").isNull()) | (col("main_category").isNull()) | (col("sub_category").isNull()) | (col("ratings").isNull()) | (col("no_of_ratings").isNull()) | (col("discount_price").isNull()) | (col("actual_price").isNull()) | (col("Source_File").isNull()) | (col("inserted_at").isNull())  ).display()

# to check column is correctly type taking sum 
df_product_id_deDup_NotNull_removeID_reordered.agg(sum("ratings")).display()

df_product_id_deDup_NotNull_removeID_reordered.agg(sum("no_of_ratings")).display()

df_product_id_deDup_NotNull_removeID_reordered.agg(sum("discount_price")).display()

df_product_id_deDup_NotNull_removeID_reordered.agg(sum("actual_price")).display()