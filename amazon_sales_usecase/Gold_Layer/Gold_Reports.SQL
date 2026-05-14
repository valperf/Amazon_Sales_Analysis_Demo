
 -- most_profitable_good_rating_product

 CREATE OR REPLACE VIEW 
 gold_layer.amazon_sales_product_schema.most_profitable_good_rating_product
 AS

 SELECT

     p.productID_NK,
     p.name,

     c.main_category,
     c.sub_category,

     pf.discount_price,
     pf.actual_price,

     (
         pf.actual_price - pf.discount_price
     ) AS price_difference,

     rf.rating,
     rf.rating_count

 FROM
 gold_layer.amazon_sales_product_schema.gold_Dim_stage_product p

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_Dim_stage_category c
 ON p.Category_id = c.Category_id

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_rating_fact rf
 ON p.productID_NK = rf.productID_NK

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_price_fact pf
 ON p.productID_NK = pf.productID_NK

 WHERE
 rf.rating > 4

 AND
 (
     pf.actual_price - pf.discount_price
 )
 BETWEEN 1 AND 70 ;
 SELECT * FROM gold_layer.amazon_sales_product_schema.most_profitable_good_rating_product;



 -- less_profitable_good_rating_product
 CREATE OR REPLACE VIEW 
 gold_layer.amazon_sales_product_schema.less_profitable_good_rating_product
 AS

 SELECT

     p.productID_NK,
     p.name,

     c.main_category,
     c.sub_category,

     pf.discount_price,
     pf.actual_price,

     (
         pf.actual_price - pf.discount_price
     ) AS price_difference,

     rf.rating,
     rf.rating_count

 FROM
 gold_layer.amazon_sales_product_schema.gold_Dim_stage_product p

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_Dim_stage_category c
 ON p.Category_id = c.Category_id

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_rating_fact rf
 ON p.productID_NK = rf.productID_NK

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_price_fact pf
 ON p.productID_NK = pf.productID_NK

 WHERE
 rf.rating > 4

 AND
 (
     pf.actual_price - pf.discount_price
 ) >= 70 ;

 SELECT * 
 FROM gold_layer.amazon_sales_product_schema.less_profitable_good_rating_product;


 -- less_rating_product
 CREATE OR REPLACE VIEW 
 gold_layer.amazon_sales_product_schema.less_rating_product
 AS

 SELECT

     p.productID_NK,
     p.name,

     c.main_category,
     c.sub_category,

     pf.discount_price,
     pf.actual_price,

     (
         pf.actual_price - pf.discount_price
     ) AS price_difference,

     rf.rating,
     rf.rating_count

 FROM
 gold_layer.amazon_sales_product_schema.gold_Dim_stage_product p

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_Dim_stage_category c
 ON p.Category_id = c.Category_id

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_rating_fact rf
 ON p.productID_NK = rf.productID_NK

 INNER JOIN
 gold_layer.amazon_sales_product_schema.gold_price_fact pf
 ON p.productID_NK = pf.productID_NK

 WHERE
 rf.rating < 3 ;

 SELECT * 
 FROM gold_layer.amazon_sales_product_schema.less_rating_product;