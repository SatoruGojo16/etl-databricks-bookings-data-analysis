# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists hotels.staging;
# MAGIC use catalog hotels;
# MAGIC use schema staging;

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import date 

# COMMAND ----------

current_date = date.today()
day = current_date.day
month = current_date.month
year = current_date.year
raw_bucket_path = f's3://raw-data-bucket-5f593a/raw_hotels/year={year}/month={month}/day={day}'
df = spark.read.parquet(raw_bucket_path)

# COMMAND ----------

cast_columns_list = {
    "title": F.col('title').cast(T.StringType()),
    "review_text": F.col('review_text').cast(T.StringType()),
    "sustainability_cert": F.col('sustainability_cert').cast(T.StringType()),
    "address": F.col('address').cast(T.StringType()),
    "total_reviews": F.col('total_reviews').cast(T.StringType()),
    "review_score": F.col('review_score').cast(T.StringType()),
    "price_undiscounted_amount": F.col('price_undiscounted_amount').cast(T.StringType()),
    "price_discounted_amount": F.col('price_discounted_amount').cast(T.StringType()),
    "taxes_and_charges": F.col('taxes_and_charges').cast(T.StringType()),
    "booking_room_feature": F.col('booking_room_feature').cast(T.StringType()),
    "red_text": F.col('red_text').cast(T.StringType()),
    "room_entire_text": F.col('room_entire_text').cast(T.StringType()),
    "breakfast_included": F.col('breakfast_included').cast(T.StringType()),
    "free_cancellation": F.col('free_cancellation').cast(T.StringType()),
    "deal_type": F.col('deal_type').cast(T.StringType()),
    "deal_type_description": F.col('deal_type_description').cast(T.StringType()),
    "stars_list_count": F.col('stars_list_count').cast(T.DecimalType(2,1)),
    "highly_rated_luxurious_stay": F.col('highly_rated_luxurious_stay').cast(T.StringType()),
    "image_link": F.col('image_link').cast(T.StringType()),
    "location_name": F.col('location_name').cast(T.StringType()),
} 
df = df.withColumns(cast_columns_list) 

# COMMAND ----------

df = df.withColumn('review_category', F.initcap(df.review_text))
df = df.withColumn('total_reviews_count',F.when(df.total_reviews.rlike(r"^[\d,]* reviews"),
                   F.regexp_extract(df.total_reviews,r"^([\d,]*) reviews", 1 ))
                   .otherwise('0'))


# COMMAND ----------

df = df.withColumn('price_undiscounted_amount',
                   F.regexp_extract(df.price_undiscounted_amount,r"([\d,]+)", 1 ))
df = df.withColumn('price_discounted_amount',
                   F.regexp_extract(df.price_discounted_amount,r"([\d,]+)", 1 ))
df = df.withColumn('total_rooms_left_count',
                   F.regexp_extract(df.red_text,r"^Only ([\d,]+) ", 1 ))
df = df.withColumn('taxes_and_charges_amount', F.when(df.taxes_and_charges.rlike(r"([\d]+ taxes and charges$)"),
                   F.regexp_extract(df.taxes_and_charges,r"([\d]+)", 1 )).otherwise(0))

# COMMAND ----------


df = df.withColumn('price_undiscounted_amount', 
                   F.regexp_replace(df.price_undiscounted_amount, ',',''))
df = df.withColumn('price_discounted_amount', 
                   F.regexp_replace(df.price_discounted_amount, ',',''))
df = df.withColumn('total_reviews_count', 
                   F.regexp_replace(df.total_reviews_count, ',',''))

# COMMAND ----------

df = df.na.fill(
    {'breakfast_included':'Not-Provided',
     'free_cancellation':'No Cancellation Allowed', 
     'deal_type':'Not-Specified', 
     'deal_type_description':'Non-Applicable',
     'highly_rated_luxurious_stay':'No',
     'stars_list_count':0,
     'sustainability_cert': 'No',
     'total_rooms_left_count': 0,
     'taxes_and_charges_amount': 0,
     'price_undiscounted_amount': 0,
     'price_discounted_amount': 0
    }
)

# COMMAND ----------

df = df.withColumn('free_cancellation',F.initcap(df.free_cancellation))

# COMMAND ----------

df = df.withColumn('stars_list_count', df.stars_list_count.cast('int'))

# COMMAND ----------

df = df.withColumn('last_updated_date', F.date_format(F.current_date(),'yMMdd')) 

# COMMAND ----------

df = df.withColumnRenamed('sustainability_cert','sustainability_certification')

# COMMAND ----------

select_cls = [
    'title',
    'sustainability_certification',
    'address',
    'review_category',
    'total_reviews_count',
    'review_score',
    'price_undiscounted_amount',
    'price_discounted_amount',
    'booking_room_feature',
    'total_rooms_left_count',
    'room_entire_text',
    'breakfast_included',
    'free_cancellation',
    'deal_type',
    'deal_type_description',
    'stars_list_count',
    'highly_rated_luxurious_stay',
    'location_name',
    'taxes_and_charges_amount',
    'last_updated_date'
    ]
df = df.select(select_cls)

# COMMAND ----------

staging_hotels = spark.sql("""
SELECT *, ROW_NUMBER() OVER (ORDER BY title) AS hotel_id FROM {df}
""", df=df)
staging_hotels_bucket_path = f's3://processed-data-bucket-5f593a/staging_hotels/'
staging_hotels.write.option("overwriteSchema","true").mode('overwrite').option("path",staging_hotels_bucket_path).saveAsTable('staging_hotels')

# COMMAND ----------

staging_bucket_path = f's3://processed-data-bucket-5f593a/staging/data_model/'

# COMMAND ----------

if spark.catalog.tableExists('stag_review_category'):
    max_review_category_id = spark.sql("select max(review_category_id) FROM stag_review_category").collect()[0][0]
else:
    max_review_category_id = 0
spark.sql(f"DECLARE OR REPLACE VARIABLE max_review_category_id = {max_review_category_id}")  

# COMMAND ----------

stag_review_category = spark.sql("""
SELECT review_category AS review_category,
ROW_NUMBER() OVER (ORDER BY review_category) + max_review_category_id AS review_category_id
FROM (SELECT distinct review_category
FROM staging_hotels
)
""")
stag_review_category.write.mode('overwrite').option("path",staging_bucket_path+'staging_review_category/').saveAsTable('stag_review_category')


# COMMAND ----------

if spark.catalog.tableExists('stag_deal_type'):
    max_stag_deal_type_id = spark.sql("select max(deal_type_id) FROM stag_deal_type").collect()[0][0]
else:
    max_stag_deal_type_id = 0
spark.sql(f"DECLARE OR REPLACE VARIABLE max_stag_deal_type_id = {max_stag_deal_type_id}")  

# COMMAND ----------

stag_deal_type = spark.sql("""
SELECT deal_type, deal_type_description,
ROW_NUMBER() OVER (ORDER BY deal_type) + max_stag_deal_type_id AS deal_type_id
FROM (SELECT distinct deal_type, deal_type_description
FROM staging_hotels
)
""")
stag_deal_type.write.mode('overwrite').option("path",staging_bucket_path+'staging_deal_type/').saveAsTable('stag_deal_type')

# COMMAND ----------

if spark.catalog.tableExists('stag_address'):
    max_stag_address_id = spark.sql("select max(address_id) FROM stag_address").collect()[0][0]
else:
    max_stag_address_id = 0
spark.sql(f"DECLARE OR REPLACE VARIABLE max_stag_address_id = {max_stag_address_id}")  

# COMMAND ----------

stag_address = spark.sql("""
SELECT address, location_name as country,
ROW_NUMBER() OVER (ORDER BY address) + max_stag_address_id AS address_id
FROM (SELECT distinct address, location_name
FROM staging_hotels
)
""")
stag_address.write.option("overwriteSchema", "true").mode('overwrite').option("path",staging_bucket_path+'staging_address/').saveAsTable('stag_address')

# COMMAND ----------

if spark.catalog.tableExists('stag_hotels'):
    max_stag_hotel_id = spark.sql("select max(hotel_id) FROM stag_hotels").collect()[0][0]
else:
    max_stag_hotel_id = 0
spark.sql(f"DECLARE OR REPLACE VARIABLE max_stag_hotel_id = {max_stag_hotel_id}")  

# COMMAND ----------

stag_hotels = spark.sql("""
SELECT title hotel_name, booking_room_feature, room_entire_text as room_description,
ROW_NUMBER() OVER (ORDER BY title) + max_stag_hotel_id AS hotel_id
FROM (SELECT title, booking_room_feature, room_entire_text
FROM staging_hotels
)
""")
stag_hotels.write.option("mergeSchema", "true").mode('overwrite').option("path",staging_bucket_path+'staging_hotels/').saveAsTable('stag_hotels')