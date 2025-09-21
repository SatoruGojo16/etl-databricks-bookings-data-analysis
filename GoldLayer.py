# Databricks notebook source
# MAGIC %md
# MAGIC **Adding Location Coordinates(Latitude & Longitude) to Staging Address Table**

# COMMAND ----------

from geopy.geocoders import Nominatim
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

coords_strct = StructType([
    StructField("latitide", StringType(), True),
    StructField("longitude", StringType(), True)
    ])
@udf(coords_strct)
def udf_get_coords(address):
    try:
        geolocator = Nominatim(user_agent="specify_your_app_name_here22")
        location = geolocator.geocode(address)
        return location.latitude, location.longitude
    except:
        return None, None

# COMMAND ----------

stag_address = spark.read.table('hotels.staging.stag_address')
stag_address = stag_address.withColumn('coordinates', udf_get_coords('address'))
stag_address = stag_address.withColumn('latitude', stag_address.coordinates.latitide)
stag_address = stag_address.withColumn('longitude', stag_address.coordinates.longitude)

# COMMAND ----------

stag_address = stag_address.drop('coordinates')
stag_address.writeTo('staging.stag_address').createOrReplace()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Dimension & Fact Tables**

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists hotels.curated;
# MAGIC use catalog hotels;

# COMMAND ----------

if spark.catalog.tableExists('curated.dim_review_category'):
    spark.sql(""" 
            MERGE INTO hotels.curated.dim_review_category AS target
            USING hotels.staging.stag_review_category AS source
            ON target.review_category = source.review_category
            WHEN NOT MATCHED THEN
            INSERT (review_category, review_category_id)
            VALUES (source.review_category, source.review_category_id)
            """)
else:
    spark.read.table('staging.stag_review_category') \
    .write \
    .saveAsTable('curated.dim_review_category')

# COMMAND ----------

if spark.catalog.tableExists('curated.dim_deal_type'):
    spark.sql(""" 
            MERGE INTO hotels.curated.dim_deal_type AS target
            USING hotels.staging.stag_deal_type AS source
            ON target.review_category = source.review_category
            WHEN NOT MATCHED THEN
            INSERT (deal_type, deal_type_description, deal_type_id)
            VALUES (source.deal_type, source.deal_type_description, source.deal_type_id)
            """)
else:
    spark.read.table('staging.stag_deal_type') \
    .write \
    .saveAsTable('curated.dim_deal_type')

# COMMAND ----------

df.writeTo('curated.fact_hotel').w

# COMMAND ----------

if spark.catalog.tableExists('curated.dim_address'):
    spark.sql(""" 
            MERGE INTO hotels.curated.dim_address AS target
            USING hotels.staging.stag_address AS source
            ON target.address = source.address
            WHEN NOT MATCHED THEN
            INSERT (address, country, address_id, latitude, longitude)
            VALUES (source.address, source.country, source.address_id, source.latitude, source.longitude)
            """)
else:
    spark.read.table('staging.stag_address') \
    .write \
    .saveAsTable('curated.dim_address')

# COMMAND ----------

if spark.catalog.tableExists('curated.dim_hotels'):
    spark.sql(""" 
            MERGE INTO hotels.curated.dim_hotels AS target
            USING hotels.staging.stag_hotels AS source
            ON target.hotel_name = source.hotel_name
            WHEN MATCHED THEN
            UPDATE SET target.booking_room_feature = source.booking_room_feature, target.room_description = source.room_description
            WHEN NOT MATCHED THEN
            INSERT (hotel_name, booking_room_feature, room_description, hotel_id)
            VALUES (source.hotel_name, source.booking_room_feature, source.room_description, source.hotel_id)
            """)
else:
    spark.read.table('staging.stag_hotels') \
    .write \
    .saveAsTable('curated.dim_hotels')

# COMMAND ----------

if not spark.catalog.tableExists('curated.dim_date'):
    dim_date = spark.sql('''
    SELECT explode(sequence(to_date('2000-01-01'), to_date('2030-01-01'))) as date
    ''')
    dim_date_cols = {
    "date_id":date_format(dim_date.date,'yMMdd'),
    "date":date_format(dim_date.date, 'd'),
    "month":date_format(dim_date.date, 'M'),
    "year":date_format(dim_date.date, 'y'),
    "day_short":date_format(dim_date.date, "E"), 
    "day_long":date_format(dim_date.date, "EEEE"), 
    "month_short":date_format(dim_date.date, "LLL"), 
    "month_long":date_format(dim_date.date, "LLLL"),
    "is_weekend": when((date_format(dim_date.date, "EEE").isin('Sat','Sun')),'Yes').otherwise('No')
    }
    dim_date = dim_date.withColumns(dim_date_cols)
    

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hotels.curated.fact_hotels
# MAGIC AS 
# MAGIC Select 
# MAGIC fact_hotels.hotel_id fact_hotel_id, 
# MAGIC stag_address.address_id,
# MAGIC stag_review_category.review_category_id,
# MAGIC stag_deal_type.deal_type_id,
# MAGIC stag_hotels.hotel_id
# MAGIC from staging.db_staging_hotels fact_hotels
# MAGIC join staging.stag_address 
# MAGIC on fact_hotels.address = stag_address.address
# MAGIC join staging.stag_review_category
# MAGIC on fact_hotels.review_category = stag_review_category.review_category
# MAGIC join staging.stag_deal_type
# MAGIC on fact_hotels.deal_type = stag_deal_type.deal_type
# MAGIC join staging.stag_hotels
# MAGIC on fact_hotels.title = stag_hotels.hotel_name
# MAGIC ;
# MAGIC Select * from hotels.curated.fact_hotels;