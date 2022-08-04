# Obtain the data from XML strings and convert them into dataframe
import findspark
findspark.init()
import requests
import xml.etree.ElementTree as ET
from pyspark import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql import *
from pyspark.sql.types import *

# Create session
spark = SparkSession.builder.getOrCreate()

# Create a new data frame for movie_review.csv
log_df = spark.read.csv('s3://s3-data-bootcamp-20220804183240579100000005/raw/log_reviews.csv', sep=',', header=True)

# Take a little part of the data frame to make tests.
mini_df = log_df.limit(100000)
#mini_df.printSchema()

# Create a schema to keep the data that comes from xml
extract_log_info_schema = StructType([
    StructField("logDate", StringType(), True),
    StructField("device", StringType(), True),
    StructField("location", StringType(), True),
    StructField("os", StringType(), True),
    StructField("ipAddress", StringType(), True),
    StructField("phoneNumber", StringType(), True),
])

# Define a function to obtain the selected data from xml
def select_text(doc, xpath):
    nodes = [e.text for e in doc.findall('./log/'+ xpath) if isinstance(e, ET.Element)]
    return next(iter(nodes), None)

# Define a function to extract the data
def extract_log_info(payload):
    doc = ET.fromstring(payload)
    return {
        'logDate':  select_text(doc, 'logDate'),
        'device': select_text(doc, 'device'),
        'location': select_text(doc, 'location'),
        'os': select_text(doc, 'os'),
        'ipAddress': select_text(doc, 'ipAddress'),
        'phoneNumber': select_text(doc, 'phoneNumber')
        }
# Create a udf function
extract_log_info_udf = udf(extract_log_info, extract_log_info_schema)

# Output dataframe
another_df = mini_df.withColumn("info", extract_log_info_udf('log')).select('id_review', 'info.logDate', 'info.device', 'info.location', 'info.os', 'info.ipAddress', 'info.phoneNumber')

# Save df
another_df.write.mode("overwrite").parquet("s3://s3-data-bootcamp-20220804183240579100000005/output")
