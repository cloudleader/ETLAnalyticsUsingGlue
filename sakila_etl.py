import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
 
# Arguments passed to the glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Extract the destination bucket parameter
newparam = getResolvedOptions(sys.argv, ['destination'])

# Set the Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize the job
job.init(args['JOB_NAME'], args)

# Create dynamic frames for the tables we want to join
films = glueContext.create_dynamic_frame.from_catalog(database = "sakiladb", table_name = "glue_sakila_film")
film_to_cat = glueContext.create_dynamic_frame.from_catalog(database = "sakiladb", table_name = "glue_sakila_film_category")
categories = glueContext.create_dynamic_frame.from_catalog(database = "sakiladb", table_name = "glue_sakila_category")

# apply joins to tie together a film and its category
films_temp = Join.apply(frame1 = films, frame2 = film_to_cat, keys1 = ["film_id"], keys2 = ["film_id"])
films_to_cat = Join.apply(frame1 = films_temp, frame2 = categories, keys1 = ["category_id"], keys2 = ["category_id"])

# Construct the destination bucket URL
destinationbucket = "s3://" + newparam['destination']

# Write the films_to_cat dynamic frame to desired S3 bucket in PARQUET format
glueContext.write_dynamic_frame.from_options(frame = films_to_cat, connection_type = "s3", \
                                                connection_options = {"path": destinationbucket}, format = "parquet")
 
job.commit()
