import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681487988671 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1681487988671",
)

# Script generated for node Amazon S3
AmazonS3_node1681488209062 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sjohal/udacity-lake/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681488209062",
)

# Script generated for node Join
Join_node1681488276037 = Join.apply(
    frame1=AmazonS3_node1681488209062,
    frame2=AWSGlueDataCatalog_node1681487988671,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1681488276037",
)

# Script generated for node Drop Fields
DropFields_node1681488383237 = DropFields.apply(
    frame=Join_node1681488276037,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1681488383237",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681488476194 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1681488383237,
    database="udacity",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1681488476194",
)

job.commit()
