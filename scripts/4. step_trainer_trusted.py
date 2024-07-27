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
AWSGlueDataCatalog_node1681489351966 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1681489351966",
)

# Script generated for node Amazon S3
AmazonS3_node1681489376359 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sjohal/udacity-lake/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1681489376359",
)

# Script generated for node Join
Join_node1681489400758 = Join.apply(
    frame1=AmazonS3_node1681489376359,
    frame2=AWSGlueDataCatalog_node1681489351966,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1681489400758",
)

# Script generated for node Drop Fields
DropFields_node1681489426597 = DropFields.apply(
    frame=Join_node1681489400758,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1681489426597",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681489441442 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1681489426597,
    database="udacity",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1681489441442",
)

job.commit()
