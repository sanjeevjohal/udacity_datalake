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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1676402494135 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1676402494135",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sjohal/udacity-lake/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1",
)

# Script generated for node Join Customer
JoinCustomer_node1626344351568 = Join.apply(
    frame1=CustomerTrustedZone_node1,
    frame2=AccelerometerLanding_node1676402494135,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1626344351568",
)

# Script generated for node Drop Fields
DropFields_node1626344452359 = DropFields.apply(
    frame=JoinCustomer_node1626344351568,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "timestamp",
    ],
    transformation_ctx="DropFields_node1626344452359",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1626344482987 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1626344452359,
    database="udacity_project_data_lake",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1626344482987",
)

job.commit()
