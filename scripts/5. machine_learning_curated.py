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
AWSGlueDataCatalog_node1681583010331 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1681583010331",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681583041375 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1681583041375",
)

# Script generated for node Join
Join_node1681583152282 = Join.apply(
    frame1=AWSGlueDataCatalog_node1681583041375,
    frame2=AWSGlueDataCatalog_node1681583010331,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1681583152282",
)

# Script generated for node Drop Fields
DropFields_node1681583200115 = DropFields.apply(
    frame=Join_node1681583152282,
    paths=["user"],
    transformation_ctx="DropFields_node1681583200115",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1681583617711 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1681583200115,
    database="udacity",
    table_name="machine_learning_curated",
    transformation_ctx="AWSGlueDataCatalog_node1681583617711",
)

job.commit()
