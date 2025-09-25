import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1758675409539 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1758675409539")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758675362256 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1758675362256")

# Script generated for node Join
Join_node1758676273670 = Join.apply(frame1=AccelerometerLanding_node1758675362256, frame2=CustomerTrusted_node1758675409539, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1758676273670")

# Script generated for node Drop Fields and Duplicates
SqlQuery4315 = '''
select distinct customername, email, phone, birthday,
serialnumber, registrationdate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
DropFieldsandDuplicates_node1758753882434 = sparkSqlQuery(glueContext, query = SqlQuery4315, mapping = {"myDataSource":Join_node1758676273670}, transformation_ctx = "DropFieldsandDuplicates_node1758753882434")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1758753882434, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758675268653", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1758675370601 = glueContext.getSink(path="s3://jtay553-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1758675370601")
CustomerCurated_node1758675370601.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1758675370601.setFormat("json")
CustomerCurated_node1758675370601.writeFrame(DropFieldsandDuplicates_node1758753882434)
job.commit()