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
CustomerTrusted_node1758821218000 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1758821218000")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758821216128 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1758821216128")

# Script generated for node Join Tables
SqlQuery3778 = '''
select * from al join ct on al.user = ct.email;

'''
JoinTables_node1758821222320 = sparkSqlQuery(glueContext, query = SqlQuery3778, mapping = {"ct":CustomerTrusted_node1758821218000, "al":AccelerometerLanding_node1758821216128}, transformation_ctx = "JoinTables_node1758821222320")

# Script generated for node Drop Fields
DropFields_node1758823668973 = DropFields.apply(frame=JoinTables_node1758821222320, paths=["customerName", "email", "phone", "birthDay", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithFriendsAsOfDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate"], transformation_ctx="DropFields_node1758823668973")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1758823668973, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758821151263", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1758822829478 = glueContext.getSink(path="s3://jtay553-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1758822829478")
AccelerometerTrusted_node1758822829478.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1758822829478.setFormat("json")
AccelerometerTrusted_node1758822829478.writeFrame(DropFields_node1758823668973)
job.commit()