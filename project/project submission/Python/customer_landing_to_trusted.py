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

# Script generated for node Customer Landing
CustomerLanding_node1758487520346 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1758487520346")

# Script generated for node Share with Research
SqlQuery3705 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SharewithResearch_node1758487349845 = sparkSqlQuery(glueContext, query = SqlQuery3705, mapping = {"myDataSource":CustomerLanding_node1758487520346}, transformation_ctx = "SharewithResearch_node1758487349845")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1758487349845, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758488843608", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1758489090602 = glueContext.getSink(path="s3://jtay553-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1758489090602")
CustomerTrusted_node1758489090602.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1758489090602.setFormat("json")
CustomerTrusted_node1758489090602.writeFrame(SharewithResearch_node1758487349845)
job.commit()