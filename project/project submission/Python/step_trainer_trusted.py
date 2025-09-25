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

# Script generated for node Customer Curated
CustomerCurated_node1758825456340 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1758825456340")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1758825455529 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1758825455529")

# Script generated for node Join Tables
SqlQuery3872 = '''
select * from cc join st 
on cc.serialNumber = st.serialNumber;
'''
JoinTables_node1758825545473 = sparkSqlQuery(glueContext, query = SqlQuery3872, mapping = {"cc":CustomerCurated_node1758825456340, "st":StepTrainerLanding_node1758825455529}, transformation_ctx = "JoinTables_node1758825545473")

# Script generated for node Drop Fields
DropFields_node1758825943939 = DropFields.apply(frame=JoinTables_node1758825545473, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1758825943939")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1758825943939, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758824362923", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1758825986219 = glueContext.getSink(path="s3://jtay553-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1758825986219")
StepTrainerTrusted_node1758825986219.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1758825986219.setFormat("json")
StepTrainerTrusted_node1758825986219.writeFrame(DropFields_node1758825943939)
job.commit()