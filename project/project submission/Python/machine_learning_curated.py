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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1758826459417 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1758826459417")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758826458054 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://jtay553-lake-house/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1758826458054")

# Script generated for node Join Tables
SqlQuery4099 = '''
select * from st join act 
on st.sensorReadingTime = act.timeStamp;
'''
JoinTables_node1758826538314 = sparkSqlQuery(glueContext, query = SqlQuery4099, mapping = {"st":StepTrainerTrusted_node1758826459417, "act":AccelerometerTrusted_node1758826458054}, transformation_ctx = "JoinTables_node1758826538314")

# Script generated for node Drop Fields
DropFields_node1758826897146 = DropFields.apply(frame=JoinTables_node1758826538314, paths=["user", "timestamp"], transformation_ctx="DropFields_node1758826897146")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=DropFields_node1758826897146, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758826390842", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1758826989615 = glueContext.getSink(path="s3://jtay553-lake-house/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1758826989615")
MachineLearningCurated_node1758826989615.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1758826989615.setFormat("json")
MachineLearningCurated_node1758826989615.writeFrame(DropFields_node1758826897146)
job.commit()