import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node step_trainer_trusted 
step_trainer_trusted_node1724412862232 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1724412862232")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1724412951923 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1724412951923")

# Script generated for node Aggregate accelerometer_trusted and step_trainer_trusted
SqlQuery259 = '''
select * from accelerometer_trusted
join step_trainer_trusted
on accelerometer_trusted.timestamp
=step_trainer_trusted.sensorreadingtime
'''
Aggregateaccelerometer_trustedandstep_trainer_trusted_node1724412968899 = sparkSqlQuery(glueContext, query = SqlQuery259, mapping = {"accelerometer_trusted":accelerometer_trusted_node1724412951923, "step_trainer_trusted":step_trainer_trusted_node1724412862232}, transformation_ctx = "Aggregateaccelerometer_trustedandstep_trainer_trusted_node1724412968899")

# Script generated for node machine_learning_curated
machine_learning_curated_node1724413270735 = glueContext.getSink(path="s3://aries-tiger-s3-bucket/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1724413270735")
machine_learning_curated_node1724413270735.setCatalogInfo(catalogDatabase="ariestiger_udacity",catalogTableName="machine_learning_curated")
machine_learning_curated_node1724413270735.setFormat("json")
machine_learning_curated_node1724413270735.writeFrame(Aggregateaccelerometer_trustedandstep_trainer_trusted_node1724412968899)
job.commit()