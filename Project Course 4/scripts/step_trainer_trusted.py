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

# Script generated for node customers_curated
customers_curated_node1724411149611 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="customers_curated", transformation_ctx="customers_curated_node1724411149611")

# Script generated for node step_trainer_landing
step_trainer_landing_node1724411097162 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1724411097162")

# Script generated for node Join Customer and Drop Fields
SqlQuery436 = '''
select 
sensorReadingTime, step_trainer_landing.serialNumber, distanceFromObject 
from step_trainer_landing
join customers_curated
on step_trainer_landing.serialnumber = customers_curated.serialnumber
'''
JoinCustomerandDropFields_node1724411180208 = sparkSqlQuery(glueContext, query = SqlQuery436, mapping = {"step_trainer_landing":step_trainer_landing_node1724411097162, "customers_curated":customers_curated_node1724411149611}, transformation_ctx = "JoinCustomerandDropFields_node1724411180208")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1724411459804 = glueContext.getSink(path="s3://aries-tiger-s3-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1724411459804")
step_trainer_trusted_node1724411459804.setCatalogInfo(catalogDatabase="ariestiger_udacity",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1724411459804.setFormat("json")
step_trainer_trusted_node1724411459804.writeFrame(JoinCustomerandDropFields_node1724411180208)
job.commit()