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

# Script generated for node Customer Landing
CustomerLanding_node1724404263953 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="customer_landing", transformation_ctx="CustomerLanding_node1724404263953")

# Script generated for node Share With Research
SqlQuery382 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null
 
'''
ShareWithResearch_node1724404587296 = sparkSqlQuery(glueContext, query = SqlQuery382, mapping = {"myDataSource":CustomerLanding_node1724404263953}, transformation_ctx = "ShareWithResearch_node1724404587296")

# Script generated for node Customer_Trusted
Customer_Trusted_node1724404685124 = glueContext.getSink(path="s3://aries-tiger-s3-bucket/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customer_Trusted_node1724404685124")
Customer_Trusted_node1724404685124.setCatalogInfo(catalogDatabase="ariestiger_udacity",catalogTableName="customer_trusted")
Customer_Trusted_node1724404685124.setFormat("json")
Customer_Trusted_node1724404685124.writeFrame(ShareWithResearch_node1724404587296)
job.commit()