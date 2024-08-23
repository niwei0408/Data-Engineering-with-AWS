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

# Script generated for node Customer Trusted
CustomerTrusted_node1724405944824 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724405944824")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1724405803420 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1724405803420")

# Script generated for node Join with Customer Trusted
SqlQuery317 = '''
select * from myAccelerometerLanding
join myCustomerTrusted
on myaccelerometerlanding.user = myCustomerTrusted.email
'''
JoinwithCustomerTrusted_node1724405875739 = sparkSqlQuery(glueContext, query = SqlQuery317, mapping = {"myAccelerometerLanding":AccelerometerLanding_node1724405803420, "myCustomerTrusted":CustomerTrusted_node1724405944824}, transformation_ctx = "JoinwithCustomerTrusted_node1724405875739")

# Script generated for node Drop Fields
SqlQuery318 = '''
select user,timestamp,x,y,z from myDataSource
'''
DropFields_node1724406937810 = sparkSqlQuery(glueContext, query = SqlQuery318, mapping = {"myDataSource":JoinwithCustomerTrusted_node1724405875739}, transformation_ctx = "DropFields_node1724406937810")

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1724406585019 = glueContext.getSink(path="s3://aries-tiger-s3-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_Trusted_node1724406585019")
Accelerometer_Trusted_node1724406585019.setCatalogInfo(catalogDatabase="ariestiger_udacity",catalogTableName="accelerometer_trusted")
Accelerometer_Trusted_node1724406585019.setFormat("json")
Accelerometer_Trusted_node1724406585019.writeFrame(DropFields_node1724406937810)
job.commit()