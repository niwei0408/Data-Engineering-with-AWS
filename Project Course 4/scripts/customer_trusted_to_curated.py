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
CustomerTrusted_node1724408784812 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724408784812")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724408673123 = glueContext.create_dynamic_frame.from_catalog(database="ariestiger_udacity", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724408673123")

# Script generated for node Join Customer
SqlQuery430 = '''
select * from CustomerTrusted
join AccelerometerTrusted
on CustomerTrusted.email = AccelerometerTrusted.user
'''
JoinCustomer_node1724408779126 = sparkSqlQuery(glueContext, query = SqlQuery430, mapping = {"AccelerometerTrusted":AccelerometerTrusted_node1724408673123, "CustomerTrusted":CustomerTrusted_node1724408784812}, transformation_ctx = "JoinCustomer_node1724408779126")

# Script generated for node Drop Fields and Duplicates
SqlQuery429 = '''
select distinct customername,email,birthday,serialnumber,registrationdate,
lastupdatedate,sharewithresearchasofdate,sharewithfriendsasofdate,sharewithpublicasofdate
from myDataSource
'''
DropFieldsandDuplicates_node1724410238171 = sparkSqlQuery(glueContext, query = SqlQuery429, mapping = {"myDataSource":JoinCustomer_node1724408779126}, transformation_ctx = "DropFieldsandDuplicates_node1724410238171")

# Script generated for node Customers Curated
CustomersCurated_node1724409641728 = glueContext.getSink(path="s3://aries-tiger-s3-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1724409641728")
CustomersCurated_node1724409641728.setCatalogInfo(catalogDatabase="ariestiger_udacity",catalogTableName="customers_curated")
CustomersCurated_node1724409641728.setFormat("json")
CustomersCurated_node1724409641728.writeFrame(DropFieldsandDuplicates_node1724410238171)
job.commit()