import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as psf

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
df=spark.read.parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet/")
df1=df.filter("account_id_type = 'Saving'")
df2=df1.withColumn('min_balance',psf.lit(20))
df3=df.filter("account_id_type != 'Saving'")
df4=df3.withColumn('min_balance',psf.lit(10))
df5=df4.unionAll(df2)

df5.show(10,False)
df5.coalesce(2).write.mode('overwrite').parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet_write_shrey/load_date=2023-01-02 ")

job.commit()