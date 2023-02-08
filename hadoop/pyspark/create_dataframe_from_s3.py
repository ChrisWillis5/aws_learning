# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkLearing") \
      .getOrCreate()

df=spark.read.parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet/")
df.show(10,False)


df.write.parquet("output/proto.parquet")