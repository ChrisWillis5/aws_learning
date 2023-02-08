# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkLearing") \
      .getOrCreate()

df=spark.read.parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet/")
df.show(10,False)
df.count()
# df=spark.read.csv("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet/")
# df2=df.withColumn("col1",lit(10))
# df3=df.withColumn("col3",lit(30))
# df1.show(10,False)
# df2.write()
# df3.write()


df.write.parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet/")


# --spark-submit  create.py