# Import SparkSession
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

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

# spark.sql(" set hive.e=true")


df=spark.read.parquet("s3://aws-train-nov-de-data/data/src_customer/customer_details_parquet/")
df1=df.filter("account_id_type = 'Saving'")
df2=df1.withColumn('min_balance',psf.lit(20))
df3=df.filter("account_id_type != 'Saving'")
df4=df3.withColumn('min_balance',psf.lit(10))
df5=df4.unionAll(df3)


# --spark-submit  create.py