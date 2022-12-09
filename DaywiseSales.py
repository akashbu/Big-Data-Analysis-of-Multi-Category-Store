from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


# ------------------ Schema Definition ----------------------------------------- #

mySchema = StructType([\
                       StructField("event_time", StringType(), True),
                       StructField("event_type", StringType(), True),
                       StructField("product_id",IntegerType(), True),
                       StructField("category_id",StringType(), True),
                       StructField("category_code", StringType(), True),
                       StructField("brand", StringType(), True),
                       StructField("price", StringType(), True),
                       StructField("user_id", IntegerType(), True),
                       StructField("user_session", StringType(), True),
                        ])

# ------------------------ Loading Data --------------------------------------- #

ecommerceData = spark.read.format("csv")\
    .schema(mySchema)\
    .option("mode", "DROPMALFORMED")\
    .option("path", "s3://sparkproject02/2019-Nov.csv")\
    .load()


ecommerceData.printSchema()
ecommerce = ecommerceData.withColumn("event_time", date_format(col("event_time"), "EEEE"))
ecommerce.show()


output = ecommerce.select(ecommerce.event_time,ecommerce.event_type, ecommerce.product_id, ecommerce.user_id, ecommerce.brand, ecommerce.price, ecommerce.category_code, ecommerce.user_session ).cache()
output.show()

# ------------ Creating View -----------------#
output.createOrReplaceTempView("view_one")

# --------------------- Daywise Sales --------------------------------------------------------- #

# outputDfNew = spark.sql("select event_time, count(event_type) from view_one where event_type = 'purchase' group by event_time ")
# outputDfNew.show()

# -------------------- Daywise Views ----------------------------------------------------------- #

outputDfNew = spark.sql("select event_time, count(event_type) from view_one where event_type = 'view' group by event_time ")
outputDfNew.show()

outputDfNew.repartition(1).write.format("csv").mode("overwrite").option("path", "s3://sparkproject02/output/DatewiseSales").save()