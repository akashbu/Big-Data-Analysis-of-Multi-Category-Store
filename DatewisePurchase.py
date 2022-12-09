from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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
ecommerceData.show()

ecommerce = ecommerceData.withColumn("event_time", func.to_date(func.col("event_time")))
ecommerce.show()

output = ecommerce.select(ecommerce.event_time, ecommerce.event_type, ecommerce.product_id, ecommerce.user_id, ecommerce.brand, ecommerce.price, ecommerce.category_code, ecommerce.user_session ).cache()

# ------------- Creating View ------------------- #

output.createOrReplaceTempView("view_one")

# -------------------- Datewise Purchase ----------------------------------------------- #

outputDf = spark.sql("select event_time, count(user_id) from view_one where event_type = 'purchase' group by event_time order by event_time ")
outputDf.show(30)


outputDf.repartition(1).write.format("csv").mode("overwrite").option("path", "s3://sparkproject02/output/DatewisePurchase").save()