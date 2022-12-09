from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# ------------------Schema Definition ----------------------------------------- #

mySchema = StructType([\
                       StructField("event_time", StringType(), True),
                       StructField("event_type", StringType(), True),
                       StructField("product_id",IntegerType(), True),
                       StructField("category_id",IntegerType(), True),
                       StructField("category_code", StringType(), True),
                       StructField("brand", StringType(), True),
                       StructField("price", StringType(), True),
                       StructField("user_id", IntegerType(), True),
                       StructField("user_session", StringType(), True),
                        ])

# ------------------------ Loading Data --------------------------------------- #

ecommerce = spark.read.format("csv")\
    .schema(mySchema)\
    .option("mode", "DROPMALFORMED")\
    .option("path", "s3://sparkproject02/2019-Nov.csv")\
    .load()


ecommerce.printSchema()
ecommerce.show()


output = ecommerce.select(ecommerce.event_time, ecommerce.event_type,
                          ecommerce.product_id, ecommerce.brand, ecommerce.category_code, ecommerce.price,
                          ecommerce.user_id, ecommerce.user_session).cache()

output.show()

# ------------- Creating View ------------------- #

output.createOrReplaceTempView("view_one")

# -------------- Users Journey -------------- #

outputDf = spark.sql("select * from view_one where user_id = 518267348")

outputDf.show()
