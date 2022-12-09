from pyspark.sql import SparkSession
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

ecommerce = spark.read.format("csv")\
    .schema(mySchema)\
    .option("mode", "DROPMALFORMED")\
    .option("path", "s3://sparkproject02/2019-Nov.csv")\
    .load()

ecommerce.printSchema()
ecommerce.show()

output = ecommerce.select(ecommerce.event_type, ecommerce.product_id, ecommerce.user_id, ecommerce.brand,
                          ecommerce.price, ecommerce.category_code, ecommerce.user_session ).cache()

# ------------- Creating View ------------------- #

output.createOrReplaceTempView("view_one")

# -------------- Most Visited Category -------------- #
outputDf = spark.sql("select category_code, count(category_code) as view_count from view_one where event_type = 'view' group by category_code order by view_count desc  ")
outputDf.show()


outputDf.repartition(1).write.format("csv").mode("overwrite").option("path", "s3://sparkproject02/output/MostVisitedCategory").save()