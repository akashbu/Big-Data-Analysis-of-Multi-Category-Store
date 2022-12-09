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


# ------------- Creating View ------------------- #

view1 = ecommerce.select(ecommerce.event_type, ecommerce.product_id, ecommerce.user_id, ecommerce.brand, ecommerce.category_code).cache()
view1.createOrReplaceTempView("eco_view")

# ---------------------------------------------- Smartphone View Analysis -------------------------------------------------------------- #

outputDf = spark.sql("select brand, count(event_type) as views from eco_view where category_code = 'electronics.smartphone' and event_type = 'view' group by brand order by views desc  ")
outputDf.show()



outputDf.repartition(1).write.format("csv").mode("overwrite").option("path", "s3://sparkproject02/output/SmartphoneViewAnalysis").save()