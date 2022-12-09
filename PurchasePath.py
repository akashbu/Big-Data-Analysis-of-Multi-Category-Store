from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# ------------------ Schema Definition ----------------------------------------- #

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
    .option("path", "/Users/csuftitan/PycharmProjects/SparkProject1/2019-Nov.csv")\
    .load()

ecommerce.printSchema()

output = ecommerce.select(ecommerce.event_time, ecommerce.event_type,
                          ecommerce.product_id, ecommerce.brand, ecommerce.category_code, ecommerce.price,
                          ecommerce.user_id, ecommerce.user_session ).cache()

# ------------- Creating View ------------------- #

output.createOrReplaceTempView("view_one")

# -------------- Purchase Path -------------- #

outputDf = spark.sql("select * from view_one where user_session = 'ef3daa59-4936-43e5-a530-32902f64b2f4'")

outputDf.show()

outputDf.write.format("csv").mode("overwrite").option("path", "/Users/csuftitan/PycharmProjects/SparkProject1/output/PurchasePath").save()