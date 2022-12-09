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
                          ecommerce.price, ecommerce.category_code, ecommerce.user_session).cache

# ------------- Creating View ------------------- #

output.createOrReplaceTempView("view_one")

# -------------- Converting to Pandas Dataframe---------- #
outputDf = spark.sql("select user_session, user_id from view_one where event_type = 'purchase'")
pandasDf = outputDf.toPandas()


# -------------- Average Number of Purchases Per Customer -------------- #
purchase_per_customer = pandasDf['user_session'].nunique()/pandasDf['user_id'].nunique()
print(f"The Average number of purchases per customer is {round(purchase_per_customer,2)}!")
