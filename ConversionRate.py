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
                       StructField("price", IntegerType(), True),
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

output = ecommerce.select(ecommerce.event_type, ecommerce.product_id, ecommerce.user_id, ecommerce.brand, ecommerce.category_code).cache()

# ------------- Creating View ------------------- #

output.createOrReplaceTempView("view_one")

outputDf = spark.sql("select event_type from view_one")

# -------------- Converting to Pandas Dataframe---------- #

pandasDf = outputDf.toPandas()

# -------------- Conversion Rate -------------- #

cart_count = pandasDf['event_type'].value_counts()['cart']
view_count = pandasDf['event_type'].value_counts()['view']
purchase_count = pandasDf['event_type'].value_counts()['purchase']

print("Rate of conversion between view and purchase events " + str((purchase_count/view_count)*100) + '%')
print("Rate of conversion between view and add to cart events " + str((cart_count/view_count)*100) + '%')
print("Rate of conversion between add to cart and purchase events " + str((purchase_count/cart_count)*100) + '%')

