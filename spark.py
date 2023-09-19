from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark session creation
spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

# Df creation
products_df = spark.createDataFrame([
    (1, "ProductA"),
    (2, "ProductB"),
    (3, "ProductC"),
], ["product_id", "product_name"])

categories_df = spark.createDataFrame([
    (1, "CategoryX"),
    (2, "CategoryY"),
    (3, "CategoryZ"),
], ["category_id", "category_name"])

# Df and products initialization
product_category_df = spark.createDataFrame([
    (1, 1),
    (1, 2),
    (2, 2),
    (3, 3),
], ["product_id", "category_id"])

# Left join 
result_df = products_df.join(product_category_df, "product_id", "left") \
    .join(categories_df, "category_id", "left") \
    .select("product_name", "category_name")

# Print result
result_df.show()
