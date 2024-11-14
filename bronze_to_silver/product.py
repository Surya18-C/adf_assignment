# Import necessary PySpark functions
from pyspark.sql.functions import when, col

product_df = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/Bronze/sales_view/product/20240107_sales_product.csv")

snake_case_product_df = toSnakeCase(product_df)

sub_category_df = snake_case_product_df.withColumn(
    "sub_category",
    when(col("category_id") == 1, "phone")
    .when(col("category_id") == 2, "laptop")
    .when(col("category_id") == 3, "playstation")
    .when(col("category_id") == 4, "e-device")
)

write_path = "dbfs:/mnt/silver/sales_view/product"
write_delta_upsert(sub_category_df, write_path)
