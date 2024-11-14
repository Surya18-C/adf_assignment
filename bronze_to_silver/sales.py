
raw_sales_df = spark.read.csv('dbfs:/mnt/Bronze/sales_view/sales/20240107_sales_data.csv', header=True, inferSchema=True)



renamed_sales_df = toSnakeCase(raw_sales_df)



writeTo = f'dbfs:/mnt/silver/sales_view/customer_sales'
write_delta_upsert(renamed_sales_df, writeTo)



