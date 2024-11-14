from pyspark.sql.functions import expr, regexp_extract, lit, date_format

customer_df = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/Bronze/sales_view/customer/20240107_sales_customer.csv")

snake_case_df = toSnakeCase(customer_df)

name_split_df = snake_case_df.withColumn("first_name", expr("split(name, ' ')[0]"))\
                             .withColumn("last_name", expr("split(name, ' ')[1]")).drop("name")

domain_extracted_df = name_split_df.withColumn("domain", regexp_extract("email_id", "@([a-zA-Z]+)\\.", 1)).drop("email_id")

gender_converted_df = domain_extracted_df.withColumn("gender", expr("CASE WHEN gender = 'male' THEN 'M' ELSE 'F' END"))

date_split_df = gender_converted_df.withColumn("date", expr("split(joining_date, ' ')[0]"))\
                                   .withColumn("time", expr("split(joining_date, ' ')[1]")).drop("joining_date")

formatted_date_df = date_split_df.withColumn("date", date_format("date", "yyyy-MM-dd"))

expenditure_status_df = formatted_date_df.withColumn("expenditure_status", expr("CASE WHEN spent < 200 THEN 'MINIMUM' ELSE 'MAXIMUM' END"))

output_path = "dbfs:/mnt/silver/sales_view/customer"
expenditure_status_df.write.format("delta").mode("overwrite").save(output_path)
