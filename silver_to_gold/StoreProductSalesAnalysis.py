

product_path = "dbfs:/mnt/silver/sales_view/product"
store_path = "dbfs:/mnt/silver/sales_view/store"

product_df = read_delta_file(product_path)
store_df = read_delta_file(store_path)

product_store_df = product_df.join(store_df, "store_id", "inner")\
    .select(
        store_df.store_id,
        "store_name",
        "location",
        "manager_name",
        "product_id",
        "product_name",
        "product_code",
        "description",
        "category_id",
        "price",
        "stock_quantity",
        "supplier_id",
        product_df.created_at.alias("product_created_at"),
        product_df.updated_at.alias("product_updated_at"),
        "image_url",
        "weight",
        "expiry_date",
        "is_active",
        "tax_rate"
    )

customer_sales_path = "dbfs:/mnt/silver/sales_view/customer_sales"
customer_sales_df = read_delta_file(customer_sales_path)

final_df = product_store_df.join(customer_sales_df, "product_id", "inner")\
    .select(
        "OrderDate",
        "Category",
        "City",
        "CustomerID",
        "OrderID",
        product_df.product_id.alias("ProductID"),
        "Profit",
        "Region",
        "Sales",
        "Segment",
        "ShipDate",
        "ShipMode",
        "latitude",
        "longitude",
        "store_name",
        "location",
        "manager_name",
        "product_name",
        "price",
        "stock_quantity",
        "image_url"
    )

output_path = "dbfs:/mnt/gold/sales_view/StoreProductSalesAnalysis"
write_delta_upsert(final_df, output_path)
