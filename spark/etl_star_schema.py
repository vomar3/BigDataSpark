from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, to_date

spark = SparkSession.builder \
    .appName("ETL Star Schema") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("/opt/spark/data/mock_data.csv")

df = df.withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy")) \
       .withColumn("product_release_date", to_date(col("product_release_date"), "M/d/yyyy")) \
       .withColumn("product_expiry_date", to_date(col("product_expiry_date"), "M/d/yyyy"))

df = df.dropna(subset=["customer_first_name", "customer_last_name", "customer_email", "product_name"])

customer_df = df.select(
    "customer_first_name", "customer_last_name", "customer_age", "customer_email",
    "customer_country", "customer_postal_code"
).distinct().withColumn("customer_id", monotonically_increasing_id())

seller_df = df.select(
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code"
).distinct().withColumn("seller_id", monotonically_increasing_id())

supplier_df = df.select(
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
    "supplier_address", "supplier_city", "supplier_country"
).distinct().withColumn("supplier_id", monotonically_increasing_id())

store_df = df.select(
    "store_name", "store_location", "store_city", "store_state",
    "store_country", "store_phone", "store_email"
).distinct().withColumn("store_id", monotonically_increasing_id())

product_raw_df = df.select(
    "product_name", "product_category", "product_price", "product_quantity",
    "product_weight", "product_color", "product_size", "product_brand",
    "product_material", "product_description", "product_rating", "product_reviews",
    "product_release_date", "product_expiry_date",
    "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
    "supplier_address", "supplier_city", "supplier_country"
).distinct()

product_df = product_raw_df.join(
    supplier_df,
    on=["supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
        "supplier_address", "supplier_city", "supplier_country"],
    how="left"
).select(
    "product_name", "product_category", "product_price", "product_quantity",
    "product_weight", "product_color", "product_size", "product_brand",
    "product_material", "product_description", "product_rating", "product_reviews",
    "product_release_date", "product_expiry_date",
    "supplier_id"                           # только внешний ключ
).distinct().withColumn("product_id", monotonically_increasing_id())

sale_fact = df \
    .join(customer_df, on=["customer_first_name", "customer_last_name", "customer_age",
                           "customer_email", "customer_country", "customer_postal_code"], how="left") \
    .join(seller_df, on=["seller_first_name", "seller_last_name", "seller_email",
                         "seller_country", "seller_postal_code"], how="left") \
    .join(store_df, on=["store_name", "store_location", "store_city", "store_state",
                        "store_country", "store_phone", "store_email"], how="left") \
    .join(product_df, on=[
        "product_name", "product_category", "product_price", "product_quantity",
        "product_weight", "product_color", "product_size", "product_brand",
        "product_material", "product_description", "product_rating", "product_reviews",
        "product_release_date", "product_expiry_date"
    ], how="left") \
    .select(
        col("sale_date"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    ).withColumn("sale_id", monotonically_increasing_id())

pg_url = "jdbc:postgresql://lab2_postgres:5432/lab2_db"
pg_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

customer_df.write.jdbc(pg_url, "customer", mode="overwrite", properties=pg_props)
seller_df.write.jdbc(pg_url, "seller", mode="overwrite", properties=pg_props)
supplier_df.write.jdbc(pg_url, "supplier", mode="overwrite", properties=pg_props)
store_df.write.jdbc(pg_url, "store", mode="overwrite", properties=pg_props)
product_df.write.jdbc(pg_url, "product", mode="overwrite", properties=pg_props)
sale_fact.write.jdbc(pg_url, "sale", mode="overwrite", properties=pg_props)

print("ETL Star Schema completed.")
spark.stop()