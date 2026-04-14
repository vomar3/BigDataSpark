from pyspark.sql import SparkSession, functions as F
import clickhouse_connect

spark = SparkSession.builder \
    .appName("Build Marts ClickHouse") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

pg_url = "jdbc:postgresql://lab2_postgres:5432/lab2_db"
pg_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

sale_df = spark.read.jdbc(pg_url, "sale", properties=pg_props)
product_df = spark.read.jdbc(pg_url, "product", properties=pg_props)
customer_df = spark.read.jdbc(pg_url, "customer", properties=pg_props)
store_df = spark.read.jdbc(pg_url, "store", properties=pg_props)
supplier_df = spark.read.jdbc(pg_url, "supplier", properties=pg_props)

product_sales = sale_df.groupBy("product_id").agg(
    F.sum("total_price").alias("revenue"),
    F.sum("quantity").alias("total_quantity")
).join(product_df, "product_id")

top10_products = product_sales.orderBy(F.desc("revenue")).limit(10)
category_revenue = product_sales.groupBy("product_category").agg(F.sum("revenue").alias("category_revenue"))
product_rating_stats = product_df.groupBy("product_id", "product_name").agg(
    F.avg("product_rating").alias("avg_rating"),
    F.sum("product_reviews").alias("total_reviews")
)

customer_sales = sale_df.groupBy("customer_id").agg(
    F.sum("total_price").alias("total_spent"),
    F.count("*").alias("order_count"),
    F.avg("total_price").alias("avg_receipt")
).join(customer_df, "customer_id")

top10_customers = customer_sales.orderBy(F.desc("total_spent")).limit(10)
country_dist = customer_df.groupBy("customer_country").count()

time_sales = sale_df.withColumn("year", F.year("sale_date")).withColumn("month", F.month("sale_date"))
monthly_trend = time_sales.groupBy("year", "month").agg(
    F.sum("total_price").alias("monthly_revenue"),
    F.avg("total_price").alias("avg_order_value")
).orderBy("year", "month")
yearly_revenue = time_sales.groupBy("year").agg(F.sum("total_price").alias("yearly_revenue"))

store_sales = sale_df.groupBy("store_id").agg(
    F.sum("total_price").alias("revenue"),
    F.avg("total_price").alias("avg_receipt")
).join(store_df, "store_id")

top5_stores = store_sales.orderBy(F.desc("revenue")).limit(5)
store_city_dist = store_df.groupBy("store_city", "store_country").count()
store_avg_receipt_all = store_sales.select("store_id", "avg_receipt").join(store_df, "store_id")

supplier_sales = sale_df.join(product_df, "product_id") \
    .groupBy("supplier_id").agg(
        F.sum("total_price").alias("revenue"),
        F.avg("product_price").alias("avg_product_price")
    ).join(supplier_df, "supplier_id")

top5_suppliers = supplier_sales.orderBy(F.desc("revenue")).limit(5)
supplier_country_dist = supplier_df.groupBy("supplier_country").count()
supplier_avg_price_all = supplier_sales.select("supplier_id", "avg_product_price").join(supplier_df, "supplier_id")

product_quality = product_df.select("product_id", "product_name", "product_rating", "product_reviews") \
    .join(sale_df.groupBy("product_id").agg(F.sum("quantity").alias("total_sold")), "product_id", "left") \
    .fillna(0, subset=["total_sold"])

highest_rated = product_quality.orderBy(F.desc("product_rating")).limit(5)
lowest_rated = product_quality.orderBy("product_rating").limit(5)
most_reviewed = product_quality.orderBy(F.desc("product_reviews")).limit(5)
correlation = product_quality.stat.corr("product_rating", "total_sold")

client = clickhouse_connect.get_client(
    host='lab2_clickhouse',
    port=8123,
    username='postgres',
    password='postgres',
    database='lab2_db'
)

def get_clickhouse_type(spark_type, nullable=False):
    t = spark_type.typeName()
    base = 'String'
    if t in ('string',):
        base = 'String'
    elif t in ('int', 'integer', 'short', 'byte'):
        base = 'Int32'
    elif t in ('long', 'bigint'):
        base = 'Int64'
    elif t in ('float',):
        base = 'Float32'
    elif t in ('double',):
        base = 'Float64'
    elif t in ('date',):
        base = 'Date'
    elif t in ('timestamp',):
        base = 'DateTime'
    return f"Nullable({base})" if nullable else base

def write_ch(df, table_name):
    cols = []
    for field in df.schema.fields:
        nullable = True  # разрешаем NULL для всех
        col_type = get_clickhouse_type(field.dataType, nullable)
        cols.append(f"`{field.name}` {col_type}")
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)}) ENGINE = MergeTree() ORDER BY tuple()"
    client.command(ddl)

    data = df.collect()
    if not data:
        return
    columns = df.columns
    rows = []
    for row in data:
        r = []
        for i, v in enumerate(row):
            if v is None:
                r.append(None)
            elif isinstance(v, float) and (v != v):  # NaN
                r.append(0.0)
            else:
                r.append(v)
        rows.append(tuple(r))
    client.insert(table_name, rows, column_names=columns)

write_ch(top10_products, "mart_top10_products")
write_ch(category_revenue, "mart_category_revenue")
write_ch(product_rating_stats, "mart_product_rating_stats")
write_ch(top10_customers, "mart_top10_customers")
write_ch(country_dist, "mart_customer_country_dist")
write_ch(customer_sales.select("customer_id", "total_spent", "order_count", "avg_receipt"), "mart_customer_sales")
write_ch(monthly_trend, "mart_monthly_trend")
write_ch(yearly_revenue, "mart_yearly_revenue")
write_ch(top5_stores, "mart_top5_stores")
write_ch(store_city_dist, "mart_store_city_dist")
write_ch(store_avg_receipt_all, "mart_store_avg_receipt")
write_ch(top5_suppliers, "mart_top5_suppliers")
write_ch(supplier_country_dist, "mart_supplier_country_dist")
write_ch(supplier_avg_price_all, "mart_supplier_avg_price")
write_ch(highest_rated, "mart_highest_rated")
write_ch(lowest_rated, "mart_lowest_rated")
write_ch(most_reviewed, "mart_most_reviewed")

corr_val = correlation if correlation is not None else 0.0
corr_df = spark.createDataFrame([(corr_val,)], ["rating_sales_correlation"])
write_ch(corr_df, "mart_rating_sales_correlation")

print("All marts written to ClickHouse.")
spark.stop()