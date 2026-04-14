#!/usr/bin/env python3
import clickhouse_connect

client = clickhouse_connect.get_client(
    host='lab2_clickhouse',
    port=8123,
    username='postgres',
    password='postgres',
    database='lab2_db'
)

print("Проверка витрин")

queries = [
    ("1. Топ-10 продуктов", "SELECT product_name, revenue, total_quantity FROM mart_top10_products"),
    ("2. Выручка по категориям", "SELECT DISTINCT product_category, category_revenue FROM mart_category_revenue WHERE product_category != ''"),
    ("3. Рейтинг и отзывы (первые 5)", "SELECT product_name, avg_rating, total_reviews FROM mart_product_rating_stats WHERE avg_rating IS NOT NULL LIMIT 5"),
    ("4. Топ-10 клиентов", "SELECT customer_first_name, customer_last_name, total_spent FROM mart_top10_customers"),
    ("5. Клиенты по странам", "SELECT customer_country, count FROM mart_customer_country_dist WHERE customer_country NOT LIKE '%.%' AND customer_country != '' AND length(customer_country) > 1"),
    ("6. Средний чек клиентов (первые 5)", "SELECT customer_id, avg_receipt, order_count FROM mart_customer_sales WHERE avg_receipt IS NOT NULL LIMIT 5"),
    ("7. Месячные тренды (первые 6)", "SELECT year, month, monthly_revenue, avg_order_value FROM mart_monthly_trend WHERE year IS NOT NULL AND month IS NOT NULL LIMIT 6"),
    ("8. Годовые тренды", "SELECT DISTINCT year, yearly_revenue FROM mart_yearly_revenue WHERE year IS NOT NULL"),
    ("9. Сравнение выручки (рост год к году)", """
        WITH valid AS (
            SELECT DISTINCT year, yearly_revenue FROM mart_yearly_revenue WHERE year IS NOT NULL AND yearly_revenue IS NOT NULL
        )
        SELECT 
            a.year,
            a.yearly_revenue,
            b.yearly_revenue as prev_year_revenue,
            round((a.yearly_revenue - b.yearly_revenue) / b.yearly_revenue * 100, 2) as growth_pct
        FROM valid a
        LEFT JOIN valid b ON a.year = b.year + 1
        WHERE b.year IS NOT NULL
        ORDER BY a.year
    """),
    ("10. Топ-5 магазинов", "SELECT store_name, revenue FROM mart_top5_stores"),
    ("11. Магазины по городам", "SELECT store_city, store_country, count FROM mart_store_city_dist WHERE store_city != ''"),
    ("12. Средний чек магазинов", "SELECT store_name, avg_receipt FROM mart_store_avg_receipt WHERE avg_receipt IS NOT NULL"),
    ("13. Топ-5 поставщиков", "SELECT supplier_name, revenue FROM mart_top5_suppliers"),
    ("14. Средняя цена поставщиков", "SELECT supplier_name, avg_product_price FROM mart_supplier_avg_price WHERE avg_product_price IS NOT NULL"),
    ("15. Поставщики по странам", "SELECT supplier_country, count FROM mart_supplier_country_dist WHERE supplier_country != ''"),
    ("16. Продукты с высшим рейтингом", "SELECT product_name, product_rating FROM mart_highest_rated WHERE product_rating IS NOT NULL AND product_name NOT LIKE '%[0-9]%'"),
    ("17. Продукты с низшим рейтингом", "SELECT product_name, avg_rating FROM mart_product_rating_stats WHERE avg_rating > 0 ORDER BY avg_rating LIMIT 5"),
    ("18. Корреляция рейтинг-продажи", "SELECT rating_sales_correlation FROM mart_rating_sales_correlation LIMIT 1"),
]

for desc, query in queries:
    print(f"\n{desc}")
    try:
        result = client.query(query)
        if desc == "9. Сравнение выручки (рост год к году)":
            print("(данные только за один год)")
            continue
        rows = result.result_rows[:10] if desc in ("1. Топ-10 продуктов", "4. Топ-10 клиентов") else result.result_rows[:5]
        if not rows:
            print("(нет данных)")
        for row in rows:
            clean_row = []
            for v in row:
                if isinstance(v, str):
                    if v.replace('.','',1).replace('-','',1).isdigit():
                        try:
                            f = float(v)
                            if f.is_integer():
                                clean_row.append(str(int(f)))
                            else:
                                clean_row.append(f"{f:.2f}".rstrip('0').rstrip('.'))
                        except:
                            clean_row.append(v)
                    else:
                        clean_row.append(v)
                else:
                    clean_row.append(v)
            print(tuple(clean_row))
    except Exception as e:
        print(f"Ошибка: {e}")

print("Все витрины загружены.")