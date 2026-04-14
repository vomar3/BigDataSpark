#!/bin/bash
set -e

echo "Waiting for services to be ready..."
sleep 15

echo "Running ETL (this may take a minute)..."
/opt/spark/bin/spark-submit /opt/spark/work-dir/etl_star_schema.py > /dev/null 2>&1
/opt/spark/bin/spark-submit /opt/spark/work-dir/build_marts.py > /dev/null 2>&1

echo ""
echo "отчёты"
python3 /opt/spark/work-dir/check_results.py