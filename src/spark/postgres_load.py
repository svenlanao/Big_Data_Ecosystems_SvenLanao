from S3_config import create_spark_session
import os

spark = create_spark_session("StockSense_Postgres_Load")

#SILO paths
inventory_path = "s3a://stocksense-data/analysis/inventory_risk.parquet"
summary_path = "s3a://stocksense-data/analysis/inventory_risk_summary.parquet"

#PostgreSQL connection
jdbc_url = "jdbc:postgresql://postgres:5432/stocksense"

properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

#Read inventory analysis from SILO
inventory_risk = spark.read.parquet(inventory_path)
risk_summary = spark.read.parquet(summary_path)

print("\n========== DATA TO LOAD ==========")
print("Inventory risk rows:", inventory_risk.count())
print("Risk summary rows:", risk_summary.count())

#Load inventory risk into PostgreSQL
inventory_risk.write.jdbc(url=jdbc_url, table="inventory_risk", mode="overwrite", properties=properties)

#Load risk summary into PostgreSQL
risk_summary.write.jdbc(url=jdbc_url, table="inventory_risk_summary", mode="overwrite", properties=properties)

print("\nPostgreSQL loading completed successfully.")

spark.stop()