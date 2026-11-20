from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, date_format, desc, round
import os

spark = SparkSession.builder.appName("StockSense_Sales_Analysis").master("local[*]").getOrCreate()

base_path = "/workspace"
input_path = os.path.join(base_path, "Data", "processed", "cleaned", "Online Retail.parquet")
output_path = os.path.join(base_path, "Data", "processed", "analysis")

#Read cleaned dataset
df = spark.read.parquet(input_path)

#Filter valid sales
sales = df.filter((col("TransactionType") == "SALE") & (col("PriceStatus") == "NORMAL_PRICE") & (col("Quantity") > 0))

#Calculate revenue
sales = sales.withColumn("Revenue", round(col("Quantity") * col("UnitPrice"), 2))

#Sales overview
print("\n========== SALES OVERVIEW ==========")
sales.agg(countDistinct("InvoiceNo").alias("Total Invoices"), sum("Quantity").alias("Units Sold"), round(sum("Revenue"), 2).alias("Total Revenue")).show()

#Monthly sales
monthly_sales = sales.withColumn("Month", date_format(col("InvoiceDate"), "yyyy-MM")).groupBy("Month").agg(sum("Quantity").alias("Units Sold"), round(sum("Revenue"), 2).alias("Revenue")).orderBy("Month")

print("\n========== MONTHLY SALES ==========")
monthly_sales.show(100, truncate=False)

#Top 10 products by revenue
top_products = sales.groupBy("StockCode", "Description").agg(sum("Quantity").alias("Units Sold"), round(sum("Revenue"), 2).alias("Revenue")).orderBy(desc("Revenue")).limit(10)

print("\n========== TOP 10 PRODUCTS ==========")
top_products.show(10, truncate=False)

#Sales by country
country_sales = sales.groupBy("Country").agg(sum("Quantity").alias("Units Sold"), round(sum("Revenue"), 2).alias("Revenue"), countDistinct("InvoiceNo").alias("Invoices")).orderBy(desc("Revenue"))

print("\n========== SALES BY COUNTRY ==========")
country_sales.show(100, truncate=False)

#Save analysis results
os.makedirs(output_path, exist_ok=True)
monthly_sales.write.mode("overwrite").parquet(os.path.join(output_path, "monthly_sales.parquet"))
top_products.write.mode("overwrite").parquet(os.path.join(output_path, "top_products.parquet"))
country_sales.write.mode("overwrite").parquet(os.path.join(output_path, "country_sales.parquet"))

print("\nSales analysis completed successfully.")

spark.stop()