from S3_config import create_spark_session
from pyspark.sql.functions import col, sum, min, max, datediff, lit, round, when, first, desc, count

spark = create_spark_session("StockSense_Inventory_Analysis")

input_path = "s3a://stocksense-data/processed/cleaned/Online Retail Inventory.parquet"
output_path = "s3a://stocksense-data/analysis"

#Read enriched dataset
df = spark.read.parquet(input_path)

#Filter valid sales
sales = df.filter((col("TransactionType") == "SALE") & (col("PriceStatus") == "NORMAL_PRICE") & (col("Quantity") > 0))

#Calculate the observed sales period
period = sales.agg((datediff(max("InvoiceDate"), min("InvoiceDate")) + lit(1)).alias("PeriodDays")).first()["PeriodDays"]

if period is None or period <= 0:
    raise ValueError("Could not determine a valid sales observation period.")

#Calculate units sold per product
sales_by_product = sales.groupBy("StockCode").agg(sum("Quantity").alias("UnitsSold"))

#Keep one inventory record per product
inventory = (df.filter(col("CurrentStock").isNotNull()).groupBy("StockCode").agg(
    first("Description", ignorenulls=True).alias("Description"),
    first("CurrentStock", ignorenulls=True).alias("CurrentStock"),
    first("ReorderLevel", ignorenulls=True).alias("ReorderLevel"),
    first("LeadTimeDays", ignorenulls=True).alias("LeadTimeDays"),
    first("Supplier", ignorenulls=True).alias("Supplier")
))

#Combine sales and inventory information
inventory_analysis = inventory.join(sales_by_product, on="StockCode", how="left").fillna({"UnitsSold": 0})

#Calculate average daily sales and inventory coverage
inventory_analysis = inventory_analysis.withColumn("AvgDailySales", col("UnitsSold") / lit(period))
inventory_analysis = inventory_analysis.withColumn("DaysOfInventory", when(col("AvgDailySales") > 0, col("CurrentStock") / col("AvgDailySales")))
inventory_analysis = inventory_analysis.withColumn("LeadTimeBufferDays", col("DaysOfInventory") - col("LeadTimeDays"))

#Classify inventory risk
inventory_analysis = inventory_analysis.withColumn("StockoutRisk", when(col("CurrentStock") <= 0, "OUT_OF_STOCK").when(col("DaysOfInventory") <= col("LeadTimeDays"), "HIGH_RISK").when(col("CurrentStock") <= col("ReorderLevel"), "REORDER").otherwise("LOW_RISK"))
inventory_analysis = inventory_analysis.withColumn("RecommendedAction", when(col("StockoutRisk") == "OUT_OF_STOCK", "RESTOCK_IMMEDIATELY").when(col("StockoutRisk") == "HIGH_RISK", "RESTOCK_BEFORE_LEAD_TIME").when(col("StockoutRisk") == "REORDER", "PLACE_REORDER").otherwise("NO_ACTION"))

#Round calculated values for reporting
inventory_analysis = (inventory_analysis.withColumn("AvgDailySales", round(col("AvgDailySales"), 2))
    .withColumn("DaysOfInventory", round(col("DaysOfInventory"), 1))
    .withColumn("LeadTimeBufferDays", round(col("LeadTimeBufferDays"), 1)))

#Create risk summary
risk_summary = inventory_analysis.groupBy("StockoutRisk").agg(count("StockCode").alias("Products")).orderBy(desc("Products"))

print("\n========== INVENTORY ANALYSIS OVERVIEW ==========")
print("Sales observation period (days):", period)
print("Products analysed:", inventory_analysis.count())

print("\n========== STOCKOUT RISK SUMMARY ==========")
risk_summary.show(truncate=False)

print("\n========== PRODUCTS REQUIRING ATTENTION ==========")
inventory_analysis.filter(col("StockoutRisk") != "LOW_RISK").orderBy(col("DaysOfInventory").asc_nulls_first()).show(100, truncate=False)

#Save inventory analysis resultss
inventory_analysis.write.mode("overwrite").parquet(f"{output_path}/inventory_risk.parquet")
risk_summary.write.mode("overwrite").parquet(f"{output_path}/inventory_risk_summary.parquet")

print("\nInventory analysis completed successfully.")
print("Saved product analysis to:", f"{output_path}/inventory_risk.parquet")
print("Saved risk summary to:", f"{output_path}/inventory_risk_summary.parquet")

spark.stop()