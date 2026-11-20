
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, trim, upper, when, sum as spark_sum, countDistinct, min as spark_min)

spark = (SparkSession.builder.appName("StockSense_Data_Profiling").master("local[*]").getOrCreate())

#Read source CSV
file_path = r"C:\Users\slanao\Desktop\Big_Data_Ecosystems_SvenLanao\Data\processed\Online Retail.csv"

df = spark.read.csv(file_path,header=True,inferSchema=True,sep=";")

#Dataset overview
total_rows = df.count()
distinct_rows = df.dropDuplicates().count()

print("\n========== DATASET OVERVIEW ==========")
print("Total rows:", total_rows)
print("Distinct rows:", distinct_rows)
print("Exact duplicate rows:", total_rows - distinct_rows)
print("Distinct invoices:", df.select("InvoiceNo").distinct().count())

#Data quality report
is_cancelled = upper(trim(col("InvoiceNo"))).startswith("C")
is_missing_description = (col("Description").isNull() | (trim(col("Description")) == ""))

quality_report = df.agg(
    spark_sum(when(col("CustomerID").isNull(), 1).otherwise(0)).alias("Missing CustomerID"),
    spark_sum(when(is_missing_description, 1).otherwise(0)).alias("Missing Description"),
    spark_sum(when(col("Quantity") < 0, 1).otherwise(0)).alias("Negative Quantity"),
    spark_sum(when(col("UnitPrice") == 0, 1).otherwise(0)).alias("Zero UnitPrice"),
    spark_sum(when(col("UnitPrice") < 0, 1).otherwise(0)).alias("Negative UnitPrice"),
    spark_sum(when(is_cancelled, 1).otherwise(0)).alias("Cancelled Invoices"),
    spark_sum(when((col("Quantity") < 0) & is_cancelled, 1).otherwise(0)).alias("Negative Quantity and Cancellation"))

print("\n========== DATA QUALITY REPORT ==========")
quality_report.show(truncate=False)

#Duplicate analysis
duplicate_rows = (df.groupBy(df.columns).count().filter(col("count") > 1))

print("\n========== DUPLICATE ANALYSIS ==========")
print("Invoices containing exact duplicate rows:",duplicate_rows.select("InvoiceNo").distinct().count())

#Description recovery analysis
missing_descriptions = df.filter(is_missing_description)

description_consistency = (df.filter(~is_missing_description).groupBy("StockCode").agg(countDistinct("Description").alias("description_count"),spark_min("Description").alias("single_description")))

safe_descriptions = (description_consistency.filter(col("description_count") == 1).select("StockCode",col("single_description").alias("Recovered_Description")))

ambiguous_stockcodes = (description_consistency.filter(col("description_count") > 1).select("StockCode"))

safe_recoverable = missing_descriptions.join(safe_descriptions, on="StockCode", how="inner")

ambiguous_records = missing_descriptions.join(ambiguous_stockcodes, on="StockCode", how="inner")

print("\n========== DESCRIPTION RECOVERY ANALYSIS ==========")
print("Missing-description records:", missing_descriptions.count())
print("Records safely recoverable:", safe_recoverable.count())
print("Records with ambiguous descriptions:", ambiguous_records.count())
print("Records without an available description:",missing_descriptions.count()- safe_recoverable.count()- ambiguous_records.count())

spark.stop()