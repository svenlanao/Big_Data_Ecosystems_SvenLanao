
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,trim,upper,when,countDistinct,min as spark_min,coalesce,to_timestamp)
import os

spark = SparkSession.builder.appName("StockSense_Data_Cleaning").master("local[*]").getOrCreate()

base_path = "/workspace"
input_path = os.path.join(base_path, "Data", "processed", "Online Retail Inventory.csv")
output_path = os.path.join(base_path, "Data", "processed", "cleaned", "Online Retail Inventory.parquet")

#Read source CSV
df = spark.read.csv(input_path, header=True, inferSchema=True, sep=";").cache()
rows_before = df.count()

print("\n========== SOURCE DATA ==========")
print("Rows before cleaning:", rows_before)

#Remove exact duplicate rows
df_clean = df.dropDuplicates()
rows_after_deduplication = df_clean.count()
duplicates_removed = rows_before - rows_after_deduplication

#Recover descriptions only when StockCode has one unique description
missing_description = col("Description").isNull() | (trim(col("Description")) == "")

description_mapping = (df_clean.filter(~missing_description).groupBy("StockCode").agg(countDistinct("Description").alias("description_count"),
spark_min("Description").alias("recovered_description")).filter(col("description_count") == 1).select("StockCode", "recovered_description"))

df_clean = (df_clean.join(description_mapping, on="StockCode", how="left").withColumn("Description",when(missing_description, coalesce(col("recovered_description"), 
col("Description"))).otherwise(col("Description"))).drop("recovered_description"))

#Convert InvoiceDate to timestamp
df_clean = df_clean.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "d/M/yyyy H:mm"))

#Classify transactions
is_cancelled = upper(trim(col("InvoiceNo"))).startswith("C")

df_clean = df_clean.withColumn("TransactionType",when(is_cancelled, "CANCELLED").when(col("Quantity") < 0, "RETURN").otherwise("SALE"))

#Classify price values
df_clean = df_clean.withColumn("PriceStatus",when(col("UnitPrice") < 0, "NEGATIVE_PRICE").when(col("UnitPrice") == 0, "ZERO_PRICE").otherwise("NORMAL_PRICE"))

#Generate cleaning report
rows_final = df_clean.count()

remaining_missing_descriptions = df_clean.filter(col("Description").isNull() | (trim(col("Description")) == "")).count()

print("\n========== CLEANING REPORT ==========")
print("Rows before cleaning:", rows_before)
print("Rows after deduplication:", rows_after_deduplication)
print("Exact duplicates removed:", duplicates_removed)
print("Rows after cleaning:", rows_final)
print("Remaining missing descriptions:", remaining_missing_descriptions)

print("\n========== TRANSACTION CLASSIFICATION ==========")
df_clean.groupBy("TransactionType").count().show()

print("\n========== PRICE CLASSIFICATION ==========")
df_clean.groupBy("PriceStatus").count().show()

#Save cleaned dataset as Parquet
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df_clean.write.mode("overwrite").parquet(output_path)

print("\n========== PARQUET OUTPUT ==========")
print("Saved to:", output_path)

#Verify Parquet output
parquet_df = spark.read.parquet(output_path)
print("Parquet rows:", parquet_df.count())
parquet_df.printSchema()
parquet_df.show(5, truncate=False)

df.unpersist()
spark.stop()

print("\nData cleaning completed successfully.")