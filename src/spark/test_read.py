from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StockSense") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    r"C:\Users\slanao\Desktop\Big_Data_Ecosystems_SvenLanao\Data\processed\Online Retail.csv",
    header=True,
    inferSchema=True,
    sep=";"
)

print("Total rows:", df.count())
df.printSchema()
df.show(5)

spark.stop()