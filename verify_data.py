
from pyspark.sql import SparkSession
from delta import *

def main():
    builder = SparkSession.builder \
        .appName("Verify IMF Data") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    df = spark.read.format("delta").load("imf_delta_table")
    
    print(f"Record Count: {df.count()}")
    print("Schema:")
    df.printSchema()
    print("Sample Data:")
    df.show(5)

if __name__ == "__main__":
    main()
