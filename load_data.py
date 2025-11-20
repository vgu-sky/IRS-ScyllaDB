
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, trim
from delta import *
from get_schema import get_schema
import os
import shutil

def main():
    builder = SparkSession.builder \
        .appName("Load IMF Data") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    print("Spark session created.")
    
    schema_fields = get_schema()
    print(f"Loaded schema with {len(schema_fields)} fields.")
    
    # Read text file
    # We use read.text to get a dataframe with a single 'value' column containing the line
    df_text = spark.read.text("imf_data.txt")
    
    # Build columns
    select_exprs = []
    current_pos = 1
    
    for name, width, type_char in schema_fields:
        # substring(str, pos, len)
        # Spark substring is 1-based
        
        # Extract the string
        raw_col = substring(col("value"), current_pos, width)
        
        if type_char == 'N':
            # Numeric: The generator uses zfill (zero padding).
            # We can cast directly to Long. Spark handles leading zeros.
            # However, if it's all spaces (unlikely for 'N' in this generator but possible in real data), it might be null.
            # The generator code: if val == "": val = 0. So it should be fine.
            col_expr = raw_col.cast("long").alias(name)
        else:
            # Alpha: The generator uses ljust (space padding on right).
            # We should trim the whitespace.
            col_expr = trim(raw_col).alias(name)
        
        select_exprs.append(col_expr)
        current_pos += width
        
    df_parsed = df_text.select(*select_exprs)
    
    print("Parsed DataFrame Schema:")
    df_parsed.printSchema()
    
    print("Sample Data:")
    df_parsed.show(5)
    
    # Save as Delta
    output_path = "imf_delta_table"
    
    # Clean up previous run if exists (optional, but good for testing)
    # Spark overwrite mode handles data, but sometimes not the directory structure if it's messy.
    # But mode("overwrite") is usually fine.
    
    print(f"Saving to {output_path}...")
    df_parsed.write.format("delta").mode("overwrite").save(output_path)
    print(f"Data successfully saved to {output_path}")

if __name__ == "__main__":
    main()
