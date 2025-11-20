from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from delta import *
from pyspark.sql import SparkSession
import os
import time

def get_scylla_session():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    return session

def create_schema(session, schema_fields):
    print("Creating keyspace and table...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS irs_data 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    session.set_keyspace('irs_data')
    
    # Build create table statement
    # We need a primary key. The first field is usually a good candidate or a composite.
    # Let's look at the schema. The first field is likely SSN or some ID.
    # Based on previous context, it's IRS data.
    # Let's assume the first column is the primary key for now, or ask the user.
    # Actually, I'll check the schema fields in the code.
    
    columns = []
    for name, width, type_char in schema_fields:
        col_type = 'bigint' if type_char == 'N' else 'text'
        columns.append(f"{name} {col_type}")
    
    # Assuming the first column is the Primary Key. 
    # If it's not unique, we might need a UUID or composite.
    # For IRS master file, SSN (first field usually) is unique.
    pk = schema_fields[0][0]
    
    create_stmt = f"CREATE TABLE IF NOT EXISTS master_file ({', '.join(columns)}, PRIMARY KEY ({pk}))"
    session.execute(create_stmt)
    print("Table created.")

def load_data(spark, session):
    print("Reading data from Delta table...")
    df = spark.read.format("delta").load("imf_delta_table")
    
    # Collect data to driver to insert (for simple local setup)
    # For large data, we would use spark-cassandra-connector, but for this task we use python driver as requested/planned.
    rows = df.collect()
    
    print(f"Loading {len(rows)} rows into ScyllaDB...")
    
    insert_stmt_str = f"INSERT INTO master_file ({', '.join(df.columns)}) VALUES ({', '.join(['?'] * len(df.columns))})"
    prepared = session.prepare(insert_stmt_str)
    
    # Batch insert is not recommended for massive bulk load, but for local testing it's fine.
    # However, unlogged batches or async inserts are better.
    # Let's use execute_async for speed.
    
    futures = []
    for row in rows:
        # Convert row to list/tuple
        values = [row[c] for c in df.columns]
        future = session.execute_async(prepared, values)
        futures.append(future)
        
    # Wait for all
    for f in futures:
        f.result()
        
    print("Data loaded successfully.")

def main():
    # Connect to Scylla
    # Retry a few times if it's still starting
    for i in range(10):
        try:
            session = get_scylla_session()
            break
        except Exception as e:
            print(f"Waiting for ScyllaDB... ({e})")
            time.sleep(5)
    else:
        raise Exception("Could not connect to ScyllaDB")

    # Get schema from the python file
    from get_schema import get_schema
    schema_fields = get_schema()
    
    create_schema(session, schema_fields)
    
    # Spark session to read Delta
    builder = SparkSession.builder \
        .appName("Load to Scylla") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g")
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    load_data(spark, session)
    
    session.shutdown()
    spark.stop()

if __name__ == "__main__":
    main()
