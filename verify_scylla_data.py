from cassandra.cluster import Cluster
import pandas as pd

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('irs_data')
    
    print("Counting rows in master_file...")
    row = session.execute("SELECT count(*) FROM master_file").one()
    count = row[0]
    print(f"Total rows in ScyllaDB: {count}")
    
    print("\nSample data (first 5 rows):")
    rows = session.execute("SELECT * FROM master_file LIMIT 5")
    for r in rows:
        print(r)
        
    cluster.shutdown()

if __name__ == "__main__":
    main()
