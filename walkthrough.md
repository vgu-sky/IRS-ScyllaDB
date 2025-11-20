# ScyllaDB Installation and Data Loading Walkthrough

## Overview
This walkthrough documents the steps taken to install ScyllaDB locally using Docker and load the IRS Master File data from a local Delta table.

## Prerequisites
- Docker installed and running.
- Python environment with `pyspark` and `delta-spark`.

## Steps

### 1. Start ScyllaDB Docker Container
We used the official ScyllaDB Docker image to run a single node.
```bash
docker run --name scylla -d -p 9042:9042 scylladb/scylla
```

### 2. Install Python Driver
We installed the `cassandra-driver` which is compatible with ScyllaDB.
```bash
pip install cassandra-driver
```

### 3. Load Data Script
We created a script `load_to_scylla.py` that:
1. Connects to the local ScyllaDB instance.
2. Creates a keyspace `irs_data` and table `master_file`.
3. Reads data from the local Delta table `imf_delta_table` using Spark.
4. Inserts the data into ScyllaDB.

### 4. Verification
We verified the data load using `verify_scylla_data.py`.
- **Source Data**: `imf_data.txt` (250 lines)
- **ScyllaDB Count**: 250 rows

## Results
The data was successfully loaded into ScyllaDB. You can now query the `irs_data.master_file` table.
