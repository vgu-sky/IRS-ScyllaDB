# IRS Mainframe to ScyllaDB Pipeline

This project demonstrates a data pipeline that processes simulated IRS Mainframe data (fixed-width text files), transforms it using Apache Spark (Delta Lake), and loads it into ScyllaDB for high-performance serving.

## Architecture

The pipeline follows this flow:
1.  **Generate Data**: Simulates Mainframe fixed-width output (`imf_data.txt`).
2.  **Process Data**: Uses Spark to parse the fixed-width file based on a COBOL-like schema and saves it to a Delta Lake table.
3.  **Load to ScyllaDB**: Reads the Delta table and inserts records into a local ScyllaDB instance.

See [architecture.md](architecture.md) for a detailed diagram and explanation.

## Prerequisites

*   **Docker**: Required to run the ScyllaDB container.
*   **Python 3.8+**: Required for the scripts.
*   **Java 8/11**: Required for Apache Spark.

## Setup & Usage

### 1. Install Dependencies

Create a virtual environment and install the required Python packages:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start ScyllaDB

Run ScyllaDB in a Docker container:

```bash
docker run --name scylla -d -p 9042:9042 scylladb/scylla
```

Wait a minute for the node to be ready. You can check status with `docker logs scylla`.

### 3. Generate Dummy Data

Generate the simulated IRS Master File data:

```bash
python generate_imf_data.py
```
This creates `imf_data.txt` (Fixed Width) and `imf_data.csv` (for reference).

### 4. Process Data (Spark -> Delta Lake)

Parse the text file and save it as a Delta table:

```bash
python load_data.py
```
This creates the `imf_delta_table` directory.

### 5. Load Data to ScyllaDB

Load the processed data from Delta Lake into ScyllaDB:

```bash
python load_to_scylla.py
```

### 6. Verify Data

Check that the data exists in ScyllaDB:

```bash
python verify_scylla_data.py
```

## Connecting with a GUI Client

You can connect to the database using clients like **DBeaver**:

*   **Host**: `localhost`
*   **Port**: `9042`
*   **Keyspace**: `irs_data`
*   **Username/Password**: (Leave empty)

## Files

*   `generate_imf_data.py`: Generates dummy taxpayer data.
*   `get_schema.py`: Defines the schema for the fixed-width file.
*   `load_data.py`: Spark script to parse text and save to Delta.
*   `load_to_scylla.py`: Script to load Delta data into ScyllaDB.
*   `verify_scylla_data.py`: Verifies the row count in ScyllaDB.
*   `architecture.md`: System architecture documentation.
