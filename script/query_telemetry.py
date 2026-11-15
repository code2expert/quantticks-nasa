import os
import duckdb
from dotenv import load_dotenv

# Load R2 credentials
load_dotenv()

R2_ENDPOINT = f"https://{os.environ.get('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com"
ACCESS_KEY = os.environ.get('R2_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
BUCKET_NAME = "quantticks-nasa-parquet"

def get_duckdb_connection():
    """Initializes DuckDB and configures S3/R2 secret parameters."""
    conn = duckdb.connect(database=':memory:')
    
    # Install and load spatial/httpfs extensions if needed
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    
    # Configure S3 connection settings for Cloudflare R2
    conn.execute(f"SET s3_endpoint='{R2_ENDPOINT.replace('https://', '')}';")
    conn.execute(f"SET s3_access_key_id='{ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{SECRET_KEY}';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl=true;")
    
    return conn

def query_active_fires_summary():
    """Queries all Parquet telemetry chunks in R2 and returns aggregated wildfire stats."""
    conn = get_duckdb_connection()
    
    parquet_glob = f"s3://{BUCKET_NAME}/telemetry/*.parquet"
    
    query = f"""
        SELECT 
            region,
            sensor_type,
            COUNT(*) as total_detections,
            ROUND(AVG(confidence), 2) as avg_confidence,
            ROUND(MAX(frp), 2) as max_frp,
            MIN(source_timestamp) as earliest_source_time,
            MAX(source_timestamp) as latest_source_time
        FROM read_parquet('{parquet_glob}')
        GROUP BY region, sensor_type
        ORDER BY total_detections DESC;
    """
    
    print("Executing analytical query against R2 Parquet chunks via DuckDB...\n")
    result = conn.execute(query).fetchdf()
    print(result)
    conn.close()

if __name__ == "__main__":
    query_active_fires_summary()