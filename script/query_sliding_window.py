import os
import duckdb
from dotenv import load_dotenv
import boto3

load_dotenv()

R2_ENDPOINT = f"https://{os.environ.get('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com"
BUCKET_NAME = "quantticks-nasa-parquet"

s3_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=os.environ.get('R2_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('R2_SECRET_ACCESS_KEY'),
    region_name='auto'
)

def get_quota_bounded_telemetry(quota_mb=10):
    """
    Walks backward from the newest Parquet chunks in R2, accumulating file sizes 
    until the cumulative quota (e.g., 10 MB demo / 100 MB production) is reached.
    Returns the list of matching S3 URLs to query via DuckDB.
    """
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="telemetry/")
    if "Contents" not in response:
        print("No telemetry chunks found.")
        return []

    # Sort newest to oldest based on LastModified
    objects = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    
    quota_bytes = quota_mb * 1024 * 1024
    accumulated_size = 0
    selected_files = []

    print(f"Calculating sliding window (Target Quota: {quota_mb} MB)...")
    for obj in objects:
        if accumulated_size + obj["Size"] <= quota_bytes:
            s3_url = f"s3://{BUCKET_NAME}/{obj['Key']}"
            selected_files.append(s3_url)
            accumulated_size += obj["Size"]
            print(f"  [Included] {obj['Key']} ({obj['Size'] / 1024:.2f} KB) - Cumulative: {accumulated_size / (1024*1024):.2f} MB")
        else:
            print(f"  [Quota Reached] Skipping older chunk: {obj['Key']}")
            break

    return selected_files

def query_sliding_quota_data(quota_mb=10):
    """Queries only the chunks that fit within the rolling quota sliding window."""
    files = get_quota_bounded_telemetry(quota_mb)
    if not files:
        return

    conn = duckdb.connect(database=':memory:')
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{R2_ENDPOINT.replace('https://', '')}';")
    conn.execute(f"SET s3_access_key_id='{os.environ.get('R2_ACCESS_KEY_ID')}';")
    conn.execute(f"SET s3_secret_access_key='{os.environ.get('R2_SECRET_ACCESS_KEY')}';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl=true;")

    # Construct a DuckDB read list from our exact quota-bounded files
    files_str = str(files).replace("'", "'") # Format as array string for DuckDB
    query = f"""
        SELECT 
            region,
            sensor_type,
            COUNT(*) as total_detections,
            MIN(source_timestamp) as window_start_time,
            MAX(source_timestamp) as window_end_time
        FROM read_parquet({files})
        GROUP BY region, sensor_type;
    """
    
    print("\nExecuting Sliding Quota Analytical Query...")
    df = conn.execute(query).fetchdf()
    print(df)
    conn.close()

if __name__ == "__main__":
    # Test with a 10MB quota demo window
    query_sliding_quota_data(quota_mb=10)