import os
import json
import duckdb
from dotenv import load_dotenv
import boto3

load_dotenv()

R2_ENDPOINT = f"https://{os.environ.get('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com"
ACCESS_KEY = os.environ.get('R2_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
BUCKET_NAME = "quantticks-nasa-parquet"

s3_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name='auto'
)

def get_quota_bounded_parquet_files(quota_mb=10):
    """
    Walks backward from newest global chunks in R2 until the quota limit is reached.
    """
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="telemetry/")
    if "Contents" not in response:
        return []

    objects = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    
    quota_bytes = quota_mb * 1024 * 1024
    accumulated_size = 0
    selected_files = []

    print(f"Calculating global sliding window quota (Target: {quota_mb} MB)...")
    for obj in objects:
        if accumulated_size + obj["Size"] <= quota_bytes:
            selected_files.append(f"s3://{BUCKET_NAME}/{obj['Key']}")
            accumulated_size += obj["Size"]
        else:
            break

    print(f"Selected {len(selected_files)} global chunks totaling {accumulated_size / (1024*1024):.2f} MB.")
    return selected_files

def get_duckdb_connection():
    conn = duckdb.connect(database=':memory:')
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{R2_ENDPOINT.replace('https://', '')}';")
    conn.execute(f"SET s3_access_key_id='{ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{SECRET_KEY}';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl=true;")
    return conn

def export_global_sliding_quota_data(quota_mb=10):
    files = get_quota_bounded_parquet_files(quota_mb)
    if not files:
        print("No global Parquet files available within quota window.")
        return

    conn = get_duckdb_connection()
    try:
        # Dynamically queries all global regions present in the sliding quota window
        aoi_query = f"""
            SELECT DISTINCT region, sensor_type
            FROM read_parquet({files})
            ORDER BY region;
        """
        aoi_df = conn.execute(aoi_query).fetchdf()
        
        summary_query = f"""
            SELECT 
                region,
                sensor_type,
                COUNT(*) as total_detections,
                ROUND(AVG(confidence), 2) as avg_confidence,
                ROUND(MAX(frp), 2) as max_frp,
                MIN(source_timestamp) as earliest_time,
                MAX(source_timestamp) as latest_time
            FROM read_parquet({files})
            GROUP BY region, sensor_type
            ORDER BY total_detections DESC;
        """
        summary_df = conn.execute(summary_query).fetchdf()
        
        payload = {
            "quota_mb": quota_mb,
            "scope": "global",
            "active_aois": aoi_df.to_dict(orient='records'),
            "summary_metrics": summary_df.to_dict(orient='records')
        }
        
        os.makedirs("data", exist_ok=True)
        output_path = "data/active_fires.json"
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, default=str, indent=2)
            
        print(f"Successfully exported global sliding quota metrics to {output_path}")
        
    except Exception as e:
        print(f"Error querying global data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    export_global_sliding_quota_data(quota_mb=10)