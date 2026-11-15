import os
import json
import duckdb
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "ec04c95f976772c86722feb4f6752898")

# Paths
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
JSON_OUTPUT = DATA_DIR / "active_fires.json"

# Connect to DuckDB and configure HTTPFS / R2 credentials
con = duckdb.connect(database=":memory:")
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Configure S3-compatible credentials for Cloudflare R2
con.execute(f"SET s3_access_key_id='{R2_ACCESS_KEY_ID}';")
con.execute(f"SET s3_secret_access_key='{R2_SECRET_ACCESS_KEY}';")
con.execute(f"SET s3_endpoint='{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com';")
con.execute("SET s3_region='auto';")
con.execute("SET s3_url_style='path';")

print("Executing analytical query directly over Cloudflare R2 bucket via DuckDB...")

# R2 Bucket Path
R2_PARQUET_GLOB = "s3://quantticks-nasa-parquet/telemetry/*.parquet"

# 1. Summary Metrics & Regions Query (Using GROUP BY 1, 2 to avoid binder errors)
metrics_df = con.execute(f"""
    SELECT 
        CASE 
            WHEN latitude BETWEEN 40 AND 55 AND longitude BETWEEN -90 AND -50 THEN 'Newfoundland & Atlantic Canada'
            WHEN latitude BETWEEN 32 AND 42 AND longitude BETWEEN -125 AND -114 THEN 'California & Western US'
            WHEN latitude BETWEEN 48 AND 60 AND longitude BETWEEN -120 AND -110 THEN 'Alberta & Western Canada'
            WHEN latitude BETWEEN -20 AND 5 AND longitude BETWEEN -75 AND -45 THEN 'Amazon Basin, South America'
            WHEN latitude BETWEEN 30 AND 45 AND longitude BETWEEN -10 AND 35 THEN 'Mediterranean Basin'
            WHEN latitude BETWEEN -40 AND -10 AND longitude BETWEEN 110 AND 155 THEN 'Southeast Australia'
            WHEN latitude BETWEEN 50 AND 70 AND longitude BETWEEN 60 AND 140 THEN 'Siberia Taiga'
            WHEN latitude BETWEEN -10 AND 6 AND longitude BETWEEN 95 AND 141 THEN 'Indonesia Archipelago'
            WHEN latitude BETWEEN -35 AND 15 AND longitude BETWEEN -20 AND 50 THEN 'Equatorial Africa'
            ELSE 'Global / Other Regions'
        END AS region,
        sensor_type,
        COUNT(*) AS total_detections,
        ROUND(AVG(confidence), 1) AS avg_confidence,
        ROUND(MAX(frp), 2) AS max_frp,
        MIN(source_timestamp) AS earliest_source_time,
        MAX(source_timestamp) AS latest_source_time
    FROM '{R2_PARQUET_GLOB}'
    GROUP BY 1, 2
    ORDER BY total_detections DESC
""").fetchdf()

# 2. Active AOIs Dropdown List
aois_df = con.execute(f"""
    SELECT DISTINCT 
        CASE 
            WHEN latitude BETWEEN 40 AND 55 AND longitude BETWEEN -90 AND -50 THEN 'Newfoundland & Atlantic Canada'
            WHEN latitude BETWEEN 32 AND 42 AND longitude BETWEEN -125 AND -114 THEN 'California & Western US'
            WHEN latitude BETWEEN 48 AND 60 AND longitude BETWEEN -120 AND -110 THEN 'Alberta & Western Canada'
            WHEN latitude BETWEEN -20 AND 5 AND longitude BETWEEN -75 AND -45 THEN 'Amazon Basin, South America'
            WHEN latitude BETWEEN 30 AND 45 AND longitude BETWEEN -10 AND 35 THEN 'Mediterranean Basin'
            WHEN latitude BETWEEN -40 AND -10 AND longitude BETWEEN 110 AND 155 THEN 'Southeast Australia'
            WHEN latitude BETWEEN 50 AND 70 AND longitude BETWEEN 60 AND 140 THEN 'Siberia Taiga'
            WHEN latitude BETWEEN -10 AND 6 AND longitude BETWEEN 95 AND 141 THEN 'Indonesia Archipelago'
            WHEN latitude BETWEEN -35 AND 15 AND longitude BETWEEN -20 AND 50 THEN 'Equatorial Africa'
            ELSE 'Global / Other Regions'
        END AS region,
        sensor_type
    FROM '{R2_PARQUET_GLOB}'
    ORDER BY region
""").fetchdf()

# 3. Exact Coordinate Hotspots for Interactive ScatterGeo Map (Top 2,000 FRP peaks)
hotspots_df = con.execute(f"""
    SELECT 
        ROUND(latitude, 3) AS lat,
        ROUND(longitude, 3) AS lon,
        ROUND(frp, 1) AS frp,
        confidence,
        sensor_type,
        CASE 
            WHEN latitude BETWEEN 40 AND 55 AND longitude BETWEEN -90 AND -50 THEN 'Newfoundland & Atlantic Canada'
            WHEN latitude BETWEEN 32 AND 42 AND longitude BETWEEN -125 AND -114 THEN 'California & Western US'
            WHEN latitude BETWEEN 48 AND 60 AND longitude BETWEEN -120 AND -110 THEN 'Alberta & Western Canada'
            WHEN latitude BETWEEN -20 AND 5 AND longitude BETWEEN -75 AND -45 THEN 'Amazon Basin, South America'
            WHEN latitude BETWEEN 30 AND 45 AND longitude BETWEEN -10 AND 35 THEN 'Mediterranean Basin'
            WHEN latitude BETWEEN -40 AND -10 AND longitude BETWEEN 110 AND 155 THEN 'Southeast Australia'
            WHEN latitude BETWEEN 50 AND 70 AND longitude BETWEEN 60 AND 140 THEN 'Siberia Taiga'
            WHEN latitude BETWEEN -10 AND 6 AND longitude BETWEEN 95 AND 141 THEN 'Indonesia Archipelago'
            WHEN latitude BETWEEN -35 AND 15 AND longitude BETWEEN -20 AND 50 THEN 'Equatorial Africa'
            ELSE 'Global / Other Regions'
        END AS region
    FROM '{R2_PARQUET_GLOB}'
    WHERE frp IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL
    ORDER BY frp DESC
    LIMIT 5000
""").fetchdf()

# Build JSON Payload
payload = {
    "quota_mb": 10,
    "active_aois": aois_df.to_dict(orient="records"),
    "summary_metrics": metrics_df.to_dict(orient="records"),
    "hotspots": hotspots_df.to_dict(orient="records")
}

with open(JSON_OUTPUT, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, default=str)

print(f"Successfully queried Cloudflare R2 bucket and exported telemetry metrics and {len(hotspots_df)} precise hotspot coordinates to {JSON_OUTPUT}")