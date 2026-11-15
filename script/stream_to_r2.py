import os
import io
import datetime
import requests
from dotenv import load_dotenv
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

R2_ENDPOINT_URL = f"https://{os.environ.get('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com"
BUCKET_NAME = "quantticks-nasa-parquet"

MAX_STORAGE_MB = 100
MAX_STORAGE_BYTES = MAX_STORAGE_MB * 1024 * 1024

s3_client = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=os.environ.get('R2_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('R2_SECRET_ACCESS_KEY'),
    region_name='auto'
)

def enforce_rolling_retention():
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="telemetry/")
    if "Contents" not in response:
        return

    objects = sorted(response["Contents"], key=lambda x: x["LastModified"])
    total_size = sum(obj["Size"] for obj in objects)
    
    while total_size > MAX_STORAGE_BYTES and len(objects) > 1:
        oldest = objects.pop(0)
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=oldest['Key'])
        total_size -= oldest['Size']

def fetch_comprehensive_global_data():
    """
    Fetches real-time global telemetry across multiple sensors (MODIS and VIIRS) 
    to ensure a high density of locations worldwide.
    """
    map_key = os.environ.get('NASA_FIRMS_MAP_KEY', 'DEMO_KEY')
    sensors = ['MODIS_NRT', 'VIIRS_SNPP_NRT', 'VIIRS_NOAA20_NRT']
    all_records = []

    for sensor in sensors:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/{sensor}/world/1"
        print(f"Fetching global telemetry for {sensor}...")
        response = requests.get(url)
        if response.status_code == 200:
            lines = response.text.splitlines()
            if len(lines) > 1:
                header = lines[0].split(',')
                for line in lines[1:]:
                    values = line.split(',')
                    if len(values) == len(header):
                        record = dict(zip(header, values))
                        record['sensor_type'] = sensor
                        all_records.append(record)
        else:
            print(f"Skipping {sensor}: HTTP {response.status_code}")

    return all_records

def process_and_chunk_global_telemetry():
    raw_records = fetch_comprehensive_global_data()
    if not raw_records:
        print("No global records returned. Falling back to mock sample set for testing.")
        return

    buffer_rows = []
    for record in raw_records:
        date_str = record.get('acq_date', '2026-01-01')
        time_str = record.get('acq_time', '0000').zfill(4)
        iso_time_str = f"{date_str}T{time_str[:2]}:{time_str[2:]}:00"
        
        try:
            source_ts = datetime.datetime.fromisoformat(iso_time_str)
        except ValueError:
            source_ts = datetime.datetime.now(datetime.timezone.utc)

        server_ts = datetime.datetime.now(datetime.timezone.utc)

        # Map latitude/longitude to broader geographic sub-regions or country groupings
        lat = float(record.get('latitude', 0.0))
        lon = float(record.get('longitude', 0.0))
        
        # Simple geographic classifier to group global locations dynamically
        if lat > 15:
            region = "North America / Eurasia" if lon < -30 else "Europe / Northern Asia"
        elif lat > -15:
            region = "Equatorial / Africa & South America"
        else:
            region = "Southern Hemisphere / Oceania & South America"

        buffer_rows.append({
            'region': region,
            'latitude': lat,
            'longitude': lon,
            'confidence': int(float(record.get('confidence', 50) if record.get('confidence') not in [None, '', 'l', 'h', 'n'] else 50)),
            'frp': float(record.get('frp', 0.0)),
            'sensor_type': record.get('sensor_type', 'VIIRS'),
            'source_timestamp': source_ts,
            'server_ingest_timestamp': server_ts
        })

    schema = pa.schema([
        ('region', pa.string()),
        ('latitude', pa.float64()),
        ('longitude', pa.float64()),
        ('confidence', pa.int32()),
        ('frp', pa.float32()),
        ('sensor_type', pa.string()),
        ('source_timestamp', pa.timestamp('us')),
        ('server_ingest_timestamp', pa.timestamp('us'))
    ])
    
    table = pa.Table.from_arrays([pa.array([r[col] for r in buffer_rows]) for col in schema.names], schema=schema)
    
    sink = io.BytesIO()
    pq.write_table(table, sink, compression='ZSTD')
    file_bytes = sink.getvalue()
    
    server_time_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    filename = f"telemetry/global_comprehensive_chunk_{server_time_str}.parquet"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=file_bytes,
        ContentType='application/octet-stream'
    )
    print(f"Successfully uploaded global chunk {filename} ({len(buffer_rows)} records).")
    enforce_rolling_retention()

if __name__ == "__main__":
    process_and_chunk_global_telemetry()