import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
import requests
import duckdb
import pyarrow.parquet as pq

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [1111-NASA-INGEST] - %(message)s')
logger = logging.getLogger("A0_Oracle_NASA_Layer1111")

def load_config():
    """Loads NASA FIRMS configuration including multiple locations from local config JSON."""
    config_path = Path("config/nasa_config.json")
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                nasa_conf = data.get("nasa_firms", {})
                logger.info("📂 Loaded multi-location configuration from config/nasa_config.json")
                return {
                    "map_key": nasa_conf.get("map_key", "YOUR_FIRMS_MAP_KEY"),
                    "day_range": int(nasa_conf.get("day_range", 5)),
                    "locations": nasa_conf.get("locations", [
                        {"location_name": "Newfoundland_AOI", "bounding_box": "-60.0,45.0,-52.0,52.0"}
                    ])
                }
        except Exception as e:
            logger.warning(f"⚠️ Failed to read config/nasa_config.json: {e}")
            
    return {
        "map_key": os.getenv("NASA_FIRMS_MAP_KEY", "YOUR_FIRMS_MAP_KEY"),
        "day_range": 5,
        "locations": [
            {"location_name": "Newfoundland_AOI", "bounding_box": "-60.0,45.0,-52.0,52.0"}
        ]
    }

class NASAFirmsIngestionEngine:
    def __init__(self, dump_path: str = "D:/nasa", location_name: str = "Newfoundland_AOI", bounding_box: str = "-60.0,45.0,-52.0,52.0"):
        self.base_path = dump_path
        self.location_name = location_name
        self.bounding_box = bounding_box
        self.db_path = os.path.join(self.base_path, "nasa_fire_physics.duckdb")
        self.parquet_path = os.path.join(self.base_path, "parquet")
        
        # Rotation Rules: 2.5 hours (9000s) or 5 MB size limit
        self.rotation_interval_sec = 9000  
        self.max_file_size_bytes = 5 * 1024 * 1024  
        self.last_rotation_time = time.time()
        
        self.sources = ["MODIS_NRT", "VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"]
        
        # Setup Persistence
        os.makedirs(self.base_path, exist_ok=True)
        os.makedirs(self.parquet_path, exist_ok=True)
        
        self.db = duckdb.connect(self.db_path)
        self.db.execute("SET memory_limit = '2GB';")
        self.db.execute("SET threads = 2;")
        
        # Harmonized schema including location tagging for web BI
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS harmonized_fires (
                location_name VARCHAR,
                bounding_box VARCHAR,
                acq_date DATE,
                year INTEGER,
                month INTEGER,
                latitude DOUBLE,
                longitude DOUBLE,
                brightness_temp DOUBLE,
                confidence FLOAT,
                harmonized_frp DOUBLE,
                sensor_type VARCHAR,
                ingested_at TIMESTAMP
            )
        """)
        logger.info(f"NASA FIRMS Sovereign Ingestion Engine initialized for [{self.location_name}]. Target: {self.base_path}")

    def discover_and_extract(self, map_key: str, day_range: int = 5):
        """Direct REST ingestion from NASA FIRMS API endpoints."""
        for source in self.sources:
            url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/{source}/{self.bounding_box}/{day_range}"
            logger.info(f"📡 Fetching data for source: {source} (Location: {self.location_name})...")
            
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    csv_data = response.text
                    if "latitude" not in csv_data:
                        logger.warning(f"⚠️ Empty payload returned for {source}.")
                        continue
                        
                    raw_dir = os.path.join(self.base_path, "raw")
                    os.makedirs(raw_dir, exist_ok=True)
                    raw_file_path = os.path.join(raw_dir, f"{source}_{int(time.time())}.csv")
                    with open(raw_file_path, "w", encoding="utf-8") as f:
                        f.write(csv_data)
                    
                    self.process_and_harmonize(raw_file_path, source)
                else:
                    logger.error(f"❌ Failed to fetch {source}. HTTP Status: {response.status_code}")
            except Exception as e:
                logger.error(f"❌ Error during extraction for {source}: {e}")

    def process_and_harmonize(self, csv_path: str, source_type: str):
        """Normalizes schemas with robust type casting and stamps location metadata."""
        logger.info(f"⚙️ Harmonizing schema for {source_type}...")
        is_modis = "MODIS" in source_type
        
        if is_modis:
            query = f"""
                INSERT INTO harmonized_fires
                SELECT 
                    '{self.location_name}' as location_name,
                    '{self.bounding_box}' as bounding_box,
                    CAST(acq_date AS DATE) as acq_date,
                    EXTRACT(YEAR FROM CAST(acq_date AS DATE)) as year,
                    EXTRACT(MONTH FROM CAST(acq_date AS DATE)) as month,
                    try_cast(latitude as DOUBLE) as latitude,
                    try_cast(longitude as DOUBLE) as longitude,
                    try_cast(brightness as DOUBLE) as brightness_temp,
                    CASE 
                        WHEN try_cast(confidence as VARCHAR) = 'h' THEN 100.0
                        WHEN try_cast(confidence as VARCHAR) = 'n' THEN 50.0
                        WHEN try_cast(confidence as VARCHAR) = 'l' THEN 25.0
                        ELSE COALESCE(try_cast(confidence as FLOAT), 50.0)
                    END as confidence,
                    try_cast(frp as DOUBLE) as harmonized_frp,
                    'MODIS' as sensor_type,
                    CURRENT_TIMESTAMP as ingested_at
                FROM read_csv_auto('{csv_path}', header=True);
            """
        else:
            query = f"""
                INSERT INTO harmonized_fires
                SELECT 
                    '{self.location_name}' as location_name,
                    '{self.bounding_box}' as bounding_box,
                    CAST(acq_date AS DATE) as acq_date,
                    EXTRACT(YEAR FROM CAST(acq_date AS DATE)) as year,
                    EXTRACT(MONTH FROM CAST(acq_date AS DATE)) as month,
                    try_cast(latitude as DOUBLE) as latitude,
                    try_cast(longitude as DOUBLE) as longitude,
                    try_cast(bright_ti4 as DOUBLE) as brightness_temp,
                    CASE 
                        WHEN try_cast(confidence as VARCHAR) = 'h' THEN 100.0
                        WHEN try_cast(confidence as VARCHAR) = 'n' THEN 50.0
                        WHEN try_cast(confidence as VARCHAR) = 'l' THEN 25.0
                        ELSE COALESCE(try_cast(confidence as FLOAT), 50.0)
                    END as confidence,
                    try_cast(frp as DOUBLE) * 0.75 as harmonized_frp,
                    'VIIRS' as sensor_type,
                    CURRENT_TIMESTAMP as ingested_at
                FROM read_csv_auto('{csv_path}', header=True);
            """

        try:
            self.db.execute(query)
            inserted_count = self.db.execute("SELECT COUNT(*) FROM harmonized_fires").fetchone()[0]
            logger.info(f"✅ Harmonized records from {source_type}. Total stashed: {inserted_count:,}")
            
            if self._should_rotate():
                self.rotate_to_parquet()
                
        except Exception as e:
            logger.error(f"❌ Error during schema harmonization for {source_type}: {e}")

    def _should_rotate(self) -> bool:
        time_elapsed = time.time() - self.last_rotation_time
        row_count = self.db.execute("SELECT COUNT(*) FROM harmonized_fires").fetchone()[0]
        estimated_size_bytes = row_count * 120 
        
        if time_elapsed >= self.rotation_interval_sec or estimated_size_bytes >= self.max_file_size_bytes:
            return True
        return False

    def rotate_to_parquet(self):
        """Exports the unified staging table containing all locations to a single ZSTD Parquet file."""
        timestamp = int(time.time())
        target_file = os.path.join(self.parquet_path, f"nasa_fires_global_{timestamp}.parquet")
        
        try:
            self.db.execute(f"""
                COPY (
                    SELECT * FROM harmonized_fires
                ) TO '{target_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
            """)
            
            self.db.execute("DELETE FROM harmonized_fires;")
            self.db.execute("CHECKPOINT;")
            self.last_rotation_time = time.time()
            
            self.audit_parquet_events(target_file)
            
        except Exception as e:
            logger.error(f"❌ Error writing unified Parquet rotation: {e}")

    def audit_parquet_events(self, file_path: str):
        try:
            if not os.path.exists(file_path):
                return
            file_size = os.path.getsize(file_path)
            metadata = pq.read_metadata(file_path)
            row_count = metadata.num_rows
            
            logger.info(f"🔍 [PARQUET EVENT AUDIT SUCCESS]")
            logger.info(f"   📂 Global File: {os.path.basename(file_path)}")
            logger.info(f"   📊 Total Audited Fire Events (All Locations): {row_count:,}")
            logger.info(f"   💾 Disk Size: {file_size / 1024:.2f} KB")
        except Exception as e:
            logger.error(f"❌ Error auditing Parquet file metadata: {e}")

    def shutdown(self):
        if self.db.execute("SELECT COUNT(*) FROM harmonized_fires").fetchone()[0] > 0:
            logger.info("💾 Flushing remaining rows to Parquet on shutdown...")
            self.rotate_to_parquet()
        self.db.close()
        logger.info("NASA Sovereign Ingestion Engine shut down cleanly.")

if __name__ == "__main__":
    config = load_config()
    map_key = config["map_key"]
    day_range = config["day_range"]
    
    engine = NASAFirmsIngestionEngine(dump_path="D:/nasa")
    
    try:
        for loc in config["locations"]:
            engine.location_name = loc["location_name"]
            engine.bounding_box = loc["bounding_box"]
            logger.info(f"🌐 Processing region target -> [{engine.location_name}] ({engine.bounding_box})")
            engine.discover_and_extract(map_key=map_key, day_range=day_range)
    finally:
        engine.shutdown()